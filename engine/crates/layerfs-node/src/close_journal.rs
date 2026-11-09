//! Durable SDK close intent and the original, correlated unmount answer.
use crate::{
    durable_records::Directory,
    job::{failed, refused},
    registry::{lock, SandboxState},
};
use layerfs_api::{
    internal::workspace, sandbox::values, OperationCause, OperationFailure, Unmounted,
};
use layerfs_bridge::control::{Answer, ControlCode, Reply, SdkRefusal, SdkValue, WorkspaceToken};
use layerfs_history::WorkspaceId;
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
};

struct Journal {
    directory: Directory,
    name: String,
    sandbox: [u8; 32],
    token: WorkspaceToken,
    path: String,
    discard: bool,
    answer: Option<Vec<u8>>,
}

enum Original {
    Before(SdkRefusal),
    Failed(Box<OperationFailure>, Option<SdkRefusal>),
    Answer(Unmounted, SdkRefusal),
}
struct Retained {
    journal: Journal,
    state: Arc<SandboxState>,
    original: Original,
}
static RETAINED: LazyLock<Mutex<BTreeMap<PathBuf, Retained>>> = LazyLock::new(Default::default);

fn workspace_name(workspace: WorkspaceId) -> String {
    format!(
        "workspace-{}.record",
        layerfs_bridge::sdk_values::hex(workspace.as_slice())
    )
}
fn bytes<'a>(value: &'a SdkValue, key: &str) -> Option<&'a [u8]> {
    match value.get(key) {
        Some(SdkValue::Bytes(bytes)) => Some(bytes),
        _ => None,
    }
}
fn is_busy(reply: &Reply) -> bool {
    matches!(reply, Reply::Refused(refusal)
        if refusal.code == ControlCode::Busy
            && refusal.phase == "unmount:kernel"
            && refusal.moved.is_none()
            && refusal.published.is_none())
}
enum Settlement {
    Closed(WorkspaceToken),
    NoEffect(WorkspaceToken, String),
}
fn valid_settled(
    directory: &Directory,
    value: &SdkValue,
    sandbox: [u8; 32],
    name: &str,
) -> Option<Settlement> {
    let workspace = bytes(value, "workspace").and_then(|b| WorkspaceId::from_slice(b).ok())?;
    let namespace = value.get_int("namespace").filter(|n| *n > 0)?;
    let prefix = format!(
        "close-intent-{}-",
        layerfs_bridge::sdk_values::hex(workspace.as_slice())
    );
    let nonce = name
        .strip_prefix(&prefix)
        .and_then(|s| s.strip_suffix(".record"))?;
    if nonce.len() != 16
        || !nonce.bytes().all(|c| c.is_ascii_hexdigit())
        || bytes(value, "sandbox") != Some(sandbox.as_slice())
    {
        return None;
    }
    let path = value.get_text("path").filter(|path| !path.is_empty())?;
    let discard = match value.get("discard") {
        Some(SdkValue::Bool(value)) => *value,
        _ => return None,
    };
    let token = WorkspaceToken {
        workspace,
        namespace,
    };
    let Some(Answer { id: 1, reply }) = bytes(value, "answer").and_then(|b| Answer::decode(b).ok())
    else {
        return None;
    };
    match value.get_text("phase") {
        Some("complete")
            if reply == Reply::Unmounted(token)
                && matches!(
                    directory
                        .path()
                        .join(workspace_name(workspace))
                        .try_exists(),
                    Ok(false)
                ) =>
        {
            Some(Settlement::Closed(token))
        }
        Some("noEffect")
            if matches!(&reply, Reply::Dirty(report) if !discard && report.token == token)
                || is_busy(&reply) =>
        {
            Some(Settlement::NoEffect(token, path.to_owned()))
        }
        _ => None,
    }
}

fn original_workspace(directory: &Directory, token: WorkspaceToken, path: &str) -> bool {
    let Ok(value) = crate::discovery::load(&directory.path().join(workspace_name(token.workspace)))
    else {
        return false;
    };
    bytes(&value, "workspace") == Some(token.workspace.as_slice())
        && value.get_int("namespace") == Some(token.namespace)
        && value.get_text("path") == Some(path)
}

/// Both partial records and retained in-process outcomes block further use.
pub(crate) fn check(state: &SandboxState) -> Result<(), SdkRefusal> {
    let sandbox = state
        .durable_identity
        .ok_or_else(|| refused("InvalidInput", "missing durable Sandbox identity"))?;
    check_at(&state.directory, sandbox)
}

/// Check before reopening the Engine or asking a daemon for status.
pub(crate) fn check_at(path: &Path, sandbox: [u8; 32]) -> Result<(), SdkRefusal> {
    if let Some(retained) = lock(&RETAINED)?.get(path) {
        let _ = (&retained.journal, &retained.state, &retained.original);
        return Err(refused(
            "RecoveryRequired",
            "a durable Workspace close is retained by this process",
        ));
    }
    let directory = Directory::open(path).map_err(failed)?;
    closed_in(&directory, sandbox).map(|_| ())
}

/// Validated terminal close identities, read while the caller holds the guard.
pub(crate) fn closed_in(
    directory: &Directory,
    sandbox: [u8; 32],
) -> Result<BTreeSet<(WorkspaceId, i64)>, SdkRefusal> {
    directory.verify().map_err(failed)?;
    let mut settled = Vec::new();
    for entry in std::fs::read_dir(directory.path()).map_err(failed)? {
        let entry = entry.map_err(failed)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("close-intent-") || !name.ends_with(".record") {
            continue;
        }
        let value = crate::discovery::load(&entry.path()).map_err(|error| {
            refused(
                "RecoveryRequired",
                format!("a durable close intent cannot be read: {}", error.message),
            )
        })?;
        let Some(record) = valid_settled(directory, &value, sandbox, &name) else {
            return Err(refused(
                "RecoveryRequired",
                "a durable Workspace close is unresolved; its original intent is retained",
            ));
        };
        settled.push(record);
    }
    let closed = settled
        .iter()
        .filter_map(|record| match record {
            Settlement::Closed(token) => Some((token.workspace, token.namespace)),
            Settlement::NoEffect(_, _) => None,
        })
        .collect::<BTreeSet<_>>();
    for record in &settled {
        if let Settlement::NoEffect(token, path) = record {
            let subsequently_closed = closed.contains(&(token.workspace, token.namespace));
            if !subsequently_closed && !original_workspace(directory, *token, path) {
                return Err(refused(
                    "RecoveryRequired",
                    "a refused durable close has lost its original Workspace record",
                ));
            }
        }
    }
    directory.verify().map_err(failed)?;
    Ok(closed)
}

impl Journal {
    fn new(
        state: &SandboxState,
        token: WorkspaceToken,
        path: &str,
        discard: bool,
    ) -> Result<Self, SdkRefusal> {
        let nonce = crate::registry::random()?;
        Ok(Self {
            directory: Directory::open(&state.directory).map_err(failed)?,
            name: format!(
                "close-intent-{}-{}.record",
                layerfs_bridge::sdk_values::hex(token.workspace.as_slice()),
                layerfs_bridge::sdk_values::hex(&nonce[..8])
            ),
            sandbox: state
                .durable_identity
                .ok_or_else(|| refused("InvalidInput", "missing durable Sandbox identity"))?,
            token,
            path: path.to_owned(),
            discard,
            answer: None,
        })
    }
    fn value(&self, phase: &str) -> SdkValue {
        SdkValue::map([
            ("phase", SdkValue::text(phase)),
            ("sandbox", SdkValue::Bytes(self.sandbox.to_vec())),
            (
                "workspace",
                SdkValue::Bytes(self.token.workspace.to_bytes().to_vec()),
            ),
            ("namespace", SdkValue::Int(self.token.namespace)),
            ("path", SdkValue::text(&self.path)),
            ("discard", SdkValue::Bool(self.discard)),
            (
                "answer",
                self.answer.clone().map(SdkValue::Bytes).unwrap_or_default(),
            ),
        ])
    }
    fn write(&self, phase: &str, create: bool) -> Result<(), SdkRefusal> {
        self.directory.verify().map_err(failed)?;
        crate::discovery::pinned_write(&self.directory, &self.name, self.value(phase), create)?;
        self.directory.verify().map_err(failed)
    }
    fn record(&mut self, reply: Reply, phase: &str) -> Result<(), SdkRefusal> {
        self.answer = Some(Answer { id: 1, reply }.encode().map_err(failed)?);
        self.write(phase, false)
    }
}

fn retain(state: &Arc<SandboxState>, journal: Journal, original: Original) -> SdkRefusal {
    let detail = match &original {
        Original::Before(error) => error.message.clone(),
        Original::Failed(error, persist) => format!(
            "{error}{}",
            persist
                .as_ref()
                .map(|e| format!("; recording the reply: {}", e.message))
                .unwrap_or_default()
        ),
        Original::Answer(answer, error) => format!("{answer:?}: {}", error.message),
    };
    let entry = Retained {
        journal,
        state: state.clone(),
        original,
    };
    match RETAINED.lock() {
        Ok(mut entries) => {
            entries.insert(state.directory.clone(), entry);
        }
        Err(_) => std::mem::forget(entry),
    }
    refused("Unknown", format!("durable Workspace close could not be settled: {detail}; the original intent and owner are retained"))
}

/// One explicit close; only its own durable reply can settle the intent.
pub(crate) fn unmount(
    state: &Arc<SandboxState>,
    path: &str,
    token: WorkspaceToken,
    args: &SdkValue,
) -> Result<SdkValue, SdkRefusal> {
    if args.get_bool("force") {
        return Err(refused(
            "Unsupported",
            "forced unmount is not available for durable Workspaces",
        ));
    }
    check(state)?;
    let discard = args.get_bool("discard");
    let mut control = crate::trace::control(&state.controls)?;
    let mut journal = Journal::new(state, token, path, discard)?;
    if let Err(error) = journal.write("beforeUnmount", true) {
        return Err(retain(state, journal, Original::Before(error)));
    }
    let answer = match workspace(&mut control).unmount_checked(token, discard) {
        Ok(answer) => answer,
        Err(failure) => {
            if let OperationCause::Remote(refusal) = &failure.cause {
                let reply = Reply::Refused(refusal.clone());
                if is_busy(&reply) {
                    if let Err(error) = journal.record(reply, "noEffect") {
                        return Err(retain(
                            state,
                            journal,
                            Original::Failed(failure, Some(error)),
                        ));
                    }
                    return Err(crate::sandbox::operation(failure));
                }
            }
            return Err(retain(state, journal, Original::Failed(failure, None)));
        }
    };
    let reply = match &answer {
        Unmounted::Closed => Reply::Unmounted(token),
        Unmounted::Dirty(report) => Reply::Dirty(report.clone()),
    };
    let phase = if matches!(answer, Unmounted::Closed) {
        "closed"
    } else {
        "noEffect"
    };
    if let Err(error) = journal.record(reply.clone(), phase) {
        return Err(retain(state, journal, Original::Answer(answer, error)));
    }
    if matches!(answer, Unmounted::Closed) {
        if let Err(error) = journal
            .directory
            .remove(OsStr::new(&workspace_name(token.workspace)))
        {
            return Err(retain(
                state,
                journal,
                Original::Answer(answer, failed(error)),
            ));
        }
        if let Err(error) = journal.write("complete", false) {
            return Err(retain(state, journal, Original::Answer(answer, error)));
        }
        if let Err(error) = lock(&state.mounts).map(|mut mounts| mounts.remove(path)) {
            return Err(retain(state, journal, Original::Answer(answer, error)));
        }
    }
    values::unmounted(&answer)
}
