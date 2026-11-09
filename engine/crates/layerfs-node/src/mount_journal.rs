//! Durable SDK mount intent and original binding/Ready acknowledgements.
use crate::{
    durable_records::Directory,
    job::{failed, refused},
    registry::{lock, Mount, SandboxState},
};
use layerfs_api::{
    internal::workspace,
    sandbox::{MountMilestone, MountObservation, ObservedMountFailure},
    MountedWorkspace,
};
use layerfs_bridge::control::{Answer, Reply, SdkRefusal, SdkValue};
use layerfs_history::{BranchId, WorkspaceId};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
};

/// Unknown attempts keep their original receipt and pinned directory in this process.
struct Retained {
    journal: Journal,
    result: RetainedResult,
}
enum RetainedResult {
    Before(SdkRefusal),
    Mount(Box<ObservedMountFailure>),
    Ready(Box<MountedWorkspace>, SdkRefusal),
}
static RETAINED: LazyLock<Mutex<BTreeMap<PathBuf, Retained>>> = LazyLock::new(Default::default);

struct Journal {
    directory: Directory,
    name: String,
    sandbox: [u8; 32],
    workspace: WorkspaceId,
    branch: BranchId,
    branch_name: String,
    bound: Option<Vec<u8>>,
    ready: Option<Vec<u8>>,
}

fn encoding(reply: Reply) -> Result<Vec<u8>, SdkRefusal> {
    Answer { id: 1, reply }.encode().map_err(failed)
}
fn field<'a>(value: &'a SdkValue, key: &str) -> Option<&'a [u8]> {
    match value.get(key) {
        Some(SdkValue::Bytes(bytes)) => Some(bytes),
        _ => None,
    }
}
fn valid_complete(value: &SdkValue, sandbox: [u8; 32], name: &str) -> bool {
    let Some(workspace) = field(value, "workspace").and_then(|b| WorkspaceId::from_slice(b).ok())
    else {
        return false;
    };
    let Some(branch) = field(value, "branchId").and_then(|b| BranchId::from_slice(b).ok()) else {
        return false;
    };
    if field(value, "sandbox") != Some(sandbox.as_slice())
        || value.get_text("phase") != Some("complete")
        || value.get_text("branch").is_none()
        || name
            != format!(
                "mount-intent-{}.record",
                layerfs_bridge::sdk_values::hex(workspace.as_slice())
            )
    {
        return false;
    }
    let bound = field(value, "bound").and_then(|b| Answer::decode(b).ok());
    let ready = field(value, "ready").and_then(|b| Answer::decode(b).ok());
    matches!((bound, ready),
        (Some(Answer { id: 1, reply: Reply::Bound { token, binding } }),
         Some(Answer { id: 1, reply: Reply::Ready(ready) }))
        if token.workspace == workspace && binding.branch.id == branch && ready.token == token)
}

/// Incomplete or malformed records are not permission to repeat a mount.
fn check_pending(directory: &Directory, sandbox: [u8; 32]) -> Result<(), SdkRefusal> {
    directory.verify().map_err(failed)?;
    for entry in std::fs::read_dir(directory.path()).map_err(failed)? {
        let entry = entry.map_err(failed)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("mount-intent-") || !name.ends_with(".record") {
            continue;
        }
        let value = crate::discovery::load(&entry.path()).map_err(|error| {
            refused(
                "RecoveryRequired",
                format!("a durable mount intent cannot be read: {}", error.message),
            )
        })?;
        if !valid_complete(&value, sandbox, &name) {
            return Err(refused(
                "RecoveryRequired",
                "a durable Workspace mount is unresolved; its original intent is retained",
            ));
        }
    }
    directory.verify().map_err(failed)
}
impl Journal {
    fn new(
        directory: Directory,
        sandbox: [u8; 32],
        workspace: WorkspaceId,
        branch: BranchId,
        branch_name: &str,
    ) -> Self {
        let name = format!(
            "mount-intent-{}.record",
            layerfs_bridge::sdk_values::hex(workspace.as_slice())
        );
        Self {
            directory,
            name,
            sandbox,
            workspace,
            branch,
            branch_name: branch_name.to_owned(),
            bound: None,
            ready: None,
        }
    }
    fn begin(&self) -> Result<(), SdkRefusal> {
        crate::discovery::pinned_write(
            &self.directory,
            &self.name,
            self.value("beforeBind"),
            true,
        )?;
        self.directory.verify().map_err(|error| {
            refused(
                "Unknown",
                format!("mount intent created but its original directory changed: {error}"),
            )
        })
    }
    fn value(&self, phase: &str) -> SdkValue {
        SdkValue::map([
            ("phase", SdkValue::text(phase)),
            ("sandbox", SdkValue::Bytes(self.sandbox.to_vec())),
            (
                "workspace",
                SdkValue::Bytes(self.workspace.to_bytes().to_vec()),
            ),
            ("branchId", SdkValue::Bytes(self.branch.to_bytes().to_vec())),
            ("branch", SdkValue::text(&self.branch_name)),
            (
                "bound",
                self.bound.clone().map(SdkValue::Bytes).unwrap_or_default(),
            ),
            (
                "ready",
                self.ready.clone().map(SdkValue::Bytes).unwrap_or_default(),
            ),
        ])
    }
    fn update(&self, phase: &str) -> Result<(), SdkRefusal> {
        self.directory.verify().map_err(|error| {
            refused(
                "Unknown",
                format!("original mount directory changed: {error}"),
            )
        })?;
        crate::discovery::pinned_write(&self.directory, &self.name, self.value(phase), false)?;
        self.directory.verify().map_err(|error| {
            refused(
                "Unknown",
                format!("mount record published but its original directory changed: {error}"),
            )
        })
    }
    fn observe(&mut self, at: MountObservation<'_>) -> io::Result<()> {
        if at.workspace != self.workspace || at.branch != self.branch {
            return Err(io::Error::other("mount observation identity changed"));
        }
        let result = (|| {
            let phase = match at.milestone {
                MountMilestone::BeforeBind => return self.directory.verify().map_err(failed),
                MountMilestone::Bound | MountMilestone::BeforeAttach => {
                    let bound = at
                        .bound
                        .ok_or_else(|| refused("Unknown", "missing original bind receipt"))?;
                    self.bound = Some(encoding(Reply::Bound {
                        token: bound.token,
                        binding: bound.binding.clone(),
                    })?);
                    if at.milestone == MountMilestone::Bound {
                        "bound"
                    } else {
                        "beforeAttach"
                    }
                }
                MountMilestone::Attached => {
                    let ready = at
                        .ready
                        .ok_or_else(|| refused("Unknown", "missing original Ready receipt"))?;
                    self.ready = Some(encoding(Reply::Ready(Box::new(ready.clone())))?);
                    "attached"
                }
            };
            self.update(phase)
        })();
        result.map_err(|error| io::Error::other(error.message))
    }
}

fn retain(path: &Path, journal: Journal, result: RetainedResult) -> SdkRefusal {
    let message = match &result {
        RetainedResult::Before(error) => {
            format!("the mount intent could not be confirmed: {}", error.message)
        }
        RetainedResult::Mount(failure) => format!(
            "the original mount stopped at {:?}: {:?}",
            failure.milestone, failure.cause
        ),
        RetainedResult::Ready(mounted, error) => format!(
            "Workspace {:?} reached Ready but its receipt was not fully recorded: {}",
            mounted.bound.token.workspace, error.message
        ),
    };
    let retained = Retained { journal, result };
    match RETAINED.lock() {
        Ok(mut entries) => {
            entries.insert(path.to_owned(), retained);
        }
        Err(_) => std::mem::forget(retained),
    }
    refused(
        "Unknown",
        format!("{message}; its original owner and intent are retained"),
    )
}

pub(crate) fn mount(
    state: &SandboxState,
    workspace_id: WorkspaceId,
    branch_id: BranchId,
    branch_name: &str,
) -> Result<MountedWorkspace, SdkRefusal> {
    crate::registry::check_durable_close(state)?;
    if let Some(retained) = lock(&RETAINED)?.get(&state.directory) {
        let _ = (&retained.journal, &retained.result);
        return Err(refused(
            "RecoveryRequired",
            "a prior Workspace mount is retained by this process",
        ));
    }
    let sandbox = state
        .durable_identity
        .ok_or_else(|| refused("InvalidInput", "missing durable Sandbox identity"))?;
    let directory = Directory::open(&state.directory).map_err(failed)?;
    check_pending(&directory, sandbox)?;
    let mut control = crate::trace::control(&state.controls)?;
    let mut journal = Journal::new(directory, sandbox, workspace_id, branch_id, branch_name);
    if let Err(error) = journal.begin() {
        return Err(retain(
            &state.directory,
            journal,
            RetainedResult::Before(error),
        ));
    }
    let mounted = match workspace(&mut control)
        .mount_observed(workspace_id, branch_id, |at| journal.observe(at))
    {
        Ok(mounted) => mounted,
        Err(failure) => {
            return Err(retain(
                &state.directory,
                journal,
                RetainedResult::Mount(failure),
            ))
        }
    };
    let published =
        crate::discovery::save_workspace_pinned(&journal.directory, &mounted, branch_name)
            .and_then(|()| {
                lock(&state.mounts)?.insert(
                    mounted.ready.directory.clone(),
                    Mount {
                        workspace: mounted.clone(),
                        branch: branch_name.to_owned(),
                    },
                );
                Ok(())
            })
            .and_then(|()| journal.update("complete"));
    if let Err(error) = published {
        return Err(retain(
            &state.directory,
            journal,
            RetainedResult::Ready(Box::new(mounted), error),
        ));
    }
    Ok(mounted)
}
