//! Fixed-size, versioned discovery records; no user path is interpreted.
use super::files::RosterError;
use layerfs_bridge::control::WorkspaceToken;
use layerfs_history::{BranchId, HistoryCatalog, WorkspaceId};
use std::fmt::Write as _;

const OWNER_MAGIC: &[u8; 8] = b"LFSROST1";
const RECORD_MAGIC: &[u8; 8] = b"LFSWORK1";
const CLOSING_MAGIC: &[u8; 8] = b"LFSCLSG1";
const CLOSED_MAGIC: &[u8; 8] = b"LFSCLOS1";
const CLOSING_V2: &[u8; 8] = b"LFSCLSG2";
const CLOSED_V2: &[u8; 8] = b"LFSCLOS2";
const CANCELLED_V2: &[u8; 8] = b"LFSCANC2";
pub(super) const OWNER_LEN: usize = 8 + 32 + 32 + 32;
pub(super) const RECORD_LEN: usize = 8 + 32 + 32 + 32 + 17 + 32;

/// Independently supplied identities of the Sandbox and its original Store.
/// The caller must authenticate these values and validate the Store itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Authority {
    pub sandbox: [u8; 32],
    pub store: [u8; 32],
}

impl Authority {
    /// Bind discovery evidence to a validated history catalog identity and
    /// incarnation. The caller must still prove the physical Store instance.
    pub fn for_catalog(
        sandbox: [u8; 32],
        catalog: &dyn HistoryCatalog,
    ) -> Result<Self, RosterError> {
        let incarnation = catalog.incarnation();
        if sandbox == [0; 32] || incarnation == 0 {
            return Err(RosterError::Invalid("roster authority"));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"layerfs/durable-roster-store/v1\0");
        hasher.update(catalog.catalog_id().as_slice());
        hasher.update(&incarnation.to_be_bytes());
        Ok(Self {
            sandbox,
            store: *hasher.finalize().as_bytes(),
        })
    }
}

/// The originally requested Branch for one immutable Workspace identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkspaceRecord {
    pub workspace: WorkspaceId,
    pub branch: BranchId,
}

impl WorkspaceRecord {
    /// A deterministic single filename for the separate durable Overlay file.
    pub fn overlay_name(self) -> String {
        format!("{}.sqlite", identity_name(self.workspace))
    }
}

/// A persisted close intent, including the exact original namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Closing {
    pub record: WorkspaceRecord,
    pub token: WorkspaceToken,
    /// Original intent sequence; zero identifies a legacy v1 close.
    pub attempt: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Event {
    Created(WorkspaceRecord),
    Closing(Closing),
    Closed(Closing),
    Cancelled(Closing),
}

impl Event {
    pub(super) fn file_name(self) -> String {
        match self {
            Self::Created(record) => format!("{}.record", identity_name(record.workspace)),
            Self::Closing(closing) if closing.attempt == 0 => {
                format!("{}.closing", identity_name(closing.record.workspace))
            }
            Self::Closing(closing) => format!(
                "{}.closing-{:016x}",
                identity_name(closing.record.workspace),
                closing.attempt
            ),
            Self::Closed(closing) => format!("{}.closed", identity_name(closing.record.workspace)),
            Self::Cancelled(closing) => format!(
                "{}.cancelled-{:016x}",
                identity_name(closing.record.workspace),
                closing.attempt
            ),
        }
    }
}

pub(super) fn identity_name(workspace: WorkspaceId) -> String {
    let mut name = String::from("workspace-");
    for byte in workspace.to_bytes() {
        write!(&mut name, "{byte:02x}").expect("writing to String cannot fail");
    }
    name
}

pub(super) fn owner_bytes(authority: Authority) -> Result<Vec<u8>, RosterError> {
    if authority.sandbox == [0; 32] || authority.store == [0; 32] {
        return Err(RosterError::Invalid("roster authority"));
    }
    let mut bytes = Vec::with_capacity(OWNER_LEN);
    bytes.extend_from_slice(OWNER_MAGIC);
    bytes.extend_from_slice(&authority.sandbox);
    bytes.extend_from_slice(&authority.store);
    checksum(&mut bytes);
    Ok(bytes)
}

pub(super) fn record_bytes(authority: Authority, event: Event) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(RECORD_LEN);
    let (magic, record, closing, v2) = match event {
        Event::Created(record) => (RECORD_MAGIC, record, None, false),
        Event::Closing(closing) if closing.attempt == 0 => {
            (CLOSING_MAGIC, closing.record, Some(closing), false)
        }
        Event::Closed(closing) if closing.attempt == 0 => {
            (CLOSED_MAGIC, closing.record, Some(closing), false)
        }
        Event::Closing(closing) => (CLOSING_V2, closing.record, Some(closing), true),
        Event::Closed(closing) => (CLOSED_V2, closing.record, Some(closing), true),
        Event::Cancelled(closing) => (CANCELLED_V2, closing.record, Some(closing), true),
    };
    bytes.extend_from_slice(magic);
    if let Some(closing) = closing {
        let namespace = closing.token.namespace;
        // Lifecycle events retain the original fixed width and bind the exact
        // namespace to both authorities and the immutable Workspace/Branch.
        bytes.extend_from_slice(&namespace.to_be_bytes());
        if v2 {
            bytes.extend_from_slice(&closing.attempt.to_be_bytes());
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(if v2 {
            b"layerfs/durable-roster-close/v2\0"
        } else {
            b"layerfs/durable-roster-close/v1\0"
        });
        hasher.update(magic);
        hasher.update(&authority.sandbox);
        hasher.update(&authority.store);
        hasher.update(record.workspace.as_slice());
        hasher.update(record.branch.as_slice());
        hasher.update(&namespace.to_be_bytes());
        if v2 {
            hasher.update(&closing.attempt.to_be_bytes());
        }
        bytes.extend_from_slice(&hasher.finalize().as_bytes()[..if v2 { 16 } else { 24 }]);
    } else {
        bytes.extend_from_slice(&authority.sandbox);
    }
    bytes.extend_from_slice(&authority.store);
    bytes.extend_from_slice(record.workspace.as_slice());
    bytes.extend_from_slice(record.branch.as_slice());
    checksum(&mut bytes);
    bytes
}

fn checksum(bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(blake3::hash(bytes).as_bytes());
}

pub(super) fn decode_record(authority: Authority, bytes: &[u8]) -> Result<Event, RosterError> {
    if bytes.len() != RECORD_LEN
        || bytes[40..72] != authority.store
        || blake3::hash(&bytes[..RECORD_LEN - 32]).as_bytes() != &bytes[RECORD_LEN - 32..]
    {
        return Err(RosterError::Invalid("roster record integrity"));
    }
    let workspace = WorkspaceId::from_slice(&bytes[72..104])
        .map_err(|_| RosterError::Invalid("roster Workspace identity"))?;
    let branch = BranchId::from_slice(&bytes[104..121])
        .map_err(|_| RosterError::Invalid("roster Branch identity"))?;
    let record = WorkspaceRecord { workspace, branch };
    let event = if &bytes[..8] == RECORD_MAGIC {
        Event::Created(record)
    } else {
        let namespace = i64::from_be_bytes(bytes[8..16].try_into().expect("fixed namespace"));
        if namespace <= 0 {
            return Err(RosterError::Invalid("roster closing namespace"));
        }
        let legacy = &bytes[..8] == CLOSING_MAGIC || &bytes[..8] == CLOSED_MAGIC;
        let attempt = if legacy {
            0
        } else {
            u64::from_be_bytes(bytes[16..24].try_into().expect("fixed attempt"))
        };
        let closing = Closing {
            record,
            token: WorkspaceToken {
                workspace,
                namespace,
            },
            attempt,
        };
        if &bytes[..8] == CLOSING_MAGIC {
            Event::Closing(closing)
        } else if &bytes[..8] == CLOSED_MAGIC {
            Event::Closed(closing)
        } else if &bytes[..8] == CLOSING_V2 && attempt != 0 {
            Event::Closing(closing)
        } else if &bytes[..8] == CLOSED_V2 && attempt != 0 {
            Event::Closed(closing)
        } else if &bytes[..8] == CANCELLED_V2 {
            Event::Cancelled(closing)
        } else {
            return Err(RosterError::Invalid("roster record version"));
        }
    };
    if record_bytes(authority, event) != bytes {
        return Err(RosterError::Invalid("roster record authority"));
    }
    Ok(event)
}
