//! Fixed-size, versioned discovery records; no user path is interpreted.
use super::files::RosterError;
use layerfs_history::{BranchId, WorkspaceId};
use std::fmt::Write as _;

const OWNER_MAGIC: &[u8; 8] = b"LFSROST1";
const RECORD_MAGIC: &[u8; 8] = b"LFSWORK1";
pub(super) const OWNER_LEN: usize = 8 + 32 + 32 + 32;
pub(super) const RECORD_LEN: usize = 8 + 32 + 32 + 32 + 17 + 32;

/// Independently supplied identities of the Sandbox and its original Store.
/// The caller must authenticate these values and validate the Store itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Authority {
    pub sandbox: [u8; 32],
    pub store: [u8; 32],
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
    pub(super) fn file_name(self) -> String {
        format!("{}.record", identity_name(self.workspace))
    }
}

fn identity_name(workspace: WorkspaceId) -> String {
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

pub(super) fn record_bytes(authority: Authority, record: WorkspaceRecord) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(RECORD_LEN);
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.extend_from_slice(&authority.sandbox);
    bytes.extend_from_slice(&authority.store);
    bytes.extend_from_slice(record.workspace.as_slice());
    bytes.extend_from_slice(record.branch.as_slice());
    checksum(&mut bytes);
    bytes
}

fn checksum(bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(blake3::hash(bytes).as_bytes());
}

pub(super) fn decode_record(
    authority: Authority,
    bytes: &[u8],
) -> Result<WorkspaceRecord, RosterError> {
    if bytes.len() != RECORD_LEN
        || &bytes[..8] != RECORD_MAGIC
        || bytes[8..40] != authority.sandbox
        || bytes[40..72] != authority.store
        || blake3::hash(&bytes[..RECORD_LEN - 32]).as_bytes() != &bytes[RECORD_LEN - 32..]
    {
        return Err(RosterError::Invalid("roster record integrity"));
    }
    let workspace = WorkspaceId::from_slice(&bytes[72..104])
        .map_err(|_| RosterError::Invalid("roster Workspace identity"))?;
    let branch = BranchId::from_slice(&bytes[104..121])
        .map_err(|_| RosterError::Invalid("roster Branch identity"))?;
    Ok(WorkspaceRecord { workspace, branch })
}
