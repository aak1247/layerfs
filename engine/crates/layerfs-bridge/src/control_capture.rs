//! Bounded identities and original receipt proofs for external captures.
use crate::{
    control::{ControlError, WorkspaceToken},
    control_history::{head, put_head, put_token, token},
    wire::{Reader, Writer},
};
use layerfs_content::ObjectId;
use layerfs_history::{CommitId, WorkspaceId};
use sha2::{Digest, Sha256};

const MAGIC: &[u8] = b"LFCR\x01";
const SNAPSHOT_LIMIT: usize = 4096;

/// The accepted purpose of a captured Workspace frontier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapturePurpose {
    /// A managed Workspace snapshot under its original Owner scope.
    ManagedWorkspace,
}

/// An original request identity; it is not itself a read authorization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureIdentity {
    /// Fresh, nonzero identity of one original capture attempt.
    pub id: [u8; 32],
    /// Digest of the Owner-authenticated original business scope.
    pub scope_digest: [u8; 32],
    /// The requested use of this capture.
    pub purpose: CapturePurpose,
}

/// A request under the current exact daemon Workspace token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureControl {
    /// Current Workspace and namespace, verified by the daemon.
    pub token: WorkspaceToken,
    /// The immutable original capture identity.
    pub identity: CaptureIdentity,
}

/// Result fields not already fixed by the complete checkpoint request digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureOutcome {
    /// The original Store publication inserted this Commit.
    Committed {
        /// Exact original committed head.
        head: CommitId,
        /// Original Store-assigned time, excluded from Commit identity.
        time_ms: i64,
        /// Original displaced head, if any.
        displaced: Option<CommitId>,
    },
    /// The original Store outcome found the captured root unchanged.
    UpToDate {
        /// Original head; absence means the initial Layer root.
        head: Option<CommitId>,
    },
}

/// Compact immutable facts from one independently verified Store receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureProof {
    /// Original capture request and scope.
    pub identity: CaptureIdentity,
    /// Original Workspace identity.
    pub workspace: WorkspaceId,
    /// Namespace at the original capture, which need not be the current token.
    pub original_namespace: i64,
    /// Original captured generation.
    pub generation: i64,
    /// Original captured revision.
    pub revision: i64,
    /// Original captured base root.
    pub base_root: [u8; 32],
    /// Original Store catalog identity.
    pub catalog: [u8; 32],
    /// Original Store incarnation.
    pub incarnation: u64,
    /// Exact recorded checkpoint attempt.
    pub attempt: [u8; 32],
    /// Digest of the complete original checkpoint request, including all facts.
    pub request_digest: [u8; 32],
    /// Exact original saved candidate root.
    pub root: ObjectId,
    /// Complete canonical target Branch snapshot bytes.
    pub target_snapshot: Vec<u8>,
    /// Original Store result.
    pub outcome: CaptureOutcome,
}

/// Observation of the original attempt; absence never permits a replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CaptureObservation {
    /// No complete, independently verified publication or nonpublication proof.
    Unknown,
    /// The original attempt was proved unpublished and cannot be resent.
    Unpublished,
    /// The original Store receipt was independently checked.
    Published(Box<CaptureProof>),
}

fn put_identity(out: &mut Writer, value: CaptureIdentity) -> Result<(), ControlError> {
    if value.id == [0; 32] || value.scope_digest == [0; 32] {
        return Err(ControlError("capture identity"));
    }
    out.put(&value.id)?;
    out.put(&value.scope_digest)?;
    out.byte(match value.purpose {
        CapturePurpose::ManagedWorkspace => 1,
    })
}

fn identity(input: &mut Reader<'_>) -> Result<CaptureIdentity, ControlError> {
    let id = input.array()?;
    let scope_digest = input.array()?;
    let purpose = match input.byte()? {
        1 => CapturePurpose::ManagedWorkspace,
        _ => return Err(ControlError("capture purpose")),
    };
    if id == [0; 32] || scope_digest == [0; 32] {
        return Err(ControlError("capture identity"));
    }
    Ok(CaptureIdentity {
        id,
        scope_digest,
        purpose,
    })
}

pub(crate) fn put_control(out: &mut Writer, value: CaptureControl) -> Result<(), ControlError> {
    put_token(out, value.token)?;
    put_identity(out, value.identity)
}
pub(crate) fn control(input: &mut Reader<'_>) -> Result<CaptureControl, ControlError> {
    Ok(CaptureControl {
        token: token(input)?,
        identity: identity(input)?,
    })
}

impl CaptureProof {
    /// Canonical versioned bytes of the original receipt facts.
    pub fn encode(&self) -> Result<Vec<u8>, ControlError> {
        if self.original_namespace <= 0
            || self.generation <= 0
            || self.revision < 0
            || self.incarnation == 0
            || self.attempt == [0; 32]
            || self.target_snapshot.is_empty()
            || self.target_snapshot.len() > SNAPSHOT_LIMIT
        {
            return Err(ControlError("capture proof fields"));
        }
        let snapshot = crate::snapshot::decode(&self.target_snapshot)?;
        if crate::snapshot::encode(&snapshot)? != self.target_snapshot {
            return Err(ControlError("capture target snapshot"));
        }
        let mut out = Writer::new(MAGIC);
        put_identity(&mut out, self.identity)?;
        out.put(&self.workspace.to_bytes())?;
        out.put(&self.original_namespace.to_be_bytes())?;
        out.put(&self.generation.to_be_bytes())?;
        out.put(&self.revision.to_be_bytes())?;
        out.put(&self.base_root)?;
        out.put(&self.catalog)?;
        out.put(&self.incarnation.to_be_bytes())?;
        out.put(&self.attempt)?;
        out.put(&self.request_digest)?;
        out.put(&self.root.to_bytes())?;
        out.blob(&self.target_snapshot)?;
        match self.outcome {
            CaptureOutcome::Committed {
                head,
                time_ms,
                displaced,
            } => {
                out.byte(1)?;
                put_head(&mut out, Some(head))?;
                out.put(&time_ms.to_be_bytes())?;
                put_head(&mut out, displaced)?;
            }
            CaptureOutcome::UpToDate { head } => {
                out.byte(2)?;
                put_head(&mut out, head)?;
            }
        }
        Ok(out.0)
    }

    /// Decode one exact proof, rejecting noncanonical or trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, ControlError> {
        let mut input = Reader::new(bytes, MAGIC)?;
        let identity = identity(&mut input)?;
        let workspace = WorkspaceId::from_authority(input.array()?)
            .map_err(|_| ControlError("capture Workspace"))?;
        let original_namespace = i64::from_be_bytes(input.array()?);
        let generation = i64::from_be_bytes(input.array()?);
        let revision = i64::from_be_bytes(input.array()?);
        let base_root = input.array()?;
        let catalog = input.array()?;
        let incarnation = u64::from_be_bytes(input.array()?);
        let attempt = input.array()?;
        let request_digest = input.array()?;
        let root = ObjectId::from_bytes(&input.array::<32>()?)
            .map_err(|_| ControlError("capture root"))?;
        let target_snapshot = input.blob(SNAPSHOT_LIMIT)?.to_vec();
        let outcome = match input.byte()? {
            1 => CaptureOutcome::Committed {
                head: head(&mut input)?.ok_or(ControlError("capture committed head"))?,
                time_ms: i64::from_be_bytes(input.array()?),
                displaced: head(&mut input)?,
            },
            2 => CaptureOutcome::UpToDate {
                head: head(&mut input)?,
            },
            _ => return Err(ControlError("capture outcome")),
        };
        input.finish()?;
        let proof = Self {
            identity,
            workspace,
            original_namespace,
            generation,
            revision,
            base_root,
            catalog,
            incarnation,
            attempt,
            request_digest,
            root,
            target_snapshot,
            outcome,
        };
        if proof.encode()? != bytes {
            return Err(ControlError("capture proof canonical encoding"));
        }
        Ok(proof)
    }

    /// SHA-256 of the exact versioned original receipt facts.
    pub fn sha256(&self) -> Result<[u8; 32], ControlError> {
        Ok(Sha256::digest(self.encode()?).into())
    }
}

pub(crate) fn put_observation(
    out: &mut Writer,
    value: &CaptureObservation,
) -> Result<(), ControlError> {
    match value {
        CaptureObservation::Unknown => out.byte(1),
        CaptureObservation::Unpublished => out.byte(2),
        CaptureObservation::Published(proof) => {
            out.byte(3)?;
            out.blob(&proof.encode()?)
        }
    }
}

pub(crate) fn observation(input: &mut Reader<'_>) -> Result<CaptureObservation, ControlError> {
    match input.byte()? {
        1 => Ok(CaptureObservation::Unknown),
        2 => Ok(CaptureObservation::Unpublished),
        3 => Ok(CaptureObservation::Published(Box::new(
            CaptureProof::decode(input.blob(crate::wire::LIMIT)?)?,
        ))),
        _ => Err(ControlError("capture observation")),
    }
}
