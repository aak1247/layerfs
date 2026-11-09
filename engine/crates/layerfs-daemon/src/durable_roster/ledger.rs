//! A fixed-width hash chain and durable head prove discovery completeness.
use super::{
    files::RosterError,
    record::{decode_record, record_bytes, Authority, Event, RECORD_LEN},
};

const HEAD_MAGIC: &[u8; 8] = b"LFSHEAD1";
pub(super) const HEAD_LEN: usize = 8 + 32 + 32 + 8 + 32 + 32;
pub(super) const ENTRY_LEN: usize = 8 + RECORD_LEN + 32;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct Head {
    pub count: u64,
    pub digest: [u8; 32],
}
impl Head {
    pub const fn empty() -> Self {
        Self {
            count: 0,
            digest: [0; 32],
        }
    }
    pub fn size(self) -> Result<u64, RosterError> {
        self.count
            .checked_mul(ENTRY_LEN as u64)
            .ok_or(RosterError::Invalid("roster ledger size"))
    }
}

pub(super) fn head_bytes(authority: Authority, head: Head) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEAD_LEN);
    bytes.extend_from_slice(HEAD_MAGIC);
    bytes.extend_from_slice(&authority.sandbox);
    bytes.extend_from_slice(&authority.store);
    bytes.extend_from_slice(&head.count.to_be_bytes());
    bytes.extend_from_slice(&head.digest);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    bytes
}

pub(super) fn decode_head(authority: Authority, bytes: &[u8]) -> Result<Head, RosterError> {
    if bytes.len() != HEAD_LEN
        || &bytes[..8] != HEAD_MAGIC
        || bytes[8..40] != authority.sandbox
        || bytes[40..72] != authority.store
        || blake3::hash(&bytes[..HEAD_LEN - 32]).as_bytes() != &bytes[HEAD_LEN - 32..]
    {
        return Err(RosterError::Invalid("roster head integrity"));
    }
    let count = u64::from_be_bytes(bytes[72..80].try_into().expect("fixed head width"));
    let digest = bytes[80..112].try_into().expect("fixed digest width");
    let head = Head { count, digest };
    head.size()?;
    if count == 0 && digest != [0; 32] {
        return Err(RosterError::Invalid("roster empty head"));
    }
    Ok(head)
}

pub(super) fn entry_bytes(
    authority: Authority,
    previous: Head,
    event: Event,
) -> Result<(Head, Vec<u8>), RosterError> {
    let count = previous
        .count
        .checked_add(1)
        .ok_or(RosterError::Invalid("roster sequence exhausted"))?;
    let mut bytes = Vec::with_capacity(ENTRY_LEN);
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.extend_from_slice(&record_bytes(authority, event));
    let digest = entry_digest(previous.digest, &bytes);
    bytes.extend_from_slice(&digest);
    let head = Head { count, digest };
    head.size()?;
    Ok((head, bytes))
}

pub(super) fn decode_entry(
    authority: Authority,
    previous: Head,
    bytes: &[u8],
) -> Result<(Head, Event), RosterError> {
    if bytes.len() != ENTRY_LEN {
        return Err(RosterError::Invalid("roster ledger entry size"));
    }
    let event = decode_record(authority, &bytes[8..8 + RECORD_LEN])?;
    let (head, expected) = entry_bytes(authority, previous, event)?;
    if bytes != expected {
        return Err(RosterError::Invalid("roster ledger chain"));
    }
    Ok((head, event))
}

fn entry_digest(previous: [u8; 32], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"layerfs/durable-roster-ledger/v1\0");
    hasher.update(&previous);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}
