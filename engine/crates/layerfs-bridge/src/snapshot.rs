//! Versioned encoding of a Branch snapshot for durable Workspace metadata.
use crate::{
    control::ControlError,
    wire::{Reader, Writer},
};
use layerfs_history::BranchSnapshot;

const MAGIC: &[u8] = b"LFSB\x01";

/// Encodes the complete snapshot using the existing bounded History codec.
pub fn encode(snapshot: &BranchSnapshot) -> Result<Vec<u8>, ControlError> {
    let mut writer = Writer::new(MAGIC);
    crate::control_history::put_binding(&mut writer, snapshot)?;
    Ok(writer.0)
}

/// Decodes one complete snapshot and rejects other versions or trailing bytes.
pub fn decode(bytes: &[u8]) -> Result<BranchSnapshot, ControlError> {
    let mut reader = Reader::new(bytes, MAGIC)?;
    let snapshot = crate::control_history::binding(&mut reader)?;
    reader.finish()?;
    Ok(snapshot)
}
