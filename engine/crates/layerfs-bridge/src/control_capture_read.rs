//! Connection-local captured-root handles and bounded path reads.
use crate::{
    control::{CaptureControl, ControlError, WorkspaceToken},
    control_history::{put_token, token},
    wire::{Reader, Writer},
};
use layerfs_content::filesystem::LogicalPath;

/// Maximum bytes returned in a single authenticated control response.
pub const CAPTURE_READ_LIMIT: u32 = 48 * 1024;

/// Opens only the root identified by this original receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureOpen {
    /// Original capture identity under the current Workspace token.
    pub control: CaptureControl,
    /// SHA-256 of its independently verified original receipt proof.
    pub receipt_sha256: [u8; 32],
}

/// Opaque identifier owned by one authenticated connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureReadHandle {
    /// Random identifier scoped to the authenticated connection.
    pub id: [u8; 32],
    /// The original current token, checked for every window.
    pub token: WorkspaceToken,
    /// Digest of the original receipt selected at open.
    pub receipt_sha256: [u8; 32],
}

/// A bounded range of a regular file reachable by canonical path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureReadRequest {
    /// The exact handle returned on this connection.
    pub handle: CaptureReadHandle,
    /// Canonical path relative to the captured root.
    pub path: String,
    /// Beginning of the requested file range.
    pub offset: u64,
    /// Requested length, no greater than [`CAPTURE_READ_LIMIT`].
    pub length: u32,
}

/// Exact bytes of the requested window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureReadResult {
    /// The original handle.
    pub handle: CaptureReadHandle,
    /// The original requested offset.
    pub offset: u64,
    /// Exactly the requested number of bytes.
    pub bytes: Vec<u8>,
}

fn valid_digest(value: &[u8; 32]) -> Result<(), ControlError> {
    if *value == [0; 32] {
        Err(ControlError("capture read identity"))
    } else {
        Ok(())
    }
}

pub(crate) fn put_open(out: &mut Writer, value: CaptureOpen) -> Result<(), ControlError> {
    valid_digest(&value.receipt_sha256)?;
    crate::control_capture::put_control(out, value.control)?;
    out.put(&value.receipt_sha256)
}
pub(crate) fn open(input: &mut Reader<'_>) -> Result<CaptureOpen, ControlError> {
    let control = crate::control_capture::control(input)?;
    let receipt_sha256 = input.array()?;
    valid_digest(&receipt_sha256)?;
    Ok(CaptureOpen {
        control,
        receipt_sha256,
    })
}
pub(crate) fn put_handle(out: &mut Writer, value: CaptureReadHandle) -> Result<(), ControlError> {
    valid_digest(&value.id)?;
    valid_digest(&value.receipt_sha256)?;
    out.put(&value.id)?;
    put_token(out, value.token)?;
    out.put(&value.receipt_sha256)
}
pub(crate) fn handle(input: &mut Reader<'_>) -> Result<CaptureReadHandle, ControlError> {
    let id = input.array()?;
    let token = token(input)?;
    let receipt_sha256 = input.array()?;
    valid_digest(&id)?;
    valid_digest(&receipt_sha256)?;
    Ok(CaptureReadHandle {
        id,
        token,
        receipt_sha256,
    })
}
fn valid_range(path: &str, offset: u64, length: u32) -> Result<(), ControlError> {
    LogicalPath::new(path).map_err(|_| ControlError("capture read path"))?;
    if length > CAPTURE_READ_LIMIT || offset.checked_add(u64::from(length)).is_none() {
        return Err(ControlError("capture read range"));
    }
    Ok(())
}
pub(crate) fn put_read(out: &mut Writer, value: &CaptureReadRequest) -> Result<(), ControlError> {
    valid_range(&value.path, value.offset, value.length)?;
    put_handle(out, value.handle)?;
    out.blob(value.path.as_bytes())?;
    out.put(&value.offset.to_be_bytes())?;
    out.put(&value.length.to_be_bytes())
}
pub(crate) fn read(input: &mut Reader<'_>) -> Result<CaptureReadRequest, ControlError> {
    let handle = handle(input)?;
    let path = input.text(4096)?;
    let offset = u64::from_be_bytes(input.array()?);
    let length = u32::from_be_bytes(input.array()?);
    valid_range(&path, offset, length)?;
    Ok(CaptureReadRequest {
        handle,
        path,
        offset,
        length,
    })
}
pub(crate) fn put_result(out: &mut Writer, value: &CaptureReadResult) -> Result<(), ControlError> {
    if value.bytes.len() > CAPTURE_READ_LIMIT as usize {
        return Err(ControlError("capture read result limit"));
    }
    put_handle(out, value.handle)?;
    out.put(&value.offset.to_be_bytes())?;
    out.blob(&value.bytes)
}
pub(crate) fn result(input: &mut Reader<'_>) -> Result<CaptureReadResult, ControlError> {
    let handle = handle(input)?;
    let offset = u64::from_be_bytes(input.array()?);
    let bytes = input.blob(CAPTURE_READ_LIMIT as usize)?.to_vec();
    Ok(CaptureReadResult {
        handle,
        offset,
        bytes,
    })
}
