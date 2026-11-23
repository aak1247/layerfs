//! Original capture requests and read-only observations on the existing channel.
use super::WorkspaceApi;
use crate::OperationFailure;
use layerfs_bridge::control::{
    CaptureControl, CaptureObservation, CaptureOpen, CaptureProof, CaptureReadHandle,
    CaptureReadRequest, Reply, Request,
};

/// A captured root owned by the same authenticated control connection.
/// Dropping the connection releases the server-side handle.
#[derive(Debug)]
pub struct CapturedWorkspace {
    handle: CaptureReadHandle,
}

impl WorkspaceApi<'_> {
    /// Sends one capture attempt. An unknown delivery must only be observed.
    pub fn capture_external(
        &mut self,
        control: CaptureControl,
    ) -> Result<CaptureProof, Box<OperationFailure>> {
        let request = Request::Capture(control);
        match self.exchange(request.clone())? {
            Reply::Capture(CaptureObservation::Published(proof)) => Ok(*proof),
            reply => Err(OperationFailure::unexpected(request, reply)),
        }
    }

    /// Observes the original attempt without issuing another capture.
    pub fn observe_external_capture(
        &mut self,
        control: CaptureControl,
    ) -> Result<CaptureObservation, Box<OperationFailure>> {
        let request = Request::ObserveCapture(control);
        match self.exchange(request.clone())? {
            Reply::Capture(observation) => Ok(observation),
            reply => Err(OperationFailure::unexpected(request, reply)),
        }
    }

    /// Opens the original receipt's root on this control connection.
    /// The caller must separately check its current Owner authorization.
    pub fn open_captured(
        &mut self,
        control: CaptureControl,
        receipt_sha256: [u8; 32],
    ) -> Result<CapturedWorkspace, Box<OperationFailure>> {
        let request = Request::OpenCapture(CaptureOpen {
            control,
            receipt_sha256,
        });
        match self.exchange(request.clone())? {
            Reply::CaptureOpened(handle) => Ok(CapturedWorkspace { handle }),
            reply => Err(OperationFailure::unexpected(request, reply)),
        }
    }

    /// Reads one bounded file window using the same current authorization.
    pub fn read_captured(
        &mut self,
        captured: &CapturedWorkspace,
        path: &str,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, Box<OperationFailure>> {
        let request = Request::ReadCapture(CaptureReadRequest {
            handle: captured.handle,
            path: path.to_owned(),
            offset,
            length,
        });
        match self.exchange(request.clone())? {
            Reply::CaptureRead(result) => Ok(result.bytes),
            reply => Err(OperationFailure::unexpected(request, reply)),
        }
    }

    /// Releases this connection's handle without closing the Workspace.
    pub fn close_captured(
        &mut self,
        captured: &CapturedWorkspace,
    ) -> Result<(), Box<OperationFailure>> {
        let request = Request::CloseCapture(captured.handle);
        match self.exchange(request.clone())? {
            Reply::CaptureClosed(_) => Ok(()),
            reply => Err(OperationFailure::unexpected(request, reply)),
        }
    }
}
