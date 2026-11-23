//! Original capture requests and read-only observations on the existing channel.
use super::WorkspaceApi;
use crate::OperationFailure;
use layerfs_bridge::control::{CaptureControl, CaptureObservation, CaptureProof, Reply, Request};

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
}
