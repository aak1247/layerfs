//! One authenticated connection's bounded captured-root ownership.
use super::{CapturedRoot, Failure, Service, Success};
use layerfs_bridge::control::{CaptureReadHandle, CaptureReadResult, ControlCode, Reply, Request};

/// At most one captured-root handle on an authenticated connection. Dropping
/// the session releases that handle without closing the underlying Workspace.
#[derive(Default)]
pub struct CaptureSession {
    open: Option<(CaptureReadHandle, CapturedRoot)>,
}

impl Service {
    /// Runs a control operation with this connection's private read state.
    /// A session must never be shared between authenticated connections.
    pub fn execute_session(
        &self,
        session: &mut CaptureSession,
        request: &Request,
    ) -> Result<Success, Failure> {
        match request {
            Request::OpenCapture(open) => {
                if session.open.is_some() {
                    return Err(Failure::Rejected(
                        ControlCode::Capacity,
                        "connection already holds a capture",
                    ));
                }
                let root = self.open_captured_root(open.control, open.receipt_sha256)?;
                let id = layerfs_bridge::duplex::generate_keypair()
                    .map_err(|_| {
                        Failure::Rejected(
                            ControlCode::Failed,
                            "capture handle randomness unavailable",
                        )
                    })?
                    .public;
                let handle = CaptureReadHandle {
                    id,
                    token: open.control.token,
                    receipt_sha256: open.receipt_sha256,
                };
                session.open = Some((handle, root));
                Ok(Success::reply(Reply::CaptureOpened(handle)))
            }
            Request::ReadCapture(read) => {
                let Some((handle, root)) = session.open.as_ref() else {
                    return Err(Failure::Rejected(
                        ControlCode::Invalid,
                        "capture handle absent",
                    ));
                };
                if *handle != read.handle {
                    return Err(Failure::Rejected(
                        ControlCode::Invalid,
                        "capture handle mismatch",
                    ));
                }
                let bytes = self.read_captured_range(
                    root,
                    handle.token,
                    &read.path,
                    read.offset,
                    read.length,
                )?;
                Ok(Success::reply(Reply::CaptureRead(CaptureReadResult {
                    handle: *handle,
                    offset: read.offset,
                    bytes,
                })))
            }
            Request::CloseCapture(requested) => {
                if !session
                    .open
                    .as_ref()
                    .is_some_and(|(handle, _)| handle == requested)
                {
                    return Err(Failure::Rejected(
                        ControlCode::Invalid,
                        "capture handle mismatch",
                    ));
                }
                session.open = None;
                Ok(Success::reply(Reply::CaptureClosed(*requested)))
            }
            _ => self.execute_control(request),
        }
    }
}
