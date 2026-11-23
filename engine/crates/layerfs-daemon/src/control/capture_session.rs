//! One authenticated connection's bounded captured-root ownership.
use super::{CapturedRoot, Failure, Service, Success};
use layerfs_bridge::control::{
    CaptureLink, CapturePage, CaptureReadHandle, CaptureReadResult, ControlCode, Reply, Request,
};
use std::collections::BTreeMap;

const CURSOR_LIMIT: usize = 256;

struct Cursor {
    handle: CaptureReadHandle,
    path: String,
    after: String,
}

/// At most one captured-root handle on an authenticated connection. Dropping
/// the session releases that handle without closing the underlying Workspace.
#[derive(Default)]
pub struct CaptureSession {
    open: Option<(CaptureReadHandle, CapturedRoot)>,
    cursors: BTreeMap<[u8; 32], Cursor>,
}

impl CaptureSession {
    fn root(&self, requested: CaptureReadHandle) -> Result<&CapturedRoot, Failure> {
        match self.open.as_ref() {
            Some((handle, root)) if *handle == requested => Ok(root),
            _ => Err(Failure::Rejected(
                ControlCode::Invalid,
                "capture handle mismatch",
            )),
        }
    }
}

fn random_id() -> Result<[u8; 32], Failure> {
    let id = layerfs_bridge::duplex::generate_keypair()
        .map_err(|_| Failure::Rejected(ControlCode::Failed, "capture randomness unavailable"))?
        .public;
    if id == [0; 32] {
        return Err(Failure::Rejected(
            ControlCode::Failed,
            "capture randomness invalid",
        ));
    }
    Ok(id)
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
                let id = random_id()?;
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
                session.cursors.clear();
                Ok(Success::reply(Reply::CaptureClosed(*requested)))
            }
            Request::StatCapture(path) => {
                let root = session.root(path.handle)?;
                let value = self.stat_captured(root, path.clone())?;
                Ok(Success::reply(Reply::CaptureStat(value)))
            }
            Request::ReadlinkCapture(path) => {
                let root = session.root(path.handle)?;
                let target = self.readlink_captured(root, path.handle.token, &path.path)?;
                Ok(Success::reply(Reply::CaptureLink(CaptureLink {
                    path: path.clone(),
                    target,
                })))
            }
            Request::ListCapture(request) => {
                let root = session.root(request.path.handle)?;
                let after = match request.cursor {
                    None => None,
                    Some(id) => {
                        let cursor = session.cursors.get(&id).ok_or(Failure::Rejected(
                            ControlCode::Invalid,
                            "capture cursor absent",
                        ))?;
                        if cursor.handle != request.path.handle || cursor.path != request.path.path
                        {
                            return Err(Failure::Rejected(
                                ControlCode::Invalid,
                                "capture cursor scope",
                            ));
                        }
                        Some(cursor.after.as_str())
                    }
                };
                let page = self.list_captured(
                    root,
                    request.path.handle.token,
                    &request.path.path,
                    after,
                    request.limit,
                )?;
                let continuation = if page.next.is_some() {
                    if request.cursor.is_none() && session.cursors.len() >= CURSOR_LIMIT {
                        return Err(Failure::Rejected(
                            ControlCode::Capacity,
                            "capture cursor capacity",
                        ));
                    }
                    let id = random_id()?;
                    if session.cursors.contains_key(&id) {
                        return Err(Failure::Rejected(
                            ControlCode::Failed,
                            "capture cursor collision",
                        ));
                    }
                    Some(id)
                } else {
                    None
                };
                if let Some(id) = request.cursor {
                    session.cursors.remove(&id);
                }
                if let (Some(id), Some(after)) = (continuation, page.next) {
                    session.cursors.insert(
                        id,
                        Cursor {
                            handle: request.path.handle,
                            path: request.path.path.clone(),
                            after,
                        },
                    );
                }
                Ok(Success::reply(Reply::CapturePage(CapturePage {
                    request: request.clone(),
                    names: page.names,
                    continuation,
                })))
            }
            _ => self.execute_control(request),
        }
    }
}
