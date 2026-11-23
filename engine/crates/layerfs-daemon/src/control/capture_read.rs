//! Opaque, bounded reads reachable from one proved captured root.
use super::{registry::bound, Failure, Service};
use crate::store::{BoundWorkspace, CommitError, StorePorts};
use layerfs_bridge::control::{
    Activity, CaptureControl, CaptureObservation, ControlCode, Reply, WorkspaceToken,
};
use layerfs_content::{
    file::FileView,
    filesystem::{FilesystemRead, FilesystemRootId, LogicalPath},
    object::inode_leaf::InodeKind,
    ContentError,
};
use layerfs_telemetry::timer::Timing;
use std::sync::{Arc, Weak};

const RANGE_LIMIT: u32 = 48 * 1024;

/// A root selected only from an independently verified original receipt.
/// The handle owns no Workspace lifetime and closes no Workspace on drop.
pub struct CapturedRoot {
    token: WorkspaceToken,
    workspace: Weak<BoundWorkspace>,
    pub(super) ports: Arc<StorePorts>,
    pub(super) root: FilesystemRootId,
}

pub(super) fn content(error: ContentError, ports: &StorePorts) -> Failure {
    Failure::CaptureRead(CommitError::Content {
        error,
        provider: ports.failure().ok().flatten(),
    })
}

impl Service {
    fn capture_read_workspace(
        &self,
        token: WorkspaceToken,
    ) -> Result<Arc<BoundWorkspace>, Failure> {
        #[cfg(target_os = "linux")]
        self.check_durable_state()?;
        let entries = self.entries.lock().map_err(|_| Failure::Poisoned)?;
        let value = bound(&entries, token)?;
        if !matches!(value.activity, Activity::Idle | Activity::Committing) {
            return Err(Failure::Rejected(
                ControlCode::Busy,
                "Workspace read authority unavailable",
            ));
        }
        Ok(value.workspace.clone())
    }

    pub(super) fn verify_captured(
        &self,
        handle: &CapturedRoot,
        token: WorkspaceToken,
    ) -> Result<(), Failure> {
        if token != handle.token {
            return Err(Failure::Rejected(
                ControlCode::Invalid,
                "capture read scope",
            ));
        }
        let current = self.capture_read_workspace(token)?;
        if !Weak::ptr_eq(&Arc::downgrade(&current), &handle.workspace) {
            return Err(Failure::Rejected(
                ControlCode::Invalid,
                "capture Workspace changed",
            ));
        }
        Ok(())
    }

    /// Opens the original receipt's root, never a caller-selected object ID.
    /// The application Owner must independently authorize this and each read.
    pub fn open_captured_root(
        &self,
        control: CaptureControl,
        expected_receipt_sha256: [u8; 32],
    ) -> Result<CapturedRoot, Failure> {
        let workspace = self.capture_read_workspace(control.token)?;
        let observation = self.observe_capture(control)?;
        let Reply::Capture(CaptureObservation::Published(proof)) = observation.reply else {
            return Err(Failure::Rejected(
                ControlCode::Missing,
                "original capture receipt unavailable",
            ));
        };
        if proof
            .sha256()
            .map_err(|_| Failure::Rejected(ControlCode::Invalid, "invalid capture proof"))?
            != expected_receipt_sha256
        {
            return Err(Failure::Rejected(
                ControlCode::Invalid,
                "capture receipt mismatch",
            ));
        }
        let ports = workspace
            .operation()
            .map_err(Failure::Workspace)?
            .ports()
            .fresh();
        let current = self.capture_read_workspace(control.token)?;
        if !Arc::ptr_eq(&workspace, &current) {
            return Err(Failure::Rejected(
                ControlCode::Invalid,
                "Workspace changed during capture open",
            ));
        }
        Ok(CapturedRoot {
            token: control.token,
            workspace: Arc::downgrade(&workspace),
            ports,
            root: FilesystemRootId(proof.root),
        })
    }

    /// Reads at most 48 KiB by canonical path from the original captured root.
    /// The original current token is rechecked before and after every window.
    pub fn read_captured_range(
        &self,
        handle: &CapturedRoot,
        token: WorkspaceToken,
        path: &str,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, Failure> {
        if length > RANGE_LIMIT {
            return Err(Failure::Rejected(
                ControlCode::Invalid,
                "capture read scope or range",
            ));
        }
        self.verify_captured(handle, token)?;
        let end = offset
            .checked_add(u64::from(length))
            .ok_or(Failure::Rejected(
                ControlCode::Invalid,
                "capture read range overflow",
            ))?;
        let logical = LogicalPath::new(path).map_err(|error| content(error, &handle.ports))?;
        let mut tree = FilesystemRead::new(handle.ports.as_ref(), handle.root)
            .map_err(|error| content(error, &handle.ports))?;
        let inode = tree
            .resolve(&logical)
            .map_err(|error| content(error, &handle.ports))?
            .value;
        if inode.kind != InodeKind::RegularFile {
            return Err(Failure::Rejected(
                ControlCode::Invalid,
                "capture path is not a regular file",
            ));
        }
        let mut bytes = Vec::with_capacity(length as usize);
        Timing::disabled("capture.read", |scope| {
            let view = FileView::open(
                handle.ports.as_ref(),
                inode.content_root,
                scope.child("open"),
            )?;
            view.read_range(handle.ports.as_ref(), offset..end, &mut bytes, scope)
        })
        .0
        .map_err(|error| content(error, &handle.ports))?;
        if bytes.len() != length as usize {
            return Err(Failure::Rejected(
                ControlCode::Unknown,
                "capture read length changed",
            ));
        }
        self.verify_captured(handle, token)?;
        Ok(bytes)
    }
}
