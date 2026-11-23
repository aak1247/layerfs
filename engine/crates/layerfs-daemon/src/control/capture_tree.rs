//! Metadata and directory pages resolved only beneath a proved captured root.
use super::{capture_read::content, CapturedRoot, Failure, Service};
use layerfs_bridge::control::{CaptureKind, CapturePath, CaptureStat, ControlCode, WorkspaceToken};
use layerfs_content::{
    file::FileView,
    filesystem::{FilesystemRead, LogicalPath, PathName},
    object::inode_leaf::InodeKind,
};
use layerfs_telemetry::timer::Timing;

pub(super) struct Names {
    pub names: Vec<String>,
    pub next: Option<String>,
}

impl Service {
    pub(super) fn stat_captured(
        &self,
        root: &CapturedRoot,
        path: CapturePath,
    ) -> Result<CaptureStat, Failure> {
        let token = path.handle.token;
        self.verify_captured(root, token)?;
        let logical = LogicalPath::new(&path.path).map_err(|e| content(e, &root.ports))?;
        let mut tree = FilesystemRead::new(root.ports.as_ref(), root.root)
            .map_err(|e| content(e, &root.ports))?;
        let resolved = tree
            .resolve(&logical)
            .map_err(|e| content(e, &root.ports))?;
        let metadata = tree
            .read_portable_inode(resolved.serial)
            .map_err(|e| content(e, &root.ports))?;
        let portable_attributes_only = tree
            .attribute_keys(&logical)
            .map_err(|e| content(e, &root.ports))?
            .iter()
            .all(|key| key.is_portable());
        let (kind, size) = match resolved.value.kind {
            InodeKind::Directory => (CaptureKind::Directory, 0),
            InodeKind::Symlink => (
                CaptureKind::Symlink,
                tree.readlink_inode(resolved.serial)
                    .map_err(|e| content(e, &root.ports))?
                    .as_bytes()
                    .len() as u64,
            ),
            InodeKind::RegularFile => {
                let size = Timing::disabled("capture.stat", |scope| {
                    FileView::open(
                        root.ports.as_ref(),
                        resolved.value.content_root,
                        scope.child("open"),
                    )
                    .map(|view| view.logical_len())
                })
                .0
                .map_err(|e| content(e, &root.ports))?;
                (CaptureKind::File, size)
            }
        };
        self.verify_captured(root, token)?;
        Ok(CaptureStat {
            path,
            kind,
            mode: metadata.mode,
            mtime_seconds: metadata.mtime_seconds,
            mtime_nanoseconds: metadata.mtime_nanoseconds,
            namespace_references: resolved.value.namespace_ref_count,
            size,
            portable_attributes_only,
        })
    }

    pub(super) fn list_captured(
        &self,
        root: &CapturedRoot,
        token: WorkspaceToken,
        path: &str,
        after: Option<&str>,
        limit: u16,
    ) -> Result<Names, Failure> {
        if limit == 0 || limit > layerfs_bridge::control::CAPTURE_LIST_LIMIT {
            return Err(Failure::Rejected(
                ControlCode::Invalid,
                "capture list limit",
            ));
        }
        self.verify_captured(root, token)?;
        let logical = LogicalPath::new(path).map_err(|e| content(e, &root.ports))?;
        let after = after
            .map(PathName::new)
            .transpose()
            .map_err(|e| content(e, &root.ports))?;
        let mut tree = FilesystemRead::new(root.ports.as_ref(), root.root)
            .map_err(|e| content(e, &root.ports))?;
        let page = tree
            .list(&logical, after.as_ref(), usize::from(limit), 16 * 1024)
            .map_err(|e| content(e, &root.ports))?;
        let names = page
            .entries
            .into_iter()
            .map(|(name, _)| name.as_str().to_owned())
            .collect();
        let next = page.continuation.map(|name| name.as_str().to_owned());
        self.verify_captured(root, token)?;
        Ok(Names { names, next })
    }

    pub(super) fn readlink_captured(
        &self,
        root: &CapturedRoot,
        token: WorkspaceToken,
        path: &str,
    ) -> Result<Vec<u8>, Failure> {
        self.verify_captured(root, token)?;
        let logical = LogicalPath::new(path).map_err(|e| content(e, &root.ports))?;
        let mut tree = FilesystemRead::new(root.ports.as_ref(), root.root)
            .map_err(|e| content(e, &root.ports))?;
        let target = tree
            .readlink(&logical)
            .map_err(|e| content(e, &root.ports))?
            .as_bytes()
            .to_vec();
        self.verify_captured(root, token)?;
        Ok(target)
    }
}
