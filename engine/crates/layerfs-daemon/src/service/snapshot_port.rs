//! Captured reads through the original Workspace's Store and overlay owners.
use crate::store::StoreOperation;
use layerfs_content::filesystem::{PathName, SymlinkTarget};
use layerfs_fuse::snapshot::SnapshotSource;
use layerfs_overlay::SnapshotPin;
use layerfs_workspace::{SourceView, ViewListing, ViewStat, WorkspaceResult};
use std::io::Write;

/// This port does not acquire or release a pin. The service retains the exact
/// original owner until the mounted session and external consumer have ended.
pub struct SnapshotPort {
    operation: StoreOperation,
    view: SourceView,
    pin: SnapshotPin,
}
impl SnapshotPort {
    pub fn new(operation: StoreOperation, pin: SnapshotPin) -> WorkspaceResult<Self> {
        let view = operation.workspace().view_for_source(pin.source())?;
        let _ = view.captured(operation.overlay(), pin.reader())?;
        Ok(Self {
            operation,
            view,
            pin,
        })
    }
}
impl SnapshotSource for SnapshotPort {
    fn pin(&self) -> SnapshotPin {
        self.pin
    }
    fn root_serial(&self) -> u64 {
        self.view.root_serial()
    }
    fn stat(&self, serial: u64) -> WorkspaceResult<ViewStat> {
        self.view
            .captured(self.operation.overlay(), self.pin.reader())?
            .stat(serial)
    }
    fn lookup(&self, parent: u64, name: &PathName) -> WorkspaceResult<ViewStat> {
        self.view
            .captured(self.operation.overlay(), self.pin.reader())?
            .lookup(parent, name)
    }
    fn list(&self, parent: u64, after: Option<&[u8]>) -> WorkspaceResult<ViewListing> {
        self.view
            .captured(self.operation.overlay(), self.pin.reader())?
            .list(parent, after)
    }
    fn readlink(&self, serial: u64) -> WorkspaceResult<SymlinkTarget> {
        self.view
            .captured(self.operation.overlay(), self.pin.reader())?
            .readlink(serial)
    }
    fn read(
        &self,
        serial: u64,
        offset: u64,
        length: u32,
        sink: &mut dyn Write,
    ) -> WorkspaceResult<u64> {
        self.view
            .captured(self.operation.overlay(), self.pin.reader())?
            .read(serial, offset, length, sink)
    }
}
