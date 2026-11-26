use layerfs_content::filesystem::{PathName, SymlinkTarget};
use layerfs_overlay::SnapshotPin;
use layerfs_workspace::{ViewListing, ViewStat, WorkspaceResult};
use std::io::Write;

/// One exact captured source; the original owner retains its pin until all
/// mounted requests and their consumers have actually ended.
pub trait SnapshotSource: Send + Sync {
    fn pin(&self) -> SnapshotPin;
    fn root_serial(&self) -> u64;
    fn stat(&self, serial: u64) -> WorkspaceResult<ViewStat>;
    fn lookup(&self, parent: u64, name: &PathName) -> WorkspaceResult<ViewStat>;
    fn list(&self, parent: u64, after: Option<&[u8]>) -> WorkspaceResult<ViewListing>;
    fn readlink(&self, serial: u64) -> WorkspaceResult<SymlinkTarget>;
    fn read(
        &self,
        serial: u64,
        offset: u64,
        length: u32,
        sink: &mut dyn Write,
    ) -> WorkspaceResult<u64>;
}
