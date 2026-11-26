//! Read a retained capture over its exact immutable base, without publication.
use crate::{
    OverlayCapturedNamespace, OverlayFileRead, OverlayRead, SourceView, ViewListing, ViewStat,
    WorkspaceResult,
};
use layerfs_content::filesystem::{PathName, SymlinkTarget};
use layerfs_overlay::{
    BaseSource, CapturedReader, Cell, DirectoryEntry, DirectoryEntryWindow, Inode, LocalRead,
    OverlayError,
};
use std::io::Write;

/// A view borrows the exact captured reader and base source; it does not own,
/// release or reacquire either. Both original owners must remain held until
/// every consumer and its output have finished.
pub struct CapturedView<'a, P> {
    view: &'a SourceView,
    overlay: CapturedOverlay<'a, P>,
}

struct CapturedOverlay<'a, P> {
    source: BaseSource,
    reader: CapturedReader,
    root_serial: u64,
    port: &'a P,
}

impl SourceView {
    /// Bind a captured reader to this already-held source and its real base.
    /// A different engine, root or installed floor cannot select this view.
    pub fn captured<'a, P>(
        &'a self,
        port: &'a P,
        reader: CapturedReader,
    ) -> WorkspaceResult<CapturedView<'a, P>>
    where
        P: OverlayCapturedNamespace + OverlayFileRead + OverlayRead,
    {
        let source = self.source();
        if reader.capture().engine() != source.engine()
            || reader.root() != source.root()
            || reader.installed_floor() != source.installed_floor()
            || self.base().identity().0.to_bytes() != reader.root()
        {
            return Err(OverlayError::Stale.into());
        }
        // This bounded original-source observation rejects a released or
        // substituted source; its active row is never used for the view.
        let _ = port.inode(source, self.root_serial())?;
        // A copied reader token must still name a retained reader, including
        // when the captured root is inherited entirely from the base.
        let _ = port.captured_inode(reader, self.root_serial())?;
        Ok(CapturedView {
            view: self,
            overlay: CapturedOverlay {
                source,
                reader,
                root_serial: self.root_serial(),
                port,
            },
        })
    }
}

impl<P: OverlayCapturedNamespace + OverlayFileRead + OverlayRead> CapturedView<'_, P> {
    pub fn root_serial(&self) -> u64 {
        self.view.root_serial()
    }
    pub fn stat(&self, serial: u64) -> WorkspaceResult<ViewStat> {
        self.view.stat(&self.overlay, serial)
    }
    pub fn lookup(&self, parent: u64, name: &PathName) -> WorkspaceResult<ViewStat> {
        self.view.lookup(&self.overlay, parent, name)
    }
    pub fn list(&self, parent: u64, after: Option<&[u8]>) -> WorkspaceResult<ViewListing> {
        self.view.list(&self.overlay, parent, after)
    }
    pub fn readlink(&self, serial: u64) -> WorkspaceResult<SymlinkTarget> {
        self.view.readlink(&self.overlay, serial)
    }
    pub fn read(
        &self,
        serial: u64,
        offset: u64,
        length: u32,
        sink: &mut dyn Write,
    ) -> WorkspaceResult<u64> {
        self.overlay.check(self.view.source())?;
        self.view.read_captured(
            self.overlay.port,
            self.overlay.reader,
            serial,
            offset,
            length,
            sink,
        )
    }
}

impl<P: OverlayCapturedNamespace + OverlayFileRead + OverlayRead> CapturedOverlay<'_, P> {
    fn check(&self, source: BaseSource) -> WorkspaceResult<()> {
        if source != self.source {
            return Err(OverlayError::Stale.into());
        }
        let _ = self.port.inode(source, self.root_serial)?;
        Ok(())
    }
}

impl<P: OverlayCapturedNamespace + OverlayFileRead + OverlayRead> OverlayRead
    for CapturedOverlay<'_, P>
{
    fn inode(&self, source: BaseSource, serial: u64) -> WorkspaceResult<Option<Inode>> {
        self.check(source)?;
        self.port.captured_inode(self.reader, serial)
    }
    fn directory_entry(
        &self,
        source: BaseSource,
        parent: u64,
        name: &[u8],
    ) -> WorkspaceResult<Option<DirectoryEntry>> {
        self.check(source)?;
        self.port
            .captured_directory_entry(self.reader, parent, name)
    }
    fn directory_entries(
        &self,
        source: BaseSource,
        parent: u64,
        after: Option<&[u8]>,
    ) -> WorkspaceResult<DirectoryEntryWindow> {
        self.check(source)?;
        Ok(DirectoryEntryWindow {
            source,
            parent,
            parent_inode: self.port.captured_inode(self.reader, parent)?,
            active: Vec::new(),
            captured: self.port.captured_directory_entries(
                self.reader,
                parent,
                after.map(Vec::from),
            )?,
        })
    }
    fn cell(
        &self,
        _source: BaseSource,
        _serial: u64,
        _generation: u64,
        _offset: u64,
    ) -> WorkspaceResult<Option<Cell>> {
        Err(OverlayError::Invalid("captured view has no mutable cell port").into())
    }
    fn read(
        &self,
        source: BaseSource,
        serial: u64,
        offset: u64,
        length: u32,
    ) -> WorkspaceResult<Option<LocalRead>> {
        self.check(source)?;
        self.port.captured_read(self.reader, serial, offset, length)
    }
}
