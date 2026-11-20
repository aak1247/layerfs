//! Direct connection operations, each one counted short job on the calling thread.
use crate::overlay::owner::Readiness;
use crate::{OwnerClient, OwnerError, ServiceClass};
use layerfs_overlay::{
    BaseSource, Capture, CapturedReader, OperationOwner, OverlayError, RecordedCapture,
};
use layerfs_workspace::Route;

impl OwnerClient {
    pub fn acquire_operation(
        &self,
        route: Route,
        request: u64,
    ) -> Result<layerfs_overlay::OperationOwner, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.acquire_operation(request)
        })
    }
    pub(crate) fn acquire_copy_operation(
        &self,
        route: Route,
        request: u64,
    ) -> Result<OperationOwner, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.acquire_copy_operation(request)
        })
    }
    pub(crate) fn acquire_captured_operation(
        &self,
        route: Route,
        request: u64,
        reader: CapturedReader,
    ) -> Result<OperationOwner, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.acquire_captured_operation(request, reader)
        })
    }
    pub fn release_operation(&self, route: Route, owner: OperationOwner) -> Result<(), OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.release_operation(owner)
        })
    }
    pub fn acquire_captured_reader(
        &self,
        route: Route,
        capture: Capture,
        request: u64,
    ) -> Result<layerfs_overlay::CapturedReader, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.acquire_captured_reader(capture, request)
        })
    }
    /// Acquires construction's one reader for the exact recorded capture.
    pub fn acquire_recorded_captured_reader(
        &self,
        route: Route,
        recorded: RecordedCapture,
        request: u64,
    ) -> Result<CapturedReader, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.acquire_recorded_captured_reader(recorded, request)
        })
    }
    /// Acquires construction's one operation for that exact live reader.
    pub fn acquire_recorded_captured_operation(
        &self,
        route: Route,
        recorded: RecordedCapture,
        request: u64,
        reader: CapturedReader,
    ) -> Result<OperationOwner, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.acquire_recorded_captured_operation(recorded, request, reader)
        })
    }
    pub fn release_captured_reader(
        &self,
        route: Route,
        reader: CapturedReader,
    ) -> Result<(), OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.release_captured_reader(reader)
        })
    }
    pub fn state(&self, route: Route) -> Result<layerfs_overlay::WorkspaceState, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| db.state())
    }
    /// One window of the active generation's changed inodes.
    pub fn active_inodes(
        &self,
        route: Route,
        after: u64,
    ) -> Result<Vec<layerfs_overlay::Inode>, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.active_inodes(after)
        })
    }
    /// One window of the active generation's changed name bindings.
    pub fn active_directory_entries(
        &self,
        route: Route,
        after: Option<(u64, Vec<u8>)>,
    ) -> Result<Vec<layerfs_overlay::DirectoryEntry>, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.active_directory_entries(after.as_ref().map(|(p, n)| (*p, n.as_slice())))
        })
    }
    pub fn release_base_source(&self, route: Route, source: BaseSource) -> Result<(), OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.release_base_source(source)
        })
    }
    pub fn cleanup_state(&self, route: Route) -> Result<layerfs_overlay::CleanupState, OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.cleanup_state()
        })
    }
    pub fn close(&self, route: Route) -> Result<(), OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| db.close())
    }
    pub fn capture(&self, route: Route) -> Result<layerfs_overlay::Capture, OwnerError> {
        self.call(
            route,
            ServiceClass::General,
            Some(Readiness::Capture),
            move |db| db.capture(),
        )
    }
    /// Captures once with its frozen original Store and Workspace context.
    /// The caller must authenticate that context against the bound Store.
    pub fn capture_checkpoint(
        &self,
        route: Route,
        workspace: [u8; 32],
        catalog: [u8; 32],
        incarnation: u64,
        snapshot: Vec<u8>,
    ) -> Result<RecordedCapture, OwnerError> {
        self.call(
            route,
            ServiceClass::General,
            Some(Readiness::Capture),
            move |db| db.capture_recorded(workspace, catalog, incarnation, &snapshot),
        )
    }
    /// Freezes one publication before the caller's single Store attempt.
    pub fn prepare_checkpoint_publication(
        &self,
        route: Route,
        recorded: RecordedCapture,
        request: Vec<u8>,
        request_digest: [u8; 32],
        candidate: [u8; 32],
        target_snapshot: Vec<u8>,
    ) -> Result<(), OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.prepare_checkpoint_publication(
                recorded,
                &request,
                request_digest,
                candidate,
                &target_snapshot,
            )
        })
    }
    pub fn resolve_failed(&self, route: Route, capture: Capture) -> Result<(), OwnerError> {
        self.call(route, ServiceClass::General, None, move |db| {
            db.resolve_failed_capture(capture)
        })
    }
    pub fn namespace(
        &self,
        route: Route,
        job: Box<layerfs_workspace::NamespaceJob>,
    ) -> Result<layerfs_workspace::JobOutcome, OwnerError> {
        self.call_observed(Some(route), ServiceClass::Mutation, None, move |db, _| {
            if route.engine() != job.source().engine() {
                return Err(OwnerError::Overlay(OverlayError::Stale));
            }
            job.perform(db)
                .map_err(|cause| OwnerError::Workspace(Box::new(cause)))
        })
        .map(|(value, _)| value)
    }
    pub fn install_prepared(
        &self,
        route: Route,
        workspace: std::sync::Arc<layerfs_workspace::Workspace>,
        input: layerfs_workspace::PreparedBase,
    ) -> Result<(), OwnerError> {
        self.install_prepared_recorded(route, workspace, input, None)
    }
    pub(crate) fn install_prepared_recorded(
        &self,
        route: Route,
        workspace: std::sync::Arc<layerfs_workspace::Workspace>,
        input: layerfs_workspace::PreparedBase,
        durable_snapshot: Option<Vec<u8>>,
    ) -> Result<(), OwnerError> {
        let ready = Some(Readiness::Install(input.capture()));
        self.call_observed(Some(route), ServiceClass::General, ready, move |db, _| {
            if route != workspace.route() {
                return Err(OwnerError::Install {
                    cause: Box::new(OverlayError::Stale.into()),
                    input: Box::new(input),
                    durable_snapshot,
                });
            }
            let installed = match &durable_snapshot {
                Some(snapshot) if db.profile().has_durable_binding() => {
                    workspace.install_prepared_base_recorded(db, input, snapshot)
                }
                _ => workspace.install_prepared_base(db, input),
            };
            installed.map_err(|(cause, input)| OwnerError::Install {
                cause: Box::new(cause),
                input: Box::new(input),
                durable_snapshot,
            })
        })
        .map(|(value, _)| value)
    }
}
