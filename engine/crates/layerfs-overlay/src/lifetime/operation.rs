//! Engine-minted operation ownership and independent bounded operation_record custody.
use crate::{
    contract::custody::OperationPurpose, db::integer, CapturedReader, OperationOwner, Overlay,
    OverlayError, OverlayResult, RecordedCapture,
};
impl Overlay {
    /// One acquisition attempt. Query retained custody by request after a lost reply.
    pub fn acquire_operation(&self, request: u64) -> OverlayResult<OperationOwner> {
        self.acquire_operation_for(request, OperationPurpose::Unknown, None, None)
    }
    /// Records private copy scratch; it does not establish an operation outcome.
    pub fn acquire_copy_operation(&self, request: u64) -> OverlayResult<OperationOwner> {
        self.acquire_operation_for(request, OperationPurpose::CopyScratch, None, None)
    }
    /// Binds construction scratch to its exact live captured reader.
    pub fn acquire_captured_operation(
        &self,
        request: u64,
        reader: CapturedReader,
    ) -> OverlayResult<OperationOwner> {
        self.acquire_operation_for(
            request,
            OperationPurpose::CapturedConstruction,
            Some(reader),
            None,
        )
    }
    /// Acquires construction custody for the exact recorded checkpoint reader.
    pub fn acquire_recorded_captured_operation(
        &self,
        recorded: RecordedCapture,
        request: u64,
        reader: CapturedReader,
    ) -> OverlayResult<OperationOwner> {
        self.acquire_operation_for(
            request,
            OperationPurpose::CapturedConstruction,
            Some(reader),
            Some(recorded),
        )
    }
    fn acquire_operation_for(
        &self,
        request: u64,
        purpose: OperationPurpose,
        reader: Option<CapturedReader>,
        recorded: Option<RecordedCapture>,
    ) -> OverlayResult<OperationOwner> {
        let request = integer(request)?;
        if request == 0 {
            return Err(OverlayError::Invalid("zero operation request"));
        }
        self.atomic(|| {
            self.live()?;
            if let Some(reader) = reader {
                if recorded.is_none() && crate::database::durable_attempt::pending(self)? {
                    return Err(OverlayError::Invalid(
                        "recorded checkpoint operation required",
                    ));
                }
                self.check_captured_reader(reader)?;
                self.checked_capture(reader.capture)?;
                if reader.capture.generation.0 != request {
                    return Err(OverlayError::Invalid("captured operation request differs"));
                }
            }
            let owner = self.mint_owner()?;
            let value = OperationOwner {
                engine: self.identity,
                owner,
                recorded,
            };
            self.open.borrow_mut().operations.insert(
                owner,
                request,
                value,
                "duplicate operation request",
            )?;
            crate::database::durable_owners::acquire_operation(
                self, owner, request, purpose, reader,
            )?;
            if let (Some(recorded), Some(reader)) = (recorded, reader) {
                crate::database::durable_attempt::acquire_operation(
                    self, recorded, reader, request, owner,
                )?;
            }
            Ok(value)
        })
    }
    pub fn retained_operation(&self, request: u64) -> OverlayResult<Option<OperationOwner>> {
        self.state()?;
        Ok(self.open.borrow().operations.retained(integer(request)?))
    }
    pub(crate) fn check_operation(&self, owner: OperationOwner) -> OverlayResult<()> {
        self.state()?;
        self.operation_custody(owner)
    }
    /// The owner half of `check_operation`.
    fn operation_custody(&self, owner: OperationOwner) -> OverlayResult<()> {
        if self
            .open
            .borrow()
            .operations
            .get(owner.owner)
            .map(|(held, _)| held)
            != Some(owner)
        {
            return Err(OverlayError::Stale);
        }
        Ok(())
    }
    /// The whole fence of one indexed operation_record job: current runtime
    /// Workspace state and the exact journaled operation owner.
    /// Its refusals and their order are those of `live`/`check_operation`: a
    /// stale route first, then a closed Workspace when the job writes, then a
    /// released owner. Existing custody stays readable after logical close.
    pub(crate) fn fence_operation(&self, owner: OperationOwner, write: bool) -> OverlayResult<()> {
        if write && self.state()?.closed {
            return Err(OverlayError::Closed);
        }
        self.check_operation(owner)
    }
    /// Exact last-consumer fence. Cleanup owns operation_record independently afterwards.
    pub fn release_operation(&self, owner: OperationOwner) -> OverlayResult<()> {
        self.atomic_cleanup(|| {
            self.check_operation(owner)?;
            let request = self
                .open
                .borrow()
                .operations
                .get(owner.owner)
                .ok_or(OverlayError::Stale)?
                .1;
            crate::database::durable_attempt::release_operation(self, owner, request)?;
            self.open
                .borrow_mut()
                .operations
                .remove(owner.owner, |held| *held == owner)?;
            crate::database::durable_owners::release_operation(self, owner.owner, request)?;
            self.enqueue(
                crate::maintenance::OPERATION_RECORD,
                integer(owner.owner)?,
                2,
            )?;
            Ok(())
        })
    }
}
