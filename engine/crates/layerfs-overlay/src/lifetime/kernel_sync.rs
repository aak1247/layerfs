//! A durable Workspace barrier for one exact open kernel descriptor.
use crate::{KernelMount, Overlay, OverlayResult};

impl Overlay {
    /// Synchronize this durable overlay only after fencing the exact live
    /// mount, inode and open descriptor. The caller must separately establish
    /// that its Store and its base are durable before acknowledging FUSE.
    pub fn synchronize_kernel(
        &self,
        mount: KernelMount,
        serial: u64,
        handle: u64,
        directory: bool,
    ) -> OverlayResult<()> {
        let held = if directory {
            super::kernel_visit::Held::Directory(handle)
        } else {
            super::kernel_visit::Held::File(handle)
        };
        self.kernel_fence(mount, serial, held, true)?;
        self.synchronize_durable_database()
    }
}
