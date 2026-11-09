//! Match a daemon's acknowledged mount to its original host export lease.
use super::{ManagedSandbox, MountedWorkspace};
use layerfs_sandbox::host_export::HostWorkspaceMount;
use std::io;

impl ManagedSandbox {
    /// Checks the original mounted Workspace against this Sandbox's host
    /// export and returns a pinned host-visible directory. The Workspace must
    /// be the original authenticated acknowledgement from this Sandbox;
    /// callers must retain its lifecycle intent and authorize each consumer.
    /// Drop the guard before mutably borrowing this Sandbox for unmount.
    pub fn observe_host_workspace(
        &self,
        mounted: &MountedWorkspace,
    ) -> io::Result<HostWorkspaceMount<'_>> {
        let disposition = self.runtime.disposition();
        if !disposition.started || disposition.stopped || disposition.deleted {
            return Err(invalid("Sandbox is not in its acknowledged started state"));
        }
        let token = mounted.bound.token;
        if token.namespace <= 0
            || mounted.ready.token != token
            || mounted.ready.directory != format!("/workspaces/{}", token.namespace)
        {
            return Err(invalid("Workspace binding and Ready receipt disagree"));
        }
        let lease = self
            .runtime
            .request()
            .host_export
            .as_deref()
            .ok_or_else(|| invalid("Sandbox has no host export lease"))?;
        lease.observe_workspace(
            token.namespace,
            mounted.ready.receipt.device_major,
            mounted.ready.receipt.device_minor,
        )
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
