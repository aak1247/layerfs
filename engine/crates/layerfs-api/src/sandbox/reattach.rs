//! Authenticate an independently fenced controller to an exact live daemon.
use super::create::{
    hello, ManagedSandbox, SandboxApi, SandboxCause, SandboxCreate, SandboxFailure, SandboxPhase,
};
use crate::internal::Control;
use layerfs_bridge::{
    control::{ControlError, DaemonPhase, HelloRequest},
    duplex::{initiate_observed, ChannelWork},
};
use layerfs_sandbox::{backend::docker::Sandbox, host_export::ExportIdentity, ContainerId};
use std::net::TcpStream;

impl SandboxApi {
    /// Reattaches to the saved running container and original daemon instance.
    /// The caller must first persist its independent owner-fencing and journal
    /// freshness evidence. Matching local files or acquiring their locks alone
    /// are not those proofs. This sends only one authenticated Hello; it does
    /// not create, start, install, bind, attach, commit or replay a command.
    pub fn reattach_exported(
        &self,
        request: SandboxCreate,
        container: ContainerId,
        export: &ExportIdentity,
        instance: [u8; 32],
    ) -> Result<ManagedSandbox, Box<SandboxFailure>> {
        let mut phase = SandboxPhase::Validate;
        let mut sandbox = None;
        let mut control = None;
        let mut handshake = ChannelWork::default();
        let mut observed = None;
        let result = (|| {
            request.setup.encode().map_err(SandboxCause::Setup)?;
            let matches = request.setup.host_export.as_ref().is_some_and(|saved| {
                saved.boot_id == export.boot_id
                    && saved.device == export.device
                    && saved.inode == export.inode
                    && saved.shared_peer == export.shared_peer
            });
            if !matches || request.startup_wait.is_zero() {
                return Err(SandboxCause::Setup(ControlError(
                    "saved exported daemon identity",
                )));
            }
            phase = SandboxPhase::Endpoint;
            sandbox = Some(
                Sandbox::reattach_exported(
                    self.docker.clone(),
                    container,
                    request.deployment.clone(),
                    export,
                )
                .map_err(SandboxCause::Runtime)?,
            );
            let endpoint = sandbox
                .as_ref()
                .expect("observed")
                .endpoint()
                .map_err(SandboxCause::Runtime)?;
            phase = SandboxPhase::Connect;
            let address = super::connect_address(endpoint, request.connect_host.as_deref())
                .map_err(SandboxCause::Io)?;
            let stream = TcpStream::connect_timeout(&address, request.startup_wait)
                .map_err(SandboxCause::Io)?;
            stream
                .set_read_timeout(Some(request.startup_wait))
                .map_err(SandboxCause::Io)?;
            stream
                .set_write_timeout(Some(request.startup_wait))
                .map_err(SandboxCause::Io)?;
            phase = SandboxPhase::Authenticate;
            let attempt =
                initiate_observed(stream, &request.controller_private, request.daemon_peer);
            handshake = attempt.work;
            control = Some(Control::new(attempt.result.map_err(SandboxCause::Channel)?));
            phase = SandboxPhase::Policy;
            control
                .as_ref()
                .expect("authenticated")
                .clear_io_waits()
                .map_err(SandboxCause::Channel)?;
            phase = SandboxPhase::Hello;
            let status = hello(
                control.as_mut().expect("authenticated"),
                HelloRequest {
                    expected_instance: Some(instance),
                    wait_for_store: true,
                },
            )
            .map_err(SandboxCause::Control)?;
            observed = Some(status.clone());
            if status.instance != instance || status.phase != DaemonPhase::ControlReady {
                return Err(SandboxCause::Setup(ControlError(
                    "saved live daemon incarnation",
                )));
            }
            Ok(status)
        })();
        match result {
            Ok(startup) => Ok(ManagedSandbox {
                command_identity: layerfs_sandbox::CommandIdentity {
                    uid: request.setup.command_uid,
                    gid: request.setup.command_gid,
                },
                request,
                recovery: None,
                installed: None,
                runtime: sandbox.expect("observed"),
                control: control.expect("authenticated"),
                startup,
                handshake,
            }),
            Err(cause) => Err(Box::new(SandboxFailure {
                request,
                recovery: None,
                phase,
                sandbox,
                control,
                handshake,
                observed,
                installed: None,
                cause,
            })),
        }
    }
}
