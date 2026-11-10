//! Exact one-time installation admission and original Store-to-Service transfer.
use super::{Application, ApplicationError, ConnectionFailure};
use crate::install_types::StoreSettings;
use layerfs_bridge::{
    control::{DaemonPhase, InstallPhase, InstallRefusal, InstallReply, RefusalKind},
    duplex::Connection,
    provision::StoreManifest,
};
use layerfs_storage::ReservationBlocks;
use std::{io::Write, path::Path};

impl Application {
    pub(super) fn install(
        &self,
        connection: &mut Connection,
        manifest: StoreManifest,
        slot: usize,
    ) -> Result<bool, ConnectionFailure> {
        if self.setup.fresh_workspace.is_some()
            && manifest.profile != layerfs_bridge::provision::StoreProfile::Durable
        {
            return refuse(
                connection,
                slot,
                RefusalKind::Invalid,
                "fresh durable Workspaces require a Durable Store",
            );
        }
        let admitted = {
            let mut state = self.state.lock().map_err(|_| self.poisoned(slot))?;
            match state.startup.phase {
                DaemonPhase::InstallPending => {
                    state.startup.phase = DaemonPhase::Installing;
                    true
                }
                _ => false,
            }
        };
        if !admitted {
            return refuse(
                connection,
                slot,
                RefusalKind::Exists,
                "one original installer already admitted; no resume/replay",
            );
        }
        let l = self.setup.limits;
        let settings = StoreSettings {
            workspace_capacity: l.namespaces as usize,
            cache_bytes: usize::try_from(l.cache_bytes).map_err(|_| self.poisoned(slot))?,
            reservations: ReservationBlocks::default(),
        };
        let result = crate::install::receive_install_manifest(
            connection,
            Path::new(&self.setup.store),
            settings,
            manifest,
        );
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return Err(ConnectionFailure {
                    slot,
                    received: None,
                    original: None,
                    cause: ApplicationError::InstallPublication(Box::new(result)),
                    fence_error: None,
                    diagnostic_error: None,
                })
            }
        };
        match result {
            Ok(installed) => {
                self.publish_service(&mut state, installed.opened.store.clone());
                state.startup.installed = Some(installed);
                self.changed.notify_all();
                Ok(true)
            }
            Err(failure) => {
                // Only the original final-ack boundary establishes completed
                // Branch/root validation. Open/published alone is insufficient.
                let ready = failure.phase == InstallPhase::Reply
                    && failure.published
                    && failure
                        .manifest
                        .as_ref()
                        .is_some_and(|m| m.daemon_sqlite.is_some())
                    && failure.opened.is_some();
                if ready {
                    let opened = failure.opened.as_ref().expect("validated original Store");
                    self.publish_service(&mut state, opened.store.clone());
                } else {
                    state.startup.phase = DaemonPhase::Retained;
                }
                state.startup.install_failure = Some(failure);
                self.changed.notify_all();
                // The original installer already owns its reply/fence result.
                // Keep it in startup custody, and stop this conversation once.
                Ok(false)
            }
        }
    }
    fn publish_service(
        &self,
        state: &mut super::owner::State,
        store: std::sync::Arc<crate::store::Store>,
    ) {
        match super::filesystem::service(
            store,
            &self.owner,
            self.kernel.as_ref(),
            &self.setup,
            None,
        ) {
            Ok(service) => {
                state.startup.service = Some(service);
                state.startup.phase = DaemonPhase::ControlReady;
            }
            Err(mut failure) => {
                failure.diagnostic_error = writeln!(std::io::stderr().lock(), "{failure}").err();
                state.startup.service_failure = Some(failure);
                state.startup.phase = DaemonPhase::Retained;
            }
        }
    }
    fn poisoned(&self, slot: usize) -> ConnectionFailure {
        ConnectionFailure {
            slot,
            received: None,
            original: None,
            cause: ApplicationError::Poisoned,
            fence_error: None,
            diagnostic_error: None,
        }
    }
}

fn refuse(
    connection: &mut Connection,
    slot: usize,
    kind: RefusalKind,
    detail: &'static str,
) -> Result<bool, ConnectionFailure> {
    let refusal = InstallReply::Refused(InstallRefusal {
        phase: InstallPhase::Request,
        kind,
        claim_acknowledged: false,
        written: 0,
        published: false,
        os_code: None,
        detail: detail.into(),
    });
    let failure = |cause| ConnectionFailure {
        slot,
        received: None,
        original: None,
        cause,
        fence_error: None,
        diagnostic_error: None,
    };
    let bytes = refusal
        .encode()
        .map_err(|cause| failure(ApplicationError::Protocol(cause)))?;
    connection
        .send
        .send(&bytes)
        .map_err(|cause| failure(ApplicationError::Channel(cause)))?;
    connection
        .send
        .close()
        .map_err(|cause| failure(ApplicationError::Channel(cause)))?;
    Ok(false)
}
