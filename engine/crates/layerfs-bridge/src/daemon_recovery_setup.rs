//! Explicit private startup record for an independently fenced recovery.
use crate::{
    control::ControlError,
    daemon_setup::{DaemonSetup, FreshWorkspaceSetup, MAX_DAEMON_SETUP_BYTES},
    provision::StoreProfile,
    wire::{Reader, Writer},
};

const MAGIC: &[u8] = b"LFSR\x01";

/// Original Workspace authority and Store identity observed by a trusted caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryWorkspaceSetup {
    /// Original Sandbox identity from the durable catalog.
    pub sandbox: [u8; 32],
    /// Original existing discovery catalog directory.
    pub catalog_directory: String,
    /// Original existing durable Overlay directory.
    pub overlay_directory: String,
    /// Fresh namespace floor, serialized with all prior and live attempts.
    pub minimum_namespace: i64,
    /// Host kernel boot identity when the physical Store was authenticated.
    pub boot_id: String,
    /// Device of the independently authenticated Store file.
    pub store_device: u64,
    /// Inode of the independently authenticated Store file.
    pub store_inode: u64,
}

/// Explicit recovery startup. This record does not prove that a prior owner,
/// mount or consumer has stopped; the caller must establish that separately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryDaemonSetup {
    /// Ordinary protected startup, selecting the original existing Durable Store.
    pub daemon: DaemonSetup,
    /// Previously persisted Workspace authority and current Store identity.
    pub workspace: RecoveryWorkspaceSetup,
}

impl RecoveryDaemonSetup {
    /// Identifies this versioned wrapper without attempting a fallback decode.
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(MAGIC)
    }

    /// Encodes one bounded protected record; no recovery side effects occur.
    pub fn encode(&self) -> Result<Vec<u8>, ControlError> {
        self.check()?;
        let mut out = Writer::new(MAGIC);
        out.blob(&self.daemon.encode()?)?;
        out.put(&self.workspace.sandbox)?;
        out.blob(self.workspace.catalog_directory.as_bytes())?;
        out.blob(self.workspace.overlay_directory.as_bytes())?;
        out.put(&self.workspace.minimum_namespace.to_be_bytes())?;
        out.blob(self.workspace.boot_id.as_bytes())?;
        out.put(&self.workspace.store_device.to_be_bytes())?;
        out.put(&self.workspace.store_inode.to_be_bytes())?;
        if out.0.len() > MAX_DAEMON_SETUP_BYTES {
            return Err(ControlError("daemon recovery config size"));
        }
        Ok(out.0)
    }

    /// Decodes exactly one protected recovery record without fallback.
    pub fn decode(bytes: &[u8]) -> Result<Self, ControlError> {
        if bytes.len() > MAX_DAEMON_SETUP_BYTES {
            return Err(ControlError("daemon recovery config size"));
        }
        let mut input = Reader::new(bytes, MAGIC)?;
        let daemon = DaemonSetup::decode(input.blob(MAX_DAEMON_SETUP_BYTES)?)?;
        let workspace = RecoveryWorkspaceSetup {
            sandbox: input.array()?,
            catalog_directory: input.text(4096)?,
            overlay_directory: input.text(4096)?,
            minimum_namespace: i64::from_be_bytes(input.array()?),
            boot_id: input.text(36)?,
            store_device: u64::from_be_bytes(input.array()?),
            store_inode: u64::from_be_bytes(input.array()?),
        };
        input.finish()?;
        let setup = Self { daemon, workspace };
        setup.check()?;
        Ok(setup)
    }

    fn check(&self) -> Result<(), ControlError> {
        let workspace = &self.workspace;
        let boot = workspace.boot_id.as_bytes();
        if self.daemon.fresh_workspace.is_some()
            || !self.daemon.existing_store.as_ref().is_some_and(|store| {
                store.profile == StoreProfile::Durable && store.daemon_sqlite.is_some()
            })
            || workspace.minimum_namespace <= 0
            || workspace.minimum_namespace == i64::MAX
            || workspace.store_inode == 0
            || boot.len() != 36
            || !boot.iter().enumerate().all(|(i, byte)| {
                if [8, 13, 18, 23].contains(&i) {
                    *byte == b'-'
                } else {
                    byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)
                }
            })
        {
            return Err(ControlError("daemon recovery authority"));
        }
        // Reuse the ordinary bounded, disjoint directory checks without ever
        // asking the daemon to create or replace either existing directory.
        let mut checked = self.daemon.clone();
        checked.fresh_workspace = Some(FreshWorkspaceSetup {
            sandbox: workspace.sandbox,
            catalog_directory: workspace.catalog_directory.clone(),
            overlay_directory: workspace.overlay_directory.clone(),
            create_directories: false,
        });
        checked.encode()?;
        Ok(())
    }
}
