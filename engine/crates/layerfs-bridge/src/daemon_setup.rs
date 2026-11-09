//! Bounded private deployment configuration, kept out of container env/argv.
use crate::{
    control::ControlError,
    provision::StoreManifest,
    wire::{Reader, Writer},
};
use std::fmt;
const LEGACY_MAGIC: &[u8] = b"LFSD\x05";
const PRECREATED_MAGIC: &[u8] = b"LFSD\x06";
const MAGIC: &[u8] = b"LFSD\x07";
const HOST_EXPORT_MAGIC: &[u8] = b"LFSD\x08";
/// The daemon reads at most this many bytes from its private config file.
pub const MAX_DAEMON_SETUP_BYTES: usize = 8192;
/// Explicit daemon startup resources; these are windows/admission, not file caps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DaemonLimits {
    /// Ordinary control connections; one additional slot is reserved for startup.
    pub connections: u16,
    /// Maximum blocking I/O wait during authentication and the first metadata record.
    /// Cleared before product operations; never applies to commands.
    pub handshake_ms: u32,
    /// Shared immutable cache allowance, separate from whole-process memory.
    pub cache_bytes: u64,
    /// Explicit simultaneously admitted Workspace count.
    pub namespaces: u32,
    /// Explicit Overlay pager suggestion, not a resident ceiling.
    pub pager_kib: u32,
    /// Unconsumed local inode serials below which a create makes one early
    /// reservation attempt. Zero makes none before the range is exhausted.
    pub serial_low_water: u64,
    /// Seconds with no Workspace and no control call after which the daemon
    /// ends its process (#373 idle close). Zero never ends it.
    pub idle_exit_s: u32,
}
/// Private, empty directories for one newly created durable Sandbox.
/// Existing catalogs require independent recovery and cannot use this setup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FreshWorkspaceSetup {
    /// Independently generated identity of this newly created Sandbox.
    pub sandbox: [u8; 32],
    /// Private, initially empty discovery directory.
    pub catalog_directory: String,
    /// Private, initially empty durable Overlay directory.
    pub overlay_directory: String,
    /// The daemon creates a new identity-specific directory on the volume.
    /// False requires the caller to supply pre-created private directories.
    pub create_directories: bool,
}
impl FreshWorkspaceSetup {
    /// Returns the volume only for the exact identity-derived creation layout.
    pub fn creation_volume(&self) -> Result<Option<std::path::PathBuf>, ControlError> {
        use std::{fmt::Write as _, path::Path};
        if !self.create_directories {
            return Ok(None);
        }
        let catalog = Path::new(&self.catalog_directory);
        let overlay = Path::new(&self.overlay_directory);
        let sandbox = catalog
            .parent()
            .ok_or(ControlError("fresh Sandbox directory"))?;
        let shared = sandbox
            .parent()
            .ok_or(ControlError("fresh Sandbox parent"))?;
        let volume = shared
            .parent()
            .ok_or(ControlError("fresh Sandbox volume"))?;
        let mut name = String::with_capacity(64);
        for byte in self.sandbox {
            write!(&mut name, "{byte:02x}").expect("writing to String cannot fail");
        }
        if catalog.file_name().and_then(|v| v.to_str()) != Some("catalog")
            || overlay.file_name().and_then(|v| v.to_str()) != Some("overlays")
            || overlay.parent() != Some(sandbox)
            || sandbox.file_name().and_then(|v| v.to_str()) != Some(name.as_str())
            || shared.file_name().and_then(|v| v.to_str()) != Some("layerfs-durable")
            || !volume.is_absolute()
            || volume == Path::new("/")
        {
            return Err(ControlError("fresh Sandbox creation layout"));
        }
        Ok(Some(volume.to_owned()))
    }
}
/// Identity of an explicitly provisioned host export, compared in the daemon's
/// own mount namespace before it serves a Workspace. Mount IDs differ across
/// namespaces and are deliberately not used as a cross-namespace identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostExportIdentity {
    /// Host kernel boot identity as a lowercase UUID.
    pub boot_id: String,
    /// Device of the exported directory as observed on the host.
    pub device: u64,
    /// Inode of the exported directory as observed on the host.
    pub inode: u64,
    /// Shared mount propagation group expected in both namespaces.
    pub shared_peer: u64,
}
/// Private configuration for one real daemon; caller supplies all policy/resource values.
#[derive(Clone, Eq, PartialEq)]
pub struct DaemonSetup {
    /// TCP listen address; selected explicitly by deployment.
    pub listen: String,
    /// Daemon Noise static secret, stored only in a protected file.
    pub private_key: [u8; 32],
    /// Authorized controller's static public key.
    pub control_peer: [u8; 32],
    /// Exact shared Store file on the named in-VM volume.
    pub store: String,
    /// Separate per-daemon Overlay file.
    pub overlay: String,
    /// Workspace mount directory root; no per-command launcher exists.
    pub mounts: String,
    /// Ordinary command uid, distinct from daemon uid.
    pub command_uid: u32,
    /// Ordinary command gid, with no privileged supplementary groups injected.
    pub command_gid: u32,
    /// Explicit startup resource settings.
    pub limits: DaemonLimits,
    /// None selects the original one-time installer. Some selects explicit existing
    /// Store open using retained authority; never selected after an install error.
    pub existing_store: Option<StoreManifest>,
    /// Creates a new durable Workspace catalog. None keeps disposable overlays.
    pub fresh_workspace: Option<FreshWorkspaceSetup>,
    /// Requires verification of an explicitly bound host export root. None
    /// preserves the existing container-local mount behavior and wire format.
    pub host_export: Option<HostExportIdentity>,
}
impl DaemonSetup {
    /// Encodes one bounded private config. No operation/flow byte limit is implied.
    pub fn encode(&self) -> Result<Vec<u8>, ControlError> {
        self.check()?;
        let mut out = Writer::new(if self.host_export.is_some() {
            HOST_EXPORT_MAGIC
        } else {
            MAGIC
        });
        for v in [&self.listen, &self.store, &self.overlay, &self.mounts] {
            out.blob(v.as_bytes())?;
        }
        out.put(&self.private_key)?;
        out.put(&self.control_peer)?;
        out.put(&self.command_uid.to_be_bytes())?;
        out.put(&self.command_gid.to_be_bytes())?;
        let l = self.limits;
        out.put(&l.connections.to_be_bytes())?;
        out.put(&l.handshake_ms.to_be_bytes())?;
        out.put(&l.cache_bytes.to_be_bytes())?;
        for v in [l.namespaces, l.pager_kib] {
            out.put(&v.to_be_bytes())?;
        }
        out.put(&l.serial_low_water.to_be_bytes())?;
        out.put(&l.idle_exit_s.to_be_bytes())?;
        out.byte(u8::from(self.existing_store.is_some()))?;
        if let Some(value) = &self.existing_store {
            out.blob(&value.encode()?)?;
        }
        out.byte(u8::from(self.fresh_workspace.is_some()))?;
        if let Some(value) = &self.fresh_workspace {
            out.put(&value.sandbox)?;
            out.blob(value.catalog_directory.as_bytes())?;
            out.blob(value.overlay_directory.as_bytes())?;
            out.byte(u8::from(value.create_directories))?;
        }
        if let Some(value) = &self.host_export {
            out.blob(value.boot_id.as_bytes())?;
            out.put(&value.device.to_be_bytes())?;
            out.put(&value.inode.to_be_bytes())?;
            out.put(&value.shared_peer.to_be_bytes())?;
        }
        if out.0.len() > MAX_DAEMON_SETUP_BYTES {
            return Err(ControlError("daemon config size"));
        }
        Ok(out.0)
    }
    /// Decodes exactly one config without fallback or profile selection.
    pub fn decode(bytes: &[u8]) -> Result<Self, ControlError> {
        if bytes.len() > MAX_DAEMON_SETUP_BYTES {
            return Err(ControlError("daemon config size"));
        }
        let legacy = bytes.starts_with(LEGACY_MAGIC);
        let precreated = bytes.starts_with(PRECREATED_MAGIC);
        let host_export = bytes.starts_with(HOST_EXPORT_MAGIC);
        let mut input = Reader::new(
            bytes,
            if legacy {
                LEGACY_MAGIC
            } else if precreated {
                PRECREATED_MAGIC
            } else if host_export {
                HOST_EXPORT_MAGIC
            } else {
                MAGIC
            },
        )?;
        let listen = input.text(128)?;
        let store = input.text(4096)?;
        let overlay = input.text(4096)?;
        let mounts = input.text(4096)?;
        let private_key = input.array()?;
        let control_peer = input.array()?;
        let command_uid = u32::from_be_bytes(input.array()?);
        let command_gid = u32::from_be_bytes(input.array()?);
        let limits = DaemonLimits {
            connections: u16::from_be_bytes(input.array()?),
            handshake_ms: u32::from_be_bytes(input.array()?),
            cache_bytes: u64::from_be_bytes(input.array()?),
            namespaces: u32::from_be_bytes(input.array()?),
            pager_kib: u32::from_be_bytes(input.array()?),
            serial_low_water: u64::from_be_bytes(input.array()?),
            idle_exit_s: u32::from_be_bytes(input.array()?),
        };
        let existing_store = match input.byte()? {
            0 => None,
            1 => Some(StoreManifest::decode(input.blob(crate::wire::LIMIT)?)?),
            _ => return Err(ControlError("daemon config boolean")),
        };
        let fresh_workspace = if legacy {
            None
        } else {
            match input.byte()? {
                0 => None,
                1 => {
                    let sandbox = input.array()?;
                    let catalog_directory = input.text(4096)?;
                    let overlay_directory = input.text(4096)?;
                    let create_directories = if precreated {
                        false
                    } else {
                        match input.byte()? {
                            0 => false,
                            1 => true,
                            _ => return Err(ControlError("daemon directory creation boolean")),
                        }
                    };
                    Some(FreshWorkspaceSetup {
                        sandbox,
                        catalog_directory,
                        overlay_directory,
                        create_directories,
                    })
                }
                _ => return Err(ControlError("daemon fresh Workspace boolean")),
            }
        };
        let host_export = if host_export {
            Some(HostExportIdentity {
                boot_id: input.text(36)?,
                device: u64::from_be_bytes(input.array()?),
                inode: u64::from_be_bytes(input.array()?),
                shared_peer: u64::from_be_bytes(input.array()?),
            })
        } else {
            None
        };
        input.finish()?;
        let value = Self {
            listen,
            private_key,
            control_peer,
            store,
            overlay,
            mounts,
            command_uid,
            command_gid,
            limits,
            existing_store,
            fresh_workspace,
            host_export,
        };
        value.check()?;
        Ok(value)
    }
    fn check(&self) -> Result<(), ControlError> {
        if self.listen.parse::<std::net::SocketAddr>().is_err()
            || self.private_key == [0; 32]
            || self.control_peer == [0; 32]
            || self.command_uid == 0
            || self.command_gid == 0
        {
            return Err(ControlError("daemon address/identity"));
        }
        for v in [&self.store, &self.overlay, &self.mounts] {
            let p = std::path::Path::new(v);
            if !p.is_absolute()
                || v.len() > 4096
                || v.as_bytes().contains(&0)
                || p.components().any(|c| {
                    matches!(
                        c,
                        std::path::Component::ParentDir | std::path::Component::CurDir
                    )
                })
            {
                return Err(ControlError("daemon path"));
            }
        }
        let l = self.limits;
        if l.handshake_ms == 0 || l.connections == 0 || l.namespaces == 0 || l.pager_kib == 0 {
            return Err(ControlError("daemon resources"));
        }
        if let Some(export) = &self.host_export {
            let boot = export.boot_id.as_bytes();
            if boot.len() != 36
                || !boot.iter().enumerate().all(|(i, byte)| {
                    if [8, 13, 18, 23].contains(&i) {
                        *byte == b'-'
                    } else {
                        byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)
                    }
                })
                || export.inode == 0
                || export.shared_peer == 0
            {
                return Err(ControlError("host export identity"));
            }
        }
        if let Some(m) = &self.existing_store {
            m.check().map_err(ControlError)?;
            if m.locator != self.store {
                return Err(ControlError("existing Store locator differs"));
            }
        }
        if let Some(fresh) = &self.fresh_workspace {
            if fresh.sandbox == [0; 32]
                || self
                    .existing_store
                    .as_ref()
                    .is_some_and(|m| m.profile != crate::provision::StoreProfile::Durable)
            {
                return Err(ControlError("fresh Workspace authority or Store profile"));
            }
            let catalog = std::path::Path::new(&fresh.catalog_directory);
            let overlays = std::path::Path::new(&fresh.overlay_directory);
            for (text, path) in [
                (&fresh.catalog_directory, catalog),
                (&fresh.overlay_directory, overlays),
            ] {
                if !path.is_absolute()
                    || text.len() > 4096
                    || text.as_bytes().contains(&0)
                    || path.components().any(|c| {
                        matches!(
                            c,
                            std::path::Component::ParentDir | std::path::Component::CurDir
                        )
                    })
                {
                    return Err(ControlError("fresh Workspace path"));
                }
                for other in [&self.store, &self.overlay, &self.mounts] {
                    let other = std::path::Path::new(other);
                    if path.starts_with(other) || other.starts_with(path) {
                        return Err(ControlError("fresh Workspace backing overlap"));
                    }
                }
            }
            if catalog.starts_with(overlays) || overlays.starts_with(catalog) {
                return Err(ControlError("fresh Workspace directories overlap"));
            }
            if let Some(volume) = fresh.creation_volume()? {
                let store = std::path::Path::new(&self.store);
                if !store.starts_with(&volume) || store == volume {
                    return Err(ControlError("fresh Workspace volume placement"));
                }
            }
        }
        Ok(())
    }
}
impl fmt::Debug for DaemonSetup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DaemonSetup")
            .field("listen", &self.listen)
            .field("private_key", &"[redacted]")
            .field("control_peer", &self.control_peer)
            .field("store", &self.store)
            .field("overlay", &self.overlay)
            .field("mounts", &self.mounts)
            .field("command_uid", &self.command_uid)
            .field("command_gid", &self.command_gid)
            .field("limits", &self.limits)
            .field("existing_store", &self.existing_store)
            .field("fresh_workspace", &self.fresh_workspace)
            .field("host_export", &self.host_export)
            .finish()
    }
}
