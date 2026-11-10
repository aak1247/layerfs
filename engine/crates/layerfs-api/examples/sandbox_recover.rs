//! Explicit one-attempt recovery proof. The external controller fences the old
//! instance and consumers, authenticates the original Store and supplies a new
//! private host export before this program is invoked.
#[cfg(target_os = "linux")]
mod linux {

    use layerfs_api::{BoundWorkspace, SandboxApi, SandboxCreate, Unmounted, WorkspaceApi};
    use layerfs_bridge::{
        control::{Activity, MountPhase, WorkspaceToken},
        daemon_recovery_setup::{RecoveryDaemonSetup, RecoveryWorkspaceSetup},
        daemon_setup::{DaemonSetup, HostExportIdentity},
        duplex,
    };
    use layerfs_history::WorkspaceId;
    use layerfs_sandbox::{
        backend::docker::{Docker, SandboxRequest},
        export_lease::HostExportLease,
        host_export::HostExportRoot,
    };
    use std::{
        error::Error,
        fs::{self, File, OpenOptions},
        io::{self, Write},
        os::unix::fs::OpenOptionsExt,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };

    fn record(dir: &Path, name: &str, contents: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(dir.join(name))?;
        file.write_all(contents)?;
        file.sync_all()?;
        File::open(dir)?.sync_all()
    }

    pub fn run() -> Result<(), Box<dyn Error>> {
        let args: Vec<_> = std::env::args().collect();
        assert_eq!(
            args.len(),
            9,
            "socket image volume daemon proof export store-device store-inode"
        );
        let proof = PathBuf::from(&args[5]);
        assert!(proof.join("original-owner-fenced").is_file());
        let mut setup = DaemonSetup::decode(&fs::read(proof.join("daemon.setup"))?)?;
        let old_namespace: i64 = fs::read_to_string(proof.join("original.receipt"))?
            .lines()
            .next()
            .ok_or("missing old namespace")?
            .parse()?;
        let fresh = setup
            .fresh_workspace
            .take()
            .ok_or("missing original Workspace authority")?;
        let export = Arc::new(HostExportLease::try_acquire(HostExportRoot::open_fresh(
            Path::new(&args[6]),
        )?)?);
        let identity = export.root().identity();
        let controller = duplex::generate_keypair()?;
        let daemon = duplex::generate_keypair()?;
        setup.private_key = daemon.private;
        setup.control_peer = controller.public;
        setup.host_export = Some(HostExportIdentity {
            boot_id: identity.boot_id.clone(),
            device: identity.device,
            inode: identity.inode,
            shared_peer: identity.shared_peer,
        });
        let recovery = RecoveryWorkspaceSetup {
            sandbox: fresh.sandbox,
            catalog_directory: fresh.catalog_directory,
            overlay_directory: fresh.overlay_directory,
            minimum_namespace: old_namespace.checked_add(1).ok_or("namespace exhausted")?,
            boot_id: identity.boot_id.clone(),
            store_device: args[7].parse()?,
            store_inode: args[8].parse()?,
        };
        let request = SandboxCreate {
            deployment: SandboxRequest {
                image: args[2].clone(),
                store_volume: args[3].clone(),
                port: 30421,
                host_export: Some(export),
                labels: vec![],
            },
            setup,
            executable: PathBuf::from(&args[4]),
            controller_private: controller.private,
            daemon_peer: daemon.public,
            startup_wait: Duration::from_secs(30),
            connect_host: None,
        };
        let encoded = RecoveryDaemonSetup {
            daemon: request.setup.clone(),
            workspace: recovery.clone(),
        }
        .encode()?;
        record(&proof, "recovery.setup", &encoded)?;
        record(&proof, "recovery.controller-key", &controller.private)?;
        record(
            &proof,
            "recovery.request",
            format!(
                "image={}\nvolume={}\nexecutable={}\nexport={}\n",
                args[2], args[3], args[4], args[6]
            )
            .as_bytes(),
        )?;
        let api = SandboxApi::new(Docker::new(&args[1], Duration::from_secs(30))?);
        let mut owner = api
            .recover_observed(request, recovery.clone(), |at| {
                record(
                    &proof,
                    &format!("sandbox-{:?}", at.milestone),
                    format!(
                        "runtime={:?}\nstatus={:?}\n",
                        at.runtime.map(|v| v.identity().to_string()),
                        at.status
                    )
                    .as_bytes(),
                )
            })
            .map_err(|e| format!("recover {e:?}"))?;
        assert_eq!(owner.recovery(), Some(&recovery));
        let workspace = WorkspaceId::from_authority([71; 32])?;
        let status = WorkspaceApi::new(&mut owner).locate_recovered(workspace)?;
        assert_eq!(status.activity, Activity::Idle);
        assert_eq!(
            status.kernel.as_ref().ok_or("kernel state absent")?.phase,
            MountPhase::Unattached
        );
        assert!(status.token.namespace > old_namespace);
        let old = WorkspaceToken {
            workspace,
            namespace: old_namespace,
        };
        assert!(WorkspaceApi::new(&mut owner).status(old).is_err());
        let bound = BoundWorkspace {
            token: status.token,
            binding: status.binding,
        };
        let mounted = WorkspaceApi::new(&mut owner)
            .attach_bound_observed(bound, |at| {
                record(
                    &proof,
                    &format!("mount-{:?}", at.milestone),
                    format!("bound={:?}\nready={:?}\n", at.bound, at.ready).as_bytes(),
                )
            })
            .map_err(|e| format!("attach {e:?}"))?;
        let namespace = mounted.bound.token.namespace;
        let minor = mounted.ready.receipt.device_minor;
        {
            let host = owner.observe_host_workspace(&mounted)?;
            host.verify()?;
            assert_eq!(
                fs::read(host.path().join("durable-host-proof"))?,
                b"host durable acknowledgement"
            );
            record(
                &proof,
                "recovery.host-read",
                format!(
                    "namespace={namespace}\nminor={minor}\nmount={}\n",
                    host.mount_id()
                )
                .as_bytes(),
            )?;
        }
        record(
            &proof,
            "recovery.before-close",
            format!("namespace={namespace}\n").as_bytes(),
        )?;
        let outcome = WorkspaceApi::new(&mut owner)
            .unmount_observed(mounted.bound.token, true, |at| {
                record(
                    &proof,
                    &format!("unmount-{:?}", at.milestone),
                    format!(
                        "token={:?}\ndiscard={}\nattempt={:?}\n",
                        at.token, at.discard, at.attempt
                    )
                    .as_bytes(),
                )
            })
            .map_err(|e| format!("unmount {e:?}"))?;
        assert_eq!(outcome, Unmounted::Closed);
        record(
            &proof,
            "recovery.closed",
            format!("namespace={namespace}\nminor={minor}\n").as_bytes(),
        )?;
        api.close_observed(owner, |at| {
            record(
                &proof,
                &format!("sandbox-close-{:?}", at.milestone),
                format!(
                    "runtime={}\nattempt={:?}\n",
                    at.owner.runtime.identity(),
                    at.attempt
                )
                .as_bytes(),
            )
        })
        .map_err(|e| format!("close {e:?}"))?;
        record(
            &proof,
            "recovery.sandbox-closed",
            b"stop and delete acknowledged\n",
        )?;
        println!("SDK_RECOVERY host_read=true old_token_refused=true old_namespace={old_namespace} new_namespace={namespace} closed=true");
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("durable host-export recovery requires Linux");
}
