//! Bounded original Engine/control peers qualify SDK close custody and ordinary exec routing.
#![cfg(unix)]
#[allow(dead_code)]
#[path = "../../layerfs-sandbox/tests/support/engine.rs"]
mod engine;
use engine::{fixed, frame, Peer};
use layerfs_api::sandbox::{SandboxCloseAttempt, SandboxCloseMilestone, SandboxMilestone};
use layerfs_api::{
    ManagedSandbox, SandboxApi, SandboxCause, SandboxCreate, SandboxPhase, WorkspaceApi,
};
use layerfs_bridge::{
    control::{Answer, Call, DaemonPhase, DaemonStatus, Reply, Request},
    daemon_recovery_setup::{RecoveryDaemonSetup, RecoveryWorkspaceSetup},
    daemon_setup::{DaemonLimits, DaemonSetup, FreshWorkspaceSetup},
    duplex,
};
use std::{
    fs,
    net::TcpListener,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
const CID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const IMAGE: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
static NEXT: AtomicUsize = AtomicUsize::new(0);
const SECCOMP: &str = r#"seccomp={\"defaultAction\":\"SCMP_ACT_ALLOW\",\"syscalls\":[{\"names\":[\"unshare\",\"clone\"],\"action\":\"SCMP_ACT_ERRNO\",\"errnoRet\":1,\"args\":[{\"index\":0,\"value\":268435456,\"valueTwo\":268435456,\"op\":\"SCMP_CMP_MASKED_EQ\"}]},{\"names\":[\"clone3\"],\"action\":\"SCMP_ACT_ERRNO\",\"errnoRet\":38}]}"#;
fn endpoint(port: u16) -> Vec<u8> {
    fixed(200,format!(r#"{{"Id":"{CID}","Image":"{IMAGE}","State":{{"Running":true,"Paused":false,"Restarting":false,"Dead":false}},"Config":{{"User":"0:0","Tty":false,"Entrypoint":["/usr/local/bin/layerfs-daemon"],"Cmd":["--config","/layerfs-local/config/daemon.setup"]}},"HostConfig":{{"Init":true,"Privileged":false,"CapAdd":["CAP_SYS_ADMIN"],"Devices":[{{"PathOnHost":"/dev/fuse","PathInContainer":"/dev/fuse","CgroupPermissions":"rwm"}}],"SecurityOpt":["no-new-privileges=true","apparmor=unconfined","{SECCOMP}"],"RestartPolicy":{{"Name":"no"}},"LogConfig":{{"Type":"json-file"}}}},"Mounts":[{{"Type":"volume","Name":"lfs-test-borrowed","Destination":"/layerfs-store","RW":true,"Driver":"local"}}],"NetworkSettings":{{"Ports":{{"30421/tcp":[{{"HostIp":"127.0.0.1","HostPort":"{port}"}}]}}}}}}"#).as_bytes())
}
fn setup(private: [u8; 32], peer: [u8; 32]) -> DaemonSetup {
    DaemonSetup {
        listen: "0.0.0.0:30421".into(),
        private_key: private,
        control_peer: peer,
        store: "/layerfs-store/global/store.sqlite".into(),
        overlay: "/layerfs-local/overlay/overlay.sqlite".into(),
        mounts: "/workspaces".into(),
        command_uid: 501,
        command_gid: 20,
        limits: DaemonLimits {
            connections: 4,
            handshake_ms: 2000,
            cache_bytes: 65536,
            namespaces: 8,
            pager_kib: 1024,
            serial_low_water: 256,
            idle_exit_s: 0,
        },
        existing_store: None,
        fresh_workspace: None,
        host_export: None,
    }
}

struct Fixture {
    api: SandboxApi,
    owner: Option<ManagedSandbox>,
    peer: Option<Peer>,
    daemon: Option<JoinHandle<()>>,
    binary: PathBuf,
}
impl Fixture {
    fn new(tail: Vec<Vec<u8>>) -> Self {
        Self::new_for(None, tail)
    }
    fn new_for(project: Option<&layerfs_api::SealedProject>, tail: Vec<Vec<u8>>) -> Self {
        let existing = project.is_some();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let daemon = thread::spawn(move || {
            let until = Instant::now() + Duration::from_secs(3);
            let socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < until =>
                    {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("bounded original Hello accept: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut channel =
                duplex::accept(socket, &[32; 32], duplex::public_key(&[31; 32]).unwrap()).unwrap();
            let call = Call::decode(channel.receive.receive().unwrap()).unwrap();
            assert!(
                matches!(call.request, Request::Hello(request) if request.wait_for_store == existing && request.expected_instance.is_none())
            );
            channel
                .send
                .send(
                    &Answer {
                        id: call.id,
                        reply: Reply::Hello(DaemonStatus {
                            instance: [9; 32],
                            phase: if existing {
                                DaemonPhase::ControlReady
                            } else {
                                DaemonPhase::InstallPending
                            },
                            overlay_sqlite: Some("3.51.0".into()),
                            store_sqlite: existing.then(|| "3.51.0".into()),
                        }),
                    }
                    .encode()
                    .unwrap(),
                )
                .unwrap();
        });
        let marker = frame(1, b"LAYERFS_DAEMON_LISTEN 0.0.0.0:30421\n");
        let mut logs = format!("HTTP/1.1 200 OK\r\nContent-Type: application/vnd.docker.multiplexed-stream\r\nContent-Length: {}\r\n\r\n",marker.len()).into_bytes();
        logs.extend(marker);
        let mut responses = vec![
            fixed(200,b"{\"Name\":\"lfs-test-borrowed\",\"Driver\":\"local\",\"Scope\":\"local\",\"Options\":null}"),
            fixed(201,format!("{{\"Id\":\"{CID}\",\"Warnings\":[]}}").as_bytes()),
            fixed(200,b""), fixed(204,b""), logs, endpoint(port),
        ];
        responses.extend(tail);
        let (docker, peer) = Peer::new(responses, 4096);
        let api = SandboxApi::new(docker);
        let binary = std::env::temp_dir().join(format!(
            "sdk-operations-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&binary, b"original executable input").unwrap();
        let mut deployment_setup = setup([32; 32], duplex::public_key(&[31; 32]).unwrap());
        if let Some(project) = project {
            let mut manifest = project.manifest.clone();
            manifest.daemon_sqlite = Some("3.51.0".into());
            deployment_setup.existing_store = Some(manifest);
        }
        let request = SandboxCreate {
            deployment: layerfs_sandbox::backend::docker::SandboxRequest {
                image: IMAGE.into(),
                store_volume: "lfs-test-borrowed".into(),
                port: 30421,
                #[cfg(target_os = "linux")]
                host_export: None,
                labels: vec![],
            },
            setup: deployment_setup,
            executable: binary.clone(),
            controller_private: [31; 32],
            daemon_peer: duplex::public_key(&[32; 32]).unwrap(),
            startup_wait: Duration::from_secs(2),
            connect_host: None,
        };
        // No Store is constructed or opened. This is the original InstallPending
        // startup used by component proofs, not a fabricated SDK owner.
        let mut fixture = Self {
            api,
            owner: None,
            peer: Some(peer),
            daemon: Some(daemon),
            binary,
        };
        fixture.owner = Some(match project {
            Some(project) => fixture.api.create(request, project).unwrap(),
            None => layerfs_api::internal::start(&fixture.api, request).unwrap(),
        });
        fixture
    }
    fn calls(mut self) -> Vec<Vec<u8>> {
        let calls = self.peer.take().unwrap().finish();
        self.daemon.take().unwrap().join().unwrap();
        calls
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(daemon) = self.daemon.take() {
            let _ = daemon.join();
        }
        let _ = fs::remove_file(&self.binary);
    }
}
#[test]
fn close_stops_then_deletes_the_original_owner_without_deleting_its_borrowed_volume() {
    let mut fixture = Fixture::new(vec![fixed(204, b""), fixed(204, b"")]);
    fixture.api.close(fixture.owner.take().unwrap()).unwrap();
    let calls = fixture.calls();
    assert_eq!(calls.len(), 8);
    assert!(calls[6].starts_with(format!("POST /v1.54/containers/{CID}/stop?t=1 ").as_bytes()));
    assert!(calls[7]
        .starts_with(format!("DELETE /v1.54/containers/{CID}?force=false&v=false ").as_bytes()));
    assert!(!calls
        .iter()
        .any(|call| call.starts_with(b"DELETE /v1.54/volumes/")));
}

#[test]
fn observed_close_records_original_results_in_order() {
    let mut fixture = Fixture::new(vec![fixed(204, b""), fixed(204, b"")]);
    let mut seen = Vec::new();
    fixture
        .api
        .close_observed(fixture.owner.take().unwrap(), |at| {
            assert_eq!(at.owner.runtime.identity().to_string(), CID);
            match at.milestone {
                SandboxCloseMilestone::BeforeStop | SandboxCloseMilestone::BeforeDelete => {
                    assert!(at.attempt.is_none())
                }
                SandboxCloseMilestone::StopFinished | SandboxCloseMilestone::DeleteFinished => {
                    assert!(matches!(
                        at.attempt,
                        Some(SandboxCloseAttempt::Acknowledged)
                    ))
                }
            }
            seen.push(at.milestone);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        seen,
        [
            SandboxCloseMilestone::BeforeStop,
            SandboxCloseMilestone::StopFinished,
            SandboxCloseMilestone::BeforeDelete,
            SandboxCloseMilestone::DeleteFinished
        ]
    );
    let calls = fixture.calls();
    assert_eq!(calls.len(), 8);
    assert!(calls[6].starts_with(format!("POST /v1.54/containers/{CID}/stop?t=1 ").as_bytes()));
    assert!(calls[7]
        .starts_with(format!("DELETE /v1.54/containers/{CID}?force=false&v=false ").as_bytes()));
}

#[test]
fn observed_close_failed_intents_send_no_next_engine_request() {
    for boundary in [
        SandboxCloseMilestone::BeforeStop,
        SandboxCloseMilestone::BeforeDelete,
    ] {
        let responses = if boundary == SandboxCloseMilestone::BeforeStop {
            vec![]
        } else {
            vec![fixed(204, b"")]
        };
        let mut fixture = Fixture::new(responses);
        let failure = fixture
            .api
            .close_observed(fixture.owner.take().unwrap(), |at| {
                if at.milestone == boundary {
                    Err(std::io::Error::other("intent sync failed"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_eq!(failure.milestone, boundary);
        assert_eq!(
            failure.observer_error.as_ref().unwrap().to_string(),
            "intent sync failed"
        );
        assert_eq!(failure.owner.runtime.identity().to_string(), CID);
        assert_eq!(failure.request().controller_private, [31; 32]);
        assert!(failure.delete.is_none());
        assert_eq!(
            failure.owner.runtime.disposition().stopped,
            boundary == SandboxCloseMilestone::BeforeDelete
        );
        assert_eq!(
            fixture.calls().len(),
            if boundary == SandboxCloseMilestone::BeforeStop {
                6
            } else {
                7
            }
        );
    }
}

#[test]
fn observed_close_failed_receipt_recording_retains_each_acknowledged_result() {
    for boundary in [
        SandboxCloseMilestone::StopFinished,
        SandboxCloseMilestone::DeleteFinished,
    ] {
        let responses = if boundary == SandboxCloseMilestone::StopFinished {
            vec![fixed(204, b"")]
        } else {
            vec![fixed(204, b""), fixed(204, b"")]
        };
        let mut fixture = Fixture::new(responses);
        let failure = fixture
            .api
            .close_observed(fixture.owner.take().unwrap(), |at| {
                if at.milestone == boundary {
                    assert!(matches!(
                        at.attempt,
                        Some(SandboxCloseAttempt::Acknowledged)
                    ));
                    Err(std::io::Error::other("receipt sync failed"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_eq!(failure.milestone, boundary);
        assert_eq!(
            failure.observer_error.as_ref().unwrap().to_string(),
            "receipt sync failed"
        );
        assert!(matches!(
            failure.stop,
            Some(SandboxCloseAttempt::Acknowledged)
        ));
        assert_eq!(
            failure.owner.runtime.disposition().deleted,
            boundary == SandboxCloseMilestone::DeleteFinished
        );
        if boundary == SandboxCloseMilestone::DeleteFinished {
            assert!(matches!(
                failure.delete,
                Some(SandboxCloseAttempt::Acknowledged)
            ));
        } else {
            assert!(failure.delete.is_none());
        }
        assert_eq!(
            fixture.calls().len(),
            if boundary == SandboxCloseMilestone::StopFinished {
                7
            } else {
                8
            }
        );
    }
}

#[test]
fn observed_close_retains_original_engine_failure_without_replay() {
    for boundary in [
        SandboxCloseMilestone::StopFinished,
        SandboxCloseMilestone::DeleteFinished,
    ] {
        let responses = if boundary == SandboxCloseMilestone::StopFinished {
            vec![fixed(409, b"original stop refusal")]
        } else {
            vec![fixed(204, b""), fixed(409, b"original delete refusal")]
        };
        let mut fixture = Fixture::new(responses);
        let failure = fixture
            .api
            .close_observed(fixture.owner.take().unwrap(), |at| {
                if at.milestone == boundary {
                    assert!(matches!(at.attempt, Some(SandboxCloseAttempt::Failed(_))));
                }
                Ok(())
            })
            .unwrap_err();
        assert_eq!(failure.milestone, boundary);
        assert!(failure.observer_error.is_none());
        let attempt = if boundary == SandboxCloseMilestone::StopFinished {
            &failure.stop
        } else {
            &failure.delete
        };
        let Some(SandboxCloseAttempt::Failed(original)) = attempt else {
            panic!("original Engine failure")
        };
        assert!(original.attempted);
        assert_eq!(
            original.requested_container,
            Some(failure.owner.runtime.identity())
        );
        assert_eq!(
            fixture.calls().len(),
            if boundary == SandboxCloseMilestone::StopFinished {
                7
            } else {
                8
            }
        );
    }
}

#[test]
fn observed_close_preserves_engine_and_journal_failures_together() {
    for boundary in [
        SandboxCloseMilestone::StopFinished,
        SandboxCloseMilestone::DeleteFinished,
    ] {
        let responses = if boundary == SandboxCloseMilestone::StopFinished {
            vec![fixed(409, b"original stop refusal")]
        } else {
            vec![fixed(204, b""), fixed(409, b"original delete refusal")]
        };
        let mut fixture = Fixture::new(responses);
        let failure = fixture
            .api
            .close_observed(fixture.owner.take().unwrap(), |at| {
                if at.milestone == boundary {
                    assert!(matches!(at.attempt, Some(SandboxCloseAttempt::Failed(_))));
                    Err(std::io::Error::other("original result could not be synced"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_eq!(failure.milestone, boundary);
        assert_eq!(
            failure.observer_error.as_ref().unwrap().to_string(),
            "original result could not be synced"
        );
        let original = if boundary == SandboxCloseMilestone::StopFinished {
            &failure.stop
        } else {
            &failure.delete
        };
        let Some(SandboxCloseAttempt::Failed(original)) = original else {
            panic!("the original Engine error must be retained")
        };
        assert!(original.attempted);
        assert_eq!(
            original.requested_container,
            Some(failure.owner.runtime.identity())
        );
        assert_eq!(
            fixture.calls().len(),
            if boundary == SandboxCloseMilestone::StopFinished {
                7
            } else {
                8
            }
        );
    }
}
#[test]
fn failed_stop_preserves_original_owner_channel_inputs_and_attempt_without_delete() {
    let mut fixture = Fixture::new(vec![fixed(409, b"original stop refusal")]);
    let mut owner = fixture.owner.take().unwrap();
    let work = layerfs_api::internal::control(&mut owner).channel_work();
    let handshake = owner.handshake;
    let failure = fixture.api.close(owner).unwrap_err();
    assert_eq!(failure.phase, SandboxPhase::Stop);
    let runtime = failure.sandbox.as_ref().unwrap();
    assert_eq!(runtime.identity().to_string(), CID);
    assert!(!runtime.disposition().stopped);
    assert!(!runtime.disposition().deleted);
    let SandboxCause::Runtime(cause) = &failure.cause else {
        panic!("original runtime failure")
    };
    assert_eq!(cause.requested_container, Some(runtime.identity()));
    assert!(cause.attempted);
    assert_eq!(failure.control.as_ref().unwrap().channel_work(), work);
    assert_eq!(failure.handshake, handshake);
    assert_eq!(failure.request.controller_private, [31; 32]);
    assert!(failure.installed.is_none());
    assert_eq!(fixture.calls().len(), 7);
}
#[test]
fn failed_delete_retains_acknowledged_stop_and_exact_delete_failure() {
    let mut fixture = Fixture::new(vec![
        fixed(204, b""),
        fixed(409, b"original delete refusal"),
    ]);
    let failure = fixture
        .api
        .close(fixture.owner.take().unwrap())
        .unwrap_err();
    assert_eq!(failure.phase, SandboxPhase::Delete);
    let runtime = failure.sandbox.as_ref().unwrap();
    assert_eq!(runtime.identity().to_string(), CID);
    assert!(runtime.disposition().stopped);
    assert!(!runtime.disposition().deleted);
    let SandboxCause::Runtime(cause) = &failure.cause else {
        panic!("original runtime failure")
    };
    assert_eq!(cause.requested_container, Some(runtime.identity()));
    assert!(cause.attempted);
    assert!(failure.control.is_some());
    assert_eq!(fixture.calls().len(), 8);
}

fn mounted() -> layerfs_api::MountedWorkspace {
    use layerfs_bridge::control::{MountReceipt, ReadyMount, WorkspaceToken};
    use layerfs_content::ObjectId;
    use layerfs_history::{
        BranchId, BranchRecord, BranchSnapshot, HistoryName, LayerId, LayerStackId, WorkspaceId,
    };
    let token = WorkspaceToken {
        workspace: WorkspaceId::from_authority([4; 32]).unwrap(),
        namespace: 7,
    };
    let root = ObjectId::for_bytes(b"SDK exec mount fixture");
    layerfs_api::MountedWorkspace {
        bound: layerfs_api::BoundWorkspace {
            token,
            binding: BranchSnapshot {
                branch: BranchRecord {
                    forked_from: None,
                    id: BranchId::from_authority([1; 16]),
                    stack: LayerStackId::from_authority([2; 16]),
                    name: HistoryName::new("main").unwrap(),
                    base_layer: LayerId::from_bytes([0x32; 33]).unwrap(),
                    head_commit: None,
                },
                head_root: None,
                base_root: root,
                effective_root: root,
                scope: ObjectId::for_bytes(b"SDK scope"),
                profile: layerfs_content::filesystem::profile_id(),
            },
        },
        ready: ReadyMount {
            token,
            directory: "/workspaces/selected-mount".into(),
            receipt: MountReceipt {
                mount: 0x0102,
                root: 7,
                mount_id: 911,
                device_major: 0,
                device_minor: 64,
                abi_major: 7,
                abi_minor: 41,
                offered: u64::MAX - 1,
                selected: (1 << 22) | 1,
                max_write: 131072,
                max_readahead: 65536,
                max_background: 12,
                congestion_threshold: 9,
                page_size: 4096,
                loops: 2,
                abort_bound: true,
            },
        },
    }
}
#[test]
#[cfg(target_os = "linux")]
fn host_workspace_refuses_mismatched_receipts_and_absent_export_without_effects() {
    let mut fixture = Fixture::new(vec![fixed(204, b""), fixed(204, b"")]);
    let owner = fixture.owner.as_mut().unwrap();
    let before = layerfs_api::internal::control(owner).channel_work();
    let mut workspace = mounted();
    assert!(owner
        .observe_host_workspace(&workspace)
        .unwrap_err()
        .to_string()
        .contains("binding and Ready receipt disagree"));
    workspace.ready.directory = "/workspaces/7".into();
    workspace.ready.token.namespace = 8;
    assert!(owner
        .observe_host_workspace(&workspace)
        .unwrap_err()
        .to_string()
        .contains("binding and Ready receipt disagree"));
    workspace.ready.token.namespace = 7;
    assert!(owner
        .observe_host_workspace(&workspace)
        .unwrap_err()
        .to_string()
        .contains("no host export lease"));
    assert_eq!(layerfs_api::internal::control(owner).channel_work(), before);
    fixture.api.close(fixture.owner.take().unwrap()).unwrap();
    assert_eq!(fixture.calls().len(), 8);
}
#[test]
fn workspace_exec_returns_original_runtime_owner_at_the_mount_with_no_daemon_registration() {
    const EID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let mut fixture = Fixture::new(vec![
        fixed(201, format!("{{\"Id\":\"{EID}\"}}").as_bytes()),
        fixed(204, b""),
        fixed(204, b""),
    ]);
    let before = layerfs_api::internal::control(fixture.owner.as_mut().unwrap()).channel_work();
    let created = WorkspaceApi::exec(
        fixture.owner.as_ref().unwrap(),
        &mounted(),
        vec!["/bin/pwd".into()],
        vec!["SDK_TEST=1".into()],
        false,
    )
    .unwrap();
    assert_eq!(created.identity().0.to_string(), CID);
    assert_eq!(created.identity().1.to_string(), EID);
    assert_eq!(created.request().directory, "/workspaces/selected-mount");
    assert_eq!(
        created.request().identity,
        layerfs_sandbox::CommandIdentity { uid: 501, gid: 20 }
    );
    assert_eq!(
        layerfs_api::internal::control(fixture.owner.as_mut().unwrap()).channel_work(),
        before
    );
    fixture.api.close(fixture.owner.take().unwrap()).unwrap();
    let calls = fixture.calls();
    assert_eq!(calls.len(), 9);
    let request = std::str::from_utf8(&calls[6]).unwrap();
    assert!(request.starts_with(&format!("POST /v1.54/containers/{CID}/exec ")));
    assert!(request.contains(r#""WorkingDir":"/workspaces/selected-mount""#));
    assert!(request.contains(r#""User":"501:20""#));
    assert!(!calls
        .iter()
        .any(|call| call.windows(12).any(|bytes| bytes == b"/exec/start ")));
}
#[test]
fn workspace_exec_failure_keeps_exact_original_request_and_container() {
    let mut fixture = Fixture::new(vec![
        fixed(409, b"original exec refusal"),
        fixed(204, b""),
        fixed(204, b""),
    ]);
    let failure = WorkspaceApi::exec(
        fixture.owner.as_ref().unwrap(),
        &mounted(),
        vec!["/bin/pwd".into()],
        vec![],
        true,
    )
    .unwrap_err();
    assert_eq!(failure.container.to_string(), CID);
    assert_eq!(failure.request.directory, "/workspaces/selected-mount");
    assert_eq!(failure.request.arguments, ["/bin/pwd"]);
    assert!(failure.request.stdin);
    assert!(failure.transfer.attempted);
    fixture.api.close(fixture.owner.take().unwrap()).unwrap();
    assert_eq!(fixture.calls().len(), 9);
}

struct ProjectFixture {
    directory: PathBuf,
    project: layerfs_api::SealedProject,
}
impl ProjectFixture {
    fn new() -> Self {
        use layerfs_history::{BranchId, HistoryCatalogConfig, HistoryName, LayerStackId};
        use layerfs_persistence::{PersistenceConfig, SqlitePersistenceProfile};
        let directory = std::env::temp_dir().join(format!(
            "sdk-existing-project-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let source = directory.join("source");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("payload"), b"original complete project").unwrap();
        let request = layerfs_api::InitRequest {
            source,
            store: PersistenceConfig::sqlite(directory.join("store.sqlite"))
                .with_sqlite_profile(SqlitePersistenceProfile::Disposable),
            locator: "/layerfs-store/global/store.sqlite".into(),
            policy: layerfs_storage::StoragePolicy::frozen_default(),
            catalog: HistoryCatalogConfig {
                binding_key: b"sdk-existing-project".to_vec(),
                incarnation: 1,
                cursor_key: [7; 32],
            },
            stack: LayerStackId::from_authority([3; 16]),
            stack_name: HistoryName::new("project").unwrap(),
            branch: BranchId::from_authority([4; 16]),
            branch_name: HistoryName::new("main").unwrap(),
            scope_seed: [5; 32],
            deadline: Instant::now() + Duration::from_secs(10),
        };
        let project = layerfs_telemetry::timer::Timing::disabled("sdk.existing.project", |timer| {
            layerfs_api::ProjectApi::new().init(request, timer)
        })
        .0
        .unwrap();
        Self { directory, project }
    }
}
impl Drop for ProjectFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}
#[test]
fn create_matching_existing_project_keeps_one_original_hello_and_does_not_reinstall() {
    let project = ProjectFixture::new();
    let mut fixture = Fixture::new_for(
        Some(&project.project),
        vec![fixed(204, b""), fixed(204, b"")],
    );
    let owner = fixture.owner.as_mut().unwrap();
    assert_eq!(owner.startup.phase, DaemonPhase::ControlReady);
    assert!(owner.installed.is_none());
    assert_eq!(
        layerfs_api::internal::control(owner)
            .channel_work()
            .0
            .records,
        1
    );
    fixture.api.close(fixture.owner.take().unwrap()).unwrap();
    assert_eq!(fixture.calls().len(), 8);
}
#[test]
fn create_mismatched_existing_project_refuses_before_any_deployment_or_control_effect() {
    let project = ProjectFixture::new();
    let (docker, peer) = Peer::new(vec![], 4096);
    let api = SandboxApi::new(docker);
    let mut deployment_setup = setup([32; 32], duplex::public_key(&[31; 32]).unwrap());
    let mut manifest = project.project.manifest.clone();
    manifest.root[0] ^= 1;
    deployment_setup.existing_store = Some(manifest);
    let result = api.create(
        SandboxCreate {
            deployment: layerfs_sandbox::backend::docker::SandboxRequest {
                image: IMAGE.into(),
                store_volume: "lfs-test-borrowed".into(),
                port: 30421,
                #[cfg(target_os = "linux")]
                host_export: None,
                labels: vec![],
            },
            setup: deployment_setup,
            executable: project.project.store.path.clone(),
            controller_private: [31; 32],
            daemon_peer: duplex::public_key(&[32; 32]).unwrap(),
            startup_wait: Duration::from_secs(2),
            connect_host: None,
        },
        &project.project,
    );
    let Err(failure) = result else {
        panic!("mismatched Project must refuse before effects")
    };
    assert_eq!(failure.phase, SandboxPhase::Validate);
    assert!(failure.sandbox.is_none() && failure.control.is_none() && failure.installed.is_none());
    assert!(peer.finish().is_empty());
}

#[test]
fn host_export_setup_without_its_lease_is_refused_before_deployment() {
    let project = ProjectFixture::new();
    let (docker, peer) = Peer::new(vec![], 4096);
    let mut deployment_setup = setup([32; 32], duplex::public_key(&[31; 32]).unwrap());
    deployment_setup.host_export = Some(layerfs_bridge::daemon_setup::HostExportIdentity {
        boot_id: "01234567-89ab-cdef-0123-456789abcdef".into(),
        device: 1,
        inode: 2,
        shared_peer: 3,
    });
    let result = SandboxApi::new(docker).create(
        SandboxCreate {
            deployment: layerfs_sandbox::backend::docker::SandboxRequest {
                image: IMAGE.into(),
                store_volume: "lfs-test-borrowed".into(),
                port: 30421,
                #[cfg(target_os = "linux")]
                host_export: None,
                labels: vec![],
            },
            setup: deployment_setup,
            executable: project.project.store.path.clone(),
            controller_private: [31; 32],
            daemon_peer: duplex::public_key(&[32; 32]).unwrap(),
            startup_wait: Duration::from_secs(2),
            connect_host: None,
        },
        &project.project,
    );
    let Err(failure) = result else {
        panic!("unleased export must be refused before effects")
    };
    assert_eq!(failure.phase, SandboxPhase::Validate);
    assert!(matches!(failure.cause, SandboxCause::Setup(_)));
    assert!(failure.sandbox.is_none() && failure.control.is_none());
    assert!(peer.finish().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires a caller-owned empty shared host export"]
fn host_export_setup_with_a_different_identity_is_refused_before_deployment() {
    use layerfs_sandbox::{export_lease::HostExportLease, host_export::HostExportRoot};
    use std::sync::Arc;
    let root = std::env::var_os("LAYERFS_EXPORT_ROOT").unwrap();
    let lease = Arc::new(
        HostExportLease::try_acquire(HostExportRoot::open_fresh(root.as_ref()).unwrap()).unwrap(),
    );
    let observed = lease.root().identity();
    let project = ProjectFixture::new();
    let (docker, peer) = Peer::new(vec![], 4096);
    let mut deployment_setup = setup([32; 32], duplex::public_key(&[31; 32]).unwrap());
    deployment_setup.host_export = Some(layerfs_bridge::daemon_setup::HostExportIdentity {
        boot_id: observed.boot_id.clone(),
        device: observed.device,
        inode: observed.inode ^ 1,
        shared_peer: observed.shared_peer,
    });
    let result = SandboxApi::new(docker).create(
        SandboxCreate {
            deployment: layerfs_sandbox::backend::docker::SandboxRequest {
                image: IMAGE.into(),
                store_volume: "lfs-test-borrowed".into(),
                port: 30421,
                host_export: Some(lease),
                labels: vec![],
            },
            setup: deployment_setup,
            executable: project.project.store.path.clone(),
            controller_private: [31; 32],
            daemon_peer: duplex::public_key(&[32; 32]).unwrap(),
            startup_wait: Duration::from_secs(2),
            connect_host: None,
        },
        &project.project,
    );
    let Err(failure) = result else {
        panic!("mismatched export must be refused before effects")
    };
    assert_eq!(failure.phase, SandboxPhase::Validate);
    assert!(matches!(failure.cause, SandboxCause::Setup(_)));
    assert!(failure.sandbox.is_none() && failure.control.is_none());
    assert!(peer.finish().is_empty());
}

#[test]
fn fresh_durable_workspace_outside_store_volume_refuses_before_deployment() {
    let project = ProjectFixture::new();
    let (docker, peer) = Peer::new(vec![], 4096);
    let api = SandboxApi::new(docker);
    let mut deployment_setup = setup([32; 32], duplex::public_key(&[31; 32]).unwrap());
    deployment_setup.fresh_workspace = Some(FreshWorkspaceSetup {
        sandbox: [9; 32],
        catalog_directory: "/layerfs-store/sandboxes/one/catalog".into(),
        overlay_directory: "/layerfs-local/durable".into(),
        create_directories: false,
    });
    let result = api.create(
        SandboxCreate {
            deployment: layerfs_sandbox::backend::docker::SandboxRequest {
                image: IMAGE.into(),
                store_volume: "lfs-test-borrowed".into(),
                port: 30421,
                #[cfg(target_os = "linux")]
                host_export: None,
                labels: vec![],
            },
            setup: deployment_setup,
            executable: project.project.store.path.clone(),
            controller_private: [31; 32],
            daemon_peer: duplex::public_key(&[32; 32]).unwrap(),
            startup_wait: Duration::from_secs(2),
            connect_host: None,
        },
        &project.project,
    );
    let Err(failure) = result else {
        panic!("volatile Workspace path must refuse")
    };
    assert_eq!(failure.phase, SandboxPhase::Validate);
    assert!(matches!(failure.cause, SandboxCause::Setup(_)));
    assert!(failure.sandbox.is_none() && failure.control.is_none());
    assert!(peer.finish().is_empty());
}

#[test]
fn created_workspace_must_use_the_actual_docker_volume_root() {
    let project = ProjectFixture::new();
    let (docker, peer) = Peer::new(vec![], 4096);
    let api = SandboxApi::new(docker);
    let mut deployment_setup = setup([32; 32], duplex::public_key(&[31; 32]).unwrap());
    let base = format!("/layerfs-store/other/layerfs-durable/{}", "09".repeat(32));
    deployment_setup.store = "/layerfs-store/other/global/store.sqlite".into();
    deployment_setup.fresh_workspace = Some(FreshWorkspaceSetup {
        sandbox: [9; 32],
        catalog_directory: format!("{base}/catalog"),
        overlay_directory: format!("{base}/overlays"),
        create_directories: true,
    });
    let result = api.create(
        SandboxCreate {
            deployment: layerfs_sandbox::backend::docker::SandboxRequest {
                image: IMAGE.into(),
                store_volume: "lfs-test-borrowed".into(),
                port: 30421,
                #[cfg(target_os = "linux")]
                host_export: None,
                labels: vec![],
            },
            setup: deployment_setup,
            executable: project.project.store.path.clone(),
            controller_private: [31; 32],
            daemon_peer: duplex::public_key(&[32; 32]).unwrap(),
            startup_wait: Duration::from_secs(2),
            connect_host: None,
        },
        &project.project,
    );
    let Err(failure) = result else {
        panic!("wrong Store volume root must refuse")
    };
    assert_eq!(failure.phase, SandboxPhase::Validate);
    assert!(matches!(failure.cause, SandboxCause::Setup(_)));
    assert!(peer.finish().is_empty());
}

fn recovery_inputs(project: &ProjectFixture) -> (SandboxCreate, RecoveryWorkspaceSetup) {
    let mut deployment_setup = setup([32; 32], duplex::public_key(&[31; 32]).unwrap());
    let mut manifest = project.project.manifest.clone();
    manifest.profile = layerfs_bridge::provision::StoreProfile::Durable;
    manifest.daemon_sqlite = Some("3.51.0".into());
    deployment_setup.existing_store = Some(manifest);
    let request = SandboxCreate {
        deployment: layerfs_sandbox::backend::docker::SandboxRequest {
            image: IMAGE.into(),
            store_volume: "lfs-test-borrowed".into(),
            port: 30421,
            #[cfg(target_os = "linux")]
            host_export: None,
            labels: vec![],
        },
        setup: deployment_setup,
        executable: project.project.store.path.clone(),
        controller_private: [31; 32],
        daemon_peer: duplex::public_key(&[32; 32]).unwrap(),
        startup_wait: Duration::from_secs(2),
        connect_host: None,
    };
    let recovery = RecoveryWorkspaceSetup {
        sandbox: [9; 32],
        catalog_directory: "/layerfs-store/sandboxes/one/catalog".into(),
        overlay_directory: "/layerfs-store/sandboxes/one/overlays".into(),
        minimum_namespace: 4,
        boot_id: "12345678-1234-1234-1234-123456789abc".into(),
        store_device: 42,
        store_inode: 123,
    };
    (request, recovery)
}

#[test]
fn recovery_refuses_invalid_or_volatile_authority_before_any_engine_effect() {
    let project = ProjectFixture::new();
    for invalid in 0..5 {
        let (docker, peer) = Peer::new(vec![], 4096);
        let (mut request, mut recovery) = recovery_inputs(&project);
        match invalid {
            0 => request.setup.existing_store = None,
            1 => {
                request.setup.existing_store.as_mut().unwrap().profile =
                    layerfs_bridge::provision::StoreProfile::Disposable;
            }
            2 => recovery.overlay_directory = "/layerfs-local/volatile".into(),
            3 => recovery.catalog_directory = "/layerfs-store/../outside".into(),
            4 => recovery.minimum_namespace = 0,
            _ => unreachable!(),
        }
        let expected = recovery.clone();
        let result = SandboxApi::new(docker).recover_observed(request, recovery, |_| {
            panic!("validation must precede the observer")
        });
        let Err(failure) = result else {
            panic!("invalid recovery must fail")
        };
        assert_eq!(failure.phase, SandboxPhase::Validate);
        assert_eq!(failure.recovery, Some(expected));
        assert!(matches!(failure.cause, SandboxCause::Setup(_)));
        assert!(failure.sandbox.is_none() && failure.control.is_none());
        assert!(peer.finish().is_empty());
    }
}

#[test]
fn recovery_observer_before_start_retains_authority_and_uploads_recovery_record() {
    let project = ProjectFixture::new();
    let (docker, peer) = Peer::new(vec![
        fixed(200, b"{\"Name\":\"lfs-test-borrowed\",\"Driver\":\"local\",\"Scope\":\"local\",\"Options\":null}"),
        fixed(201, format!("{{\"Id\":\"{CID}\",\"Warnings\":[]}}").as_bytes()),
        fixed(200, b""),
    ], 4096);
    let (request, recovery) = recovery_inputs(&project);
    let config = RecoveryDaemonSetup {
        daemon: request.setup.clone(),
        workspace: recovery.clone(),
    }
    .encode()
    .unwrap();
    let mut seen = Vec::new();
    let result = SandboxApi::new(docker).recover_observed(request, recovery.clone(), |item| {
        seen.push(item.milestone);
        if item.milestone == SandboxMilestone::BeforeStart {
            return Err(std::io::Error::other("journal unavailable"));
        }
        Ok(())
    });
    let Err(failure) = result else {
        panic!("failed journal must stop startup")
    };
    assert_eq!(failure.phase, SandboxPhase::Record);
    assert_eq!(failure.recovery, Some(recovery));
    assert!(matches!(
        failure.cause,
        SandboxCause::Observer {
            milestone: SandboxMilestone::BeforeStart,
            ..
        }
    ));
    assert_eq!(
        failure.sandbox.as_ref().unwrap().identity().to_string(),
        CID
    );
    assert!(failure.control.is_none());
    assert_eq!(
        seen,
        [
            SandboxMilestone::BeforeCreate,
            SandboxMilestone::Created,
            SandboxMilestone::Uploaded,
            SandboxMilestone::BeforeStart,
        ]
    );
    let calls = peer.finish();
    assert_eq!(calls.len(), 3);
    assert!(calls[2].windows(config.len()).any(|bytes| bytes == config));
    assert!(!calls
        .iter()
        .any(|call| call.starts_with(format!("POST /v1.54/containers/{CID}/start").as_bytes())));
}

#[test]
fn recovered_owner_and_failed_close_preserve_original_recovery_authority() {
    let project = ProjectFixture::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let daemon = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut channel =
            duplex::accept(socket, &[32; 32], duplex::public_key(&[31; 32]).unwrap()).unwrap();
        let call = Call::decode(channel.receive.receive().unwrap()).unwrap();
        assert!(matches!(call.request, Request::Hello(request) if request.wait_for_store));
        channel
            .send
            .send(
                &Answer {
                    id: call.id,
                    reply: Reply::Hello(DaemonStatus {
                        instance: [9; 32],
                        phase: DaemonPhase::ControlReady,
                        overlay_sqlite: Some("3.51.0".into()),
                        store_sqlite: Some("3.51.0".into()),
                    }),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
    });
    let marker = frame(1, b"LAYERFS_DAEMON_LISTEN 0.0.0.0:30421\n");
    let mut logs = format!("HTTP/1.1 200 OK\r\nContent-Type: application/vnd.docker.multiplexed-stream\r\nContent-Length: {}\r\n\r\n", marker.len()).into_bytes();
    logs.extend(marker);
    let (docker, peer) = Peer::new(vec![
        fixed(200, b"{\"Name\":\"lfs-test-borrowed\",\"Driver\":\"local\",\"Scope\":\"local\",\"Options\":null}"),
        fixed(201, format!("{{\"Id\":\"{CID}\",\"Warnings\":[]}}").as_bytes()),
        fixed(200, b""), fixed(204, b""), logs, endpoint(port),
        fixed(409, b"original stop refusal"),
    ], 4096);
    let (request, recovery) = recovery_inputs(&project);
    let api = SandboxApi::new(docker);
    let mut seen = Vec::new();
    let owner = api
        .recover_observed(request, recovery.clone(), |item| {
            seen.push(item.milestone);
            Ok(())
        })
        .unwrap();
    assert_eq!(owner.recovery(), Some(&recovery));
    assert!(owner.installed.is_none());
    assert_eq!(
        seen,
        [
            SandboxMilestone::BeforeCreate,
            SandboxMilestone::Created,
            SandboxMilestone::Uploaded,
            SandboxMilestone::BeforeStart,
            SandboxMilestone::Started,
            SandboxMilestone::Authenticated
        ]
    );
    let failure = api.close(owner).unwrap_err();
    assert_eq!(failure.phase, SandboxPhase::Stop);
    assert_eq!(failure.recovery, Some(recovery));
    assert!(failure.sandbox.is_some() && failure.control.is_some());
    assert_eq!(peer.finish().len(), 7);
    daemon.join().unwrap();
}

#[test]
fn lifecycle_observer_failure_before_start_keeps_original_container() {
    let project = ProjectFixture::new();
    let binary = project.directory.join("daemon");
    fs::write(&binary, b"daemon executable").unwrap();
    let (docker, peer) = Peer::new(vec![
        fixed(200, b"{\"Name\":\"lfs-test-borrowed\",\"Driver\":\"local\",\"Scope\":\"local\",\"Options\":null}"),
        fixed(201, format!("{{\"Id\":\"{CID}\",\"Warnings\":[]}}").as_bytes()),
        fixed(200, b""),
    ], 4096);
    let request = SandboxCreate {
        deployment: layerfs_sandbox::backend::docker::SandboxRequest {
            image: IMAGE.into(),
            store_volume: "lfs-test-borrowed".into(),
            port: 30421,
            #[cfg(target_os = "linux")]
            host_export: None,
            labels: vec![],
        },
        setup: setup([32; 32], duplex::public_key(&[31; 32]).unwrap()),
        executable: binary,
        controller_private: [31; 32],
        daemon_peer: duplex::public_key(&[32; 32]).unwrap(),
        startup_wait: Duration::from_secs(2),
        connect_host: None,
    };
    let mut seen = Vec::new();
    let result = SandboxApi::new(docker).create_file_observed(
        request,
        &project.project.store,
        &project.project.manifest,
        |observation| {
            seen.push(observation.milestone);
            if observation.milestone == SandboxMilestone::BeforeCreate {
                assert!(observation.runtime.is_none());
            } else {
                assert_eq!(observation.runtime.unwrap().identity().to_string(), CID);
            }
            if observation.milestone == SandboxMilestone::BeforeStart {
                return Err(std::io::Error::other("injected journal failure"));
            }
            Ok(())
        },
    );
    let Err(failure) = result else {
        panic!("failed journal must stop startup")
    };
    assert_eq!(failure.phase, SandboxPhase::Record);
    assert!(
        matches!(failure.cause, SandboxCause::Observer { milestone: SandboxMilestone::BeforeStart, ref cause } if cause.to_string() == "injected journal failure")
    );
    assert_eq!(
        failure.sandbox.as_ref().unwrap().identity().to_string(),
        CID
    );
    assert!(failure.control.is_none() && failure.observed.is_none());
    assert_eq!(
        seen,
        [
            SandboxMilestone::BeforeCreate,
            SandboxMilestone::Created,
            SandboxMilestone::Uploaded,
            SandboxMilestone::BeforeStart
        ]
    );
    let calls = peer.finish();
    assert_eq!(calls.len(), 3);
    assert!(!calls
        .iter()
        .any(|call| call.starts_with(format!("POST /v1.54/containers/{CID}/start").as_bytes())));
}

#[test]
fn lifecycle_observer_failure_after_hello_keeps_control_and_original_status() {
    let project = ProjectFixture::new();
    let binary = project.directory.join("daemon-ready");
    fs::write(&binary, b"daemon executable").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let daemon = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut channel =
            duplex::accept(socket, &[32; 32], duplex::public_key(&[31; 32]).unwrap()).unwrap();
        let call = Call::decode(channel.receive.receive().unwrap()).unwrap();
        assert!(matches!(call.request, Request::Hello(_)));
        channel
            .send
            .send(
                &Answer {
                    id: call.id,
                    reply: Reply::Hello(DaemonStatus {
                        instance: [9; 32],
                        phase: DaemonPhase::ControlReady,
                        overlay_sqlite: Some("3.51.0".into()),
                        store_sqlite: Some("3.51.0".into()),
                    }),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
    });
    let marker = frame(1, b"LAYERFS_DAEMON_LISTEN 0.0.0.0:30421\n");
    let mut logs = format!("HTTP/1.1 200 OK\r\nContent-Type: application/vnd.docker.multiplexed-stream\r\nContent-Length: {}\r\n\r\n", marker.len()).into_bytes();
    logs.extend(marker);
    let (docker, peer) = Peer::new(vec![
        fixed(200, b"{\"Name\":\"lfs-test-borrowed\",\"Driver\":\"local\",\"Scope\":\"local\",\"Options\":null}"),
        fixed(201, format!("{{\"Id\":\"{CID}\",\"Warnings\":[]}}").as_bytes()),
        fixed(200, b""), fixed(204, b""), logs, endpoint(port),
    ], 4096);
    let mut deployment_setup = setup([32; 32], duplex::public_key(&[31; 32]).unwrap());
    let mut manifest = project.project.manifest.clone();
    manifest.daemon_sqlite = Some("3.51.0".into());
    deployment_setup.existing_store = Some(manifest);
    let request = SandboxCreate {
        deployment: layerfs_sandbox::backend::docker::SandboxRequest {
            image: IMAGE.into(),
            store_volume: "lfs-test-borrowed".into(),
            port: 30421,
            #[cfg(target_os = "linux")]
            host_export: None,
            labels: vec![],
        },
        setup: deployment_setup,
        executable: binary,
        controller_private: [31; 32],
        daemon_peer: duplex::public_key(&[32; 32]).unwrap(),
        startup_wait: Duration::from_secs(2),
        connect_host: None,
    };
    let mut seen = Vec::new();
    let result = SandboxApi::new(docker).create_file_observed(
        request,
        &project.project.store,
        &project.project.manifest,
        |observation| {
            seen.push(observation.milestone);
            if observation.milestone == SandboxMilestone::Authenticated {
                assert_eq!(observation.status.unwrap().instance, [9; 32]);
                return Err(std::io::Error::other("injected ready receipt failure"));
            }
            Ok(())
        },
    );
    let Err(failure) = result else {
        panic!("failed ready receipt must retain ownership")
    };
    assert_eq!(failure.phase, SandboxPhase::Record);
    assert!(matches!(
        failure.cause,
        SandboxCause::Observer {
            milestone: SandboxMilestone::Authenticated,
            ..
        }
    ));
    assert_eq!(
        failure.sandbox.as_ref().unwrap().identity().to_string(),
        CID
    );
    assert_eq!(failure.observed.as_ref().unwrap().instance, [9; 32]);
    assert!(failure.control.is_some());
    assert_eq!(
        seen,
        [
            SandboxMilestone::BeforeCreate,
            SandboxMilestone::Created,
            SandboxMilestone::Uploaded,
            SandboxMilestone::BeforeStart,
            SandboxMilestone::Started,
            SandboxMilestone::Authenticated
        ]
    );
    assert_eq!(peer.finish().len(), 6);
    daemon.join().unwrap();
}
