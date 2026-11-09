//! Durable Node mount custody through the actual binding and authenticated channel.
#![cfg(target_os = "linux")]
mod support;
use layerfs_bridge::{
    control::{
        Answer, DaemonPhase, DaemonStatus, MountReceipt, ReadyMount, Reply, Request, SdkValue,
    },
    duplex::{accept, generate_keypair},
    provision::StoreManifest,
};
use layerfs_history::BranchSnapshot;
use layerfs_persistence::SqlitePersistenceProfile;
use layerfs_store_service::StoreService;
use std::{
    fs, net::TcpListener, os::unix::fs::DirBuilderExt, path::Path, process::Command, thread,
    time::Duration,
};
use support::{engine::*, *};

fn read_value(path: &Path) -> SdkValue {
    let answer = Answer::decode(&fs::read(path).unwrap()).unwrap();
    match answer.reply {
        Reply::Sdk(value) => value,
        other => panic!("unexpected reply {other:?}"),
    }
}
fn write_value(path: &Path, value: SdkValue) {
    fs::write(
        path,
        Answer {
            id: 1,
            reply: Reply::Sdk(value),
        }
        .encode()
        .unwrap(),
    )
    .unwrap();
}
fn field<'a>(value: &'a SdkValue, name: &str) -> &'a [u8] {
    match value.get(name) {
        Some(SdkValue::Bytes(bytes)) => bytes,
        _ => panic!("missing {name}"),
    }
}
fn setup_project(home: &Home) -> BranchSnapshot {
    for name in ["source", "volume", "volume/global"] {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(home.0.join(name))
            .unwrap();
    }
    fs::write(home.0.join("source/file"), b"fixture").unwrap();
    home.node(
        "const r=await native.call('project.init',{...args,name:'durable-project',path:process.env.LAYERFS_HOME+'/source',storePath:process.env.LAYERFS_HOME+'/volume/global/store.sqlite',storeProfile:'durable',timeoutMs:15000}); assert.equal(r.refused?.code,'Failed',JSON.stringify(r.refused)); assert.match(r.refused.message,/must run as user 0/);",
        None, Duration::from_secs(20),
    );
    let project = home.0.join("durable-project");
    let record = project.join("project.record");
    let mut value = read_value(&record);
    let mut manifest = StoreManifest::decode(field(&value, "manifest")).unwrap();
    manifest.daemon_sqlite = Some(manifest.host_sqlite.clone());
    if let SdkValue::Map(fields) = &mut value {
        fields
            .iter_mut()
            .find(|(name, _)| name == "installed")
            .unwrap()
            .1 = SdkValue::Bytes(manifest.encode().unwrap());
    }
    write_value(&record, value);
    let store = home.0.join("volume/global/store.sqlite");
    fs::copy(project.join("sealed.sqlite"), &store).unwrap();
    let service = StoreService::open_direct_with_profile(
        &store,
        &manifest.binding,
        manifest.cursor_key,
        &home.0.join("test-overlay.sqlite"),
        SqlitePersistenceProfile::Durable,
    )
    .unwrap();
    let stack =
        layerfs_store_service::history::lookup::project(&service.store, "durable-project").unwrap();
    layerfs_store_service::history::branch::fork(&service.store, &stack, "feature", None).unwrap();
    let branch = layerfs_store_service::history::lookup::branch(&service.store, &stack, "feature")
        .unwrap()
        .id;
    service
        .store
        .published(|history| history.branch_snapshot(branch))
        .unwrap()
        .unwrap()
}
fn install_sandbox(home: &Home, secret: [u8; 32], peer: [u8; 32]) {
    home.record_with_durability(CID, secret, peer, None, Some(SdkValue::Bytes(vec![8; 32])));
    let source = home.0.join("proj/sandboxes/1/sandbox.record");
    let mut value = read_value(&source);
    if let SdkValue::Map(fields) = &mut value {
        fields
            .iter_mut()
            .find(|(name, _)| name == "project")
            .unwrap()
            .1 = SdkValue::text("durable-project");
    }
    let root = home.0.join("durable-project/sandboxes");
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("1"))
        .unwrap();
    fs::copy(source, root.join("1/sandbox.record")).unwrap();
    write_value(&root.join("1/sandbox.record"), value);
}
fn intent(directory: &Path) -> SdkValue {
    let entries = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.starts_with("mount-intent-") && name.ends_with(".record")
        })
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 1);
    read_value(&entries[0])
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Ready,
    TwoReady,
    ConcurrentReady,
    LostReady,
    BoundSync,
    BoundSwap,
}
fn run(mode: Mode) {
    let lost_ready = mode == Mode::LostReady;
    let incomplete = matches!(mode, Mode::LostReady | Mode::BoundSync | Mode::BoundSwap);
    let two_ready = matches!(mode, Mode::TwoReady | Mode::ConcurrentReady);
    let concurrent = mode == Mode::ConcurrentReady;
    let swapped = mode == Mode::BoundSwap;
    let home = Home::new();
    let binding = setup_project(&home);
    let daemon = generate_keypair().unwrap();
    let client = generate_keypair().unwrap();
    install_sandbox(&home, client.private, daemon.public);
    let directory = home.0.join("durable-project/sandboxes/1");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let running = exited()
        .replace("\"Running\":false", "\"Running\":true")
        .replace(
            "\"Ports\":{}",
            &format!(r#""Ports":{{"30421/tcp":[{{"HostIp":"127.0.0.1","HostPort":"{port}"}}]}}"#),
        );
    let engine = engines(
        &home.0,
        vec![(200, running.clone()); if two_ready { 3 } else { 2 }],
    );
    let observed_directory = directory.clone();
    let worker = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut connection = accept(stream, &daemon.private, client.public).unwrap();
        let hello =
            layerfs_bridge::control::Call::decode(connection.receive.receive().unwrap()).unwrap();
        assert!(matches!(hello.request, Request::Hello(_)));
        connection
            .send
            .send(
                &Answer {
                    id: hello.id,
                    reply: Reply::Hello(DaemonStatus {
                        instance: [7; 32],
                        phase: DaemonPhase::ControlReady,
                        overlay_sqlite: Some("3.50.4".into()),
                        store_sqlite: Some("3.50.4".into()),
                    }),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
        let call =
            layerfs_bridge::control::Call::decode(connection.receive.receive().unwrap()).unwrap();
        let (workspace, branch) = match call.request {
            Request::Mount { workspace, branch } => (workspace, branch),
            other => panic!("unexpected request {other:?}"),
        };
        assert_eq!(branch, binding.branch.id);
        assert_eq!(
            intent(&observed_directory).get_text("phase"),
            Some("beforeBind")
        );
        if concurrent {
            thread::sleep(Duration::from_millis(500));
        }
        let token = layerfs_bridge::control::WorkspaceToken {
            workspace,
            namespace: 7,
        };
        connection
            .send
            .send(
                &Answer {
                    id: call.id,
                    reply: Reply::Bound {
                        token,
                        binding: binding.clone(),
                    },
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
        if matches!(mode, Mode::BoundSync | Mode::BoundSwap) {
            assert!(
                connection.receive.receive().is_err(),
                "failed binding sync must not send Attach"
            );
        } else {
            let call = layerfs_bridge::control::Call::decode(connection.receive.receive().unwrap())
                .unwrap();
            assert_eq!(call.request, Request::Attach(token));
            let saved = intent(&observed_directory);
            assert_eq!(saved.get_text("phase"), Some("beforeAttach"));
            assert!(!field(&saved, "bound").is_empty());
            if !incomplete {
                let ready = ReadyMount {
                    token,
                    directory: "/workspaces/fixture".into(),
                    receipt: MountReceipt {
                        mount: 2,
                        root: 7,
                        mount_id: 911,
                        device_major: 0,
                        device_minor: 64,
                        abi_major: 7,
                        abi_minor: 41,
                        offered: 1,
                        selected: 1,
                        max_write: 131072,
                        max_readahead: 65536,
                        max_background: 12,
                        congestion_threshold: 9,
                        page_size: 4096,
                        loops: 2,
                        abort_bound: true,
                    },
                };
                connection
                    .send
                    .send(
                        &Answer {
                            id: call.id,
                            reply: Reply::Ready(Box::new(ready.clone())),
                        }
                        .encode()
                        .unwrap(),
                    )
                    .unwrap();
                if two_ready {
                    let next = layerfs_bridge::control::Call::decode(
                        connection.receive.receive().unwrap(),
                    )
                    .unwrap();
                    let next_workspace = match next.request {
                        Request::Mount { workspace, branch } if branch == binding.branch.id => {
                            workspace
                        }
                        other => panic!("unexpected second request {other:?}"),
                    };
                    assert_ne!(next_workspace, token.workspace);
                    let next_token = layerfs_bridge::control::WorkspaceToken {
                        workspace: next_workspace,
                        namespace: 8,
                    };
                    connection
                        .send
                        .send(
                            &Answer {
                                id: next.id,
                                reply: Reply::Bound {
                                    token: next_token,
                                    binding,
                                },
                            }
                            .encode()
                            .unwrap(),
                        )
                        .unwrap();
                    let attach = layerfs_bridge::control::Call::decode(
                        connection.receive.receive().unwrap(),
                    )
                    .unwrap();
                    assert_eq!(attach.request, Request::Attach(next_token));
                    let mut next_ready = ready;
                    next_ready.token = next_token;
                    next_ready.directory = "/workspaces/fixture2".into();
                    connection
                        .send
                        .send(
                            &Answer {
                                id: attach.id,
                                reply: Reply::Ready(Box::new(next_ready)),
                            }
                            .encode()
                            .unwrap(),
                        )
                        .unwrap();
                }
            }
        }
    });
    let action = if swapped {
        "const r=await native.call('workspace.mount',a); assert.equal(r.refused?.code,'Unknown',JSON.stringify(r.refused)); const again=await native.call('workspace.mount',a); assert.equal(again.refused?.code,'NoSuchSandbox',JSON.stringify(again.refused));"
    } else if incomplete {
        "const r=await native.call('workspace.mount',a); assert.equal(r.refused?.code,'Unknown',JSON.stringify(r.refused)); const listed=await native.call('sandbox.list_workspaces',a); assert.equal(listed.refused?.code,'RecoveryRequired',JSON.stringify(listed.refused)); const again=await native.call('workspace.mount',a); assert.equal(again.refused?.code,'RecoveryRequired',JSON.stringify(again.refused));"
    } else if concurrent {
        "const fs=require('node:fs'); const d=process.env.LAYERFS_HOME+'/durable-project/sandboxes/1'; const first=native.call('workspace.mount',a); let seen=false; for(let i=0;i<200;i++){if(fs.readdirSync(d).some(n=>n.startsWith('mount-intent-'))){seen=true;break;} await new Promise(r=>setTimeout(r,5));} assert.ok(seen,'first mount intent was not observed'); const second=native.call('workspace.mount',a); const [r,next]=await Promise.all([first,second]); assert.equal(r.value?.path,'/workspaces/fixture',JSON.stringify(r.refused)); assert.equal(next.value?.path,'/workspaces/fixture2',JSON.stringify(next.refused));"
    } else if two_ready {
        "const r=await native.call('workspace.mount',a); assert.equal(r.value?.path,'/workspaces/fixture',JSON.stringify(r.refused)); const next=await native.call('workspace.mount',a); assert.equal(next.value?.path,'/workspaces/fixture2',JSON.stringify(next.refused));"
    } else {
        "const r=await native.call('workspace.mount',a); assert.equal(r.value?.path,'/workspaces/fixture',JSON.stringify(r.refused));"
    };
    let script = format!("const a={{...args,project:'durable-project',branch:'feature',storePath:process.env.LAYERFS_HOME+'/volume/global/store.sqlite'}}; const opened=await native.storeOpen(a); assert.ok(!opened.refused,JSON.stringify(opened.refused)); {action}");
    if matches!(mode, Mode::BoundSync | Mode::BoundSwap) {
        let shim = home.0.join("mount-fault.so");
        assert!(Command::new("cc")
            .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/mount_record_fault.c"))
            .args(["-ldl", "-o"])
            .arg(&shim)
            .status()
            .unwrap()
            .success());
        home.node_with_env(
            &script,
            Some(&shim),
            &[(
                "LAYERFS_TEST_MOUNT_FAULT",
                if swapped { "swap" } else { "sync" },
            )],
            Duration::from_secs(20),
        );
        assert!(home
            .0
            .join(if swapped {
                "mount-parent-swapped"
            } else {
                "mount-sync-failed"
            })
            .exists());
    } else {
        home.node(&script, None, Duration::from_secs(20));
    }
    if incomplete && !swapped {
        fs::remove_file(home.0.join("docker.sock")).unwrap();
        home.node("const a={...args,project:'durable-project',branch:'feature',storePath:process.env.LAYERFS_HOME+'/volume/global/store.sqlite'}; const opened=await native.storeOpen(a); assert.ok(!opened.refused,JSON.stringify(opened.refused)); const listed=await native.call('sandbox.list_workspaces',a); assert.equal(listed.refused?.code,'RecoveryRequired',JSON.stringify(listed.refused)); const result=await native.call('workspace.mount',a); assert.equal(result.refused?.code,'RecoveryRequired',JSON.stringify(result.refused));", None, Duration::from_secs(20));
    }
    worker.join().unwrap();
    assert_eq!(engine.join().unwrap().len(), if two_ready { 3 } else { 2 });
    if two_ready {
        let intents = directory
            .read_dir()
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                name.starts_with("mount-intent-") && name.ends_with(".record")
            })
            .collect::<Vec<_>>();
        assert_eq!(intents.len(), 2);
        assert!(intents
            .iter()
            .all(|path| read_value(path).get_text("phase") == Some("complete")));
        return;
    }
    let original_sandbox = home.0.join("original-sandbox");
    let saved = intent(if swapped {
        &original_sandbox
    } else {
        &directory
    });
    if incomplete {
        assert_eq!(
            saved.get_text("phase"),
            Some(if lost_ready {
                "beforeAttach"
            } else {
                "beforeBind"
            })
        );
        assert!(!directory.read_dir().unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("workspace-")));
        if swapped {
            assert!(!directory.read_dir().unwrap().any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("mount-intent-")));
        }
    } else {
        assert_eq!(saved.get_text("phase"), Some("complete"));
        assert!(!field(&saved, "ready").is_empty());
        assert!(directory.read_dir().unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("workspace-")));
    }
}
#[test]
fn mount_intent_precedes_both_sends_and_ready_is_recorded() {
    run(Mode::Ready)
}
#[test]
fn completed_receipts_allow_another_distinct_mount() {
    run(Mode::TwoReady)
}
#[test]
fn concurrent_mount_waits_for_an_active_intent() {
    run(Mode::ConcurrentReady)
}
#[test]
fn lost_ready_keeps_the_original_intent_and_blocks_another_mount() {
    run(Mode::LostReady)
}
#[test]
fn binding_record_sync_failure_stops_before_attach_and_blocks_replay() {
    run(Mode::BoundSync)
}
#[test]
fn replacing_the_sandbox_directory_never_redirects_its_mount_intent() {
    run(Mode::BoundSwap)
}
