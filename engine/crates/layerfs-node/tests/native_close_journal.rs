//! Durable close custody through the actual Node binding and authenticated channel.
#![cfg(target_os = "linux")]
mod support;
use layerfs_bridge::{
    control::{
        Activity, Answer, Call, ControlCode, ControlRefusal, DaemonPhase, DaemonStatus,
        DirtyReport, MountPhase, MountReceipt, MountStatus, ReadyMount, Reply, Request, SdkValue,
        WorkspaceStatus, WorkspaceToken,
    },
    duplex::{accept, generate_keypair},
    provision::StoreManifest,
};
use layerfs_history::{BranchSnapshot, WorkspaceId};
use layerfs_persistence::SqlitePersistenceProfile;
use layerfs_store_service::StoreService;
use std::{
    fs,
    net::TcpListener,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::Path,
    process::Command,
    thread,
    time::Duration,
};
use support::{engine::*, *};

fn write_value(path: &Path, value: SdkValue) {
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(
        &Answer {
            id: 1,
            reply: Reply::Sdk(value),
        }
        .encode()
        .unwrap(),
    )
    .unwrap();
}
fn read_value(path: &Path) -> SdkValue {
    match Answer::decode(&fs::read(path).unwrap()).unwrap().reply {
        Reply::Sdk(value) => value,
        other => panic!("unexpected value {other:?}"),
    }
}
fn binding(home: &Home) -> BranchSnapshot {
    for name in ["source", "volume", "volume/global"] {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(home.0.join(name))
            .unwrap();
    }
    fs::write(home.0.join("source/file"), b"fixture").unwrap();
    home.node("const r=await native.call('project.init',{...args,name:'fixture',path:process.env.LAYERFS_HOME+'/source',storePath:process.env.LAYERFS_HOME+'/volume/global/store.sqlite',storeProfile:'durable',timeoutMs:15000}); assert.equal(r.refused?.code,'Failed',JSON.stringify(r.refused)); assert.match(r.refused.message,/must run as user 0/);", None, Duration::from_secs(20));
    let project = home.0.join("fixture");
    let value = read_value(&project.join("project.record"));
    let Some(SdkValue::Bytes(bytes)) = value.get("manifest") else {
        panic!("missing manifest")
    };
    let manifest = StoreManifest::decode(bytes).unwrap();
    let store = home.0.join("volume/global/store.sqlite");
    fs::copy(project.join("sealed.sqlite"), &store).unwrap();
    let service = StoreService::open_direct_with_profile(
        &store,
        &manifest.binding,
        manifest.cursor_key,
        &home.0.join("template.sqlite"),
        SqlitePersistenceProfile::Durable,
    )
    .unwrap();
    let stack = layerfs_store_service::history::lookup::project(&service.store, "fixture").unwrap();
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
fn intents(directory: &Path) -> Vec<SdkValue> {
    let mut paths = directory
        .read_dir()
        .unwrap()
        .map(|v| v.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.starts_with("close-intent-") && name.ends_with(".record")
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths.iter().map(|path| read_value(path)).collect()
}
fn answer(connection: &mut layerfs_bridge::duplex::Connection, call: Call, reply: Reply) {
    connection
        .send
        .send(&Answer { id: call.id, reply }.encode().unwrap())
        .unwrap();
}
fn next(connection: &mut layerfs_bridge::duplex::Connection) -> Call {
    Call::decode(connection.receive.receive().unwrap()).unwrap()
}
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Closed,
    Lost,
    IntentSync,
    ReplySync,
    RemoveSync,
    CompleteSync,
    Dirty,
    DirtyMissing,
    Busy,
    AmbiguousBusy,
    Force,
}

fn run(mode: Mode) {
    let home = Home::new();
    let binding = binding(&home);
    let daemon = generate_keypair().unwrap();
    let client = generate_keypair().unwrap();
    home.record_with_durability(
        CID,
        client.private,
        daemon.public,
        None,
        Some(SdkValue::Bytes(vec![8; 32])),
    );
    let directory = home.0.join("proj/sandboxes/1");
    let token = WorkspaceToken {
        workspace: WorkspaceId::from_authority([3; 32]).unwrap(),
        namespace: 7,
    };
    let file = directory.join(format!(
        "workspace-{}.record",
        layerfs_bridge::sdk_values::hex(token.workspace.as_slice())
    ));
    write_value(
        &file,
        SdkValue::map([
            ("path", SdkValue::text("/workspaces/fixture")),
            ("branch", SdkValue::text("main")),
            (
                "workspace",
                SdkValue::Bytes(token.workspace.to_bytes().to_vec()),
            ),
            ("namespace", SdkValue::Int(token.namespace)),
        ]),
    );
    let original_record = fs::read(&file).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let running = exited()
        .replace("\"Running\":false", "\"Running\":true")
        .replace(
            "\"Ports\":{}",
            &format!(r#""Ports":{{"30421/tcp":[{{"HostIp":"127.0.0.1","HostPort":"{port}"}}]}}"#),
        );
    let engine = engines(&home.0, vec![(200, running); 2]);
    let observed = directory.clone();
    let worker = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut connection = accept(stream, &daemon.private, client.public).unwrap();
        let hello = next(&mut connection);
        assert!(matches!(hello.request, Request::Hello(_)));
        answer(
            &mut connection,
            hello,
            Reply::Hello(DaemonStatus {
                instance: [7; 32],
                phase: DaemonPhase::ControlReady,
                overlay_sqlite: Some("3.50.4".into()),
                store_sqlite: Some("3.50.4".into()),
            }),
        );
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
        let status = || {
            Reply::Status(Box::new(WorkspaceStatus {
                token,
                binding: binding.clone(),
                activity: Activity::Idle,
                epoch: 1,
                epoch_saturated: false,
                published: None,
                local: None,
                local_failure: Some(ControlRefusal {
                    code: ControlCode::Failed,
                    phase: "test local observation".into(),
                    moved: None,
                    published: None,
                    detail: "not needed by this fixture".into(),
                }),
                kernel: Some(MountStatus {
                    phase: MountPhase::Ready,
                    ready: Some(ready.clone()),
                    detached: false,
                    work: None,
                }),
            }))
        };
        for _ in 0..2 {
            let call = next(&mut connection);
            assert_eq!(call.request, Request::Status(token));
            answer(&mut connection, call, status());
        }
        if mode == Mode::Force {
            let call = next(&mut connection);
            assert_eq!(call.request, Request::Status(token));
            assert!(intents(&observed).is_empty());
            answer(&mut connection, call, status());
        }
        if mode == Mode::IntentSync {
            assert!(
                connection.receive.receive().is_err(),
                "unconfirmed intent must not send Unmount"
            );
            return;
        }
        let first = next(&mut connection);
        assert!(matches!(first.request, Request::Unmount(request) if request.token == token));
        assert_eq!(intents(&observed).len(), 1);
        assert_eq!(
            intents(&observed)[0].get_text("phase"),
            Some("beforeUnmount")
        );
        if mode == Mode::Lost {
            return;
        }
        let reply = match mode {
            Mode::Dirty | Mode::DirtyMissing => Reply::Dirty(Box::new(DirtyReport {
                token,
                paths: vec!["changed".into()],
                inodes: 1,
                entries: 0,
                pending_merge: false,
                truncated: false,
            })),
            Mode::Busy => Reply::Refused(ControlRefusal {
                code: ControlCode::Busy,
                phase: "unmount:kernel".into(),
                moved: None,
                published: None,
                detail: "kernel retained mount".into(),
            }),
            Mode::AmbiguousBusy => Reply::Refused(ControlRefusal {
                code: ControlCode::Busy,
                phase: "another busy phase".into(),
                moved: None,
                published: None,
                detail: "no terminal outcome provided".into(),
            }),
            _ => Reply::Unmounted(token),
        };
        answer(&mut connection, first, reply);
        if mode == Mode::DirtyMissing {
            return;
        }
        if matches!(mode, Mode::Dirty | Mode::Busy) {
            let call = next(&mut connection);
            assert_eq!(call.request, Request::Status(token));
            assert_eq!(intents(&observed)[0].get_text("phase"), Some("noEffect"));
            answer(&mut connection, call, status());
            let second = next(&mut connection);
            assert!(
                matches!(second.request, Request::Unmount(request) if request.token == token && request.discard)
            );
            assert_eq!(intents(&observed).len(), 2);
            answer(&mut connection, second, Reply::Unmounted(token));
        }
    });
    let script = match mode {
        Mode::Lost | Mode::IntentSync | Mode::ReplySync | Mode::RemoveSync | Mode::CompleteSync | Mode::AmbiguousBusy => "const a={...args,path:'/workspaces/fixture',discard:true}; const r=await native.call('workspace.unmount',a); assert.equal(r.refused?.code,'Unknown',JSON.stringify(r.refused)); const again=await native.call('workspace.unmount',a); assert.equal(again.refused?.code,'RecoveryRequired',JSON.stringify(again.refused));",
        Mode::Dirty => "const a={...args,path:'/workspaces/fixture'}; const r=await native.call('workspace.unmount',a); assert.equal(r.refused?.code,'Dirty',JSON.stringify(r.refused)); const next=await native.call('workspace.unmount',{...a,discard:true}); assert.equal(next.value?.kind,'Unmounted',JSON.stringify(next.refused));",
        Mode::DirtyMissing => "const r=await native.call('workspace.unmount',{...args,path:'/workspaces/fixture'}); assert.equal(r.refused?.code,'Dirty',JSON.stringify(r.refused));",
        Mode::Busy => "const a={...args,path:'/workspaces/fixture',discard:true}; const r=await native.call('workspace.unmount',a); assert.equal(r.refused?.code,'Busy',JSON.stringify(r.refused)); const next=await native.call('workspace.unmount',a); assert.equal(next.value?.kind,'Unmounted',JSON.stringify(next.refused));",
        Mode::Force => "const a={...args,path:'/workspaces/fixture',discard:true}; const r=await native.call('workspace.unmount',{...a,force:true}); assert.equal(r.refused?.code,'Unsupported',JSON.stringify(r.refused)); const next=await native.call('workspace.unmount',a); assert.equal(next.value?.kind,'Unmounted',JSON.stringify(next.refused));",
        Mode::Closed => "const a={...args,path:'/workspaces/fixture',discard:true}; const r=await native.call('workspace.unmount',a); assert.equal(r.value?.kind,'Unmounted',JSON.stringify(r.refused));",
    };
    if matches!(
        mode,
        Mode::IntentSync | Mode::ReplySync | Mode::RemoveSync | Mode::CompleteSync
    ) {
        let shim = home.0.join("close-fault.so");
        assert!(Command::new("cc")
            .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/close_record_fault.c"))
            .args(["-ldl", "-o"])
            .arg(&shim)
            .status()
            .unwrap()
            .success());
        home.node_with_env(
            script,
            Some(&shim),
            &[(
                "LAYERFS_TEST_CLOSE_FAULT",
                match mode {
                    Mode::IntentSync => "intent",
                    Mode::ReplySync => "answer",
                    Mode::RemoveSync => "remove",
                    Mode::CompleteSync => "complete",
                    _ => unreachable!(),
                },
            )],
            Duration::from_secs(20),
        );
    } else {
        home.node(script, None, Duration::from_secs(20));
    }
    worker.join().unwrap();
    assert_eq!(engine.join().unwrap().len(), 2);
    let saved = intents(&directory);
    if matches!(
        mode,
        Mode::Lost
            | Mode::IntentSync
            | Mode::ReplySync
            | Mode::RemoveSync
            | Mode::CompleteSync
            | Mode::AmbiguousBusy
    ) {
        assert_eq!(saved.len(), 1);
        let removed = matches!(mode, Mode::RemoveSync | Mode::CompleteSync);
        assert_eq!(
            saved[0].get_text("phase"),
            Some(if removed { "closed" } else { "beforeUnmount" })
        );
        assert_eq!(file.exists(), !removed);
        home.node("const r=await native.call('workspace.unmount',{...args,path:'/workspaces/fixture',discard:true}); assert.equal(r.refused?.code,'RecoveryRequired',JSON.stringify(r.refused));", None, Duration::from_secs(20));
    } else if mode == Mode::DirtyMissing {
        assert_eq!(saved[0].get_text("phase"), Some("noEffect"));
        fs::remove_file(&file).unwrap();
        home.node("const r=await native.call('workspace.unmount',{...args,path:'/workspaces/fixture',discard:true}); assert.equal(r.refused?.code,'RecoveryRequired',JSON.stringify(r.refused));", None, Duration::from_secs(20));
    } else {
        assert!(!file.exists());
        assert!(saved
            .iter()
            .any(|value| value.get_text("phase") == Some("complete")));
        if matches!(mode, Mode::Dirty | Mode::Busy) {
            assert!(saved
                .iter()
                .any(|value| value.get_text("phase") == Some("noEffect")));
        }
        // A terminal receipt and an unexpectedly reappearing Workspace record
        // cannot be mistaken for a fresh, safely mounted Workspace.
        fs::write(&file, original_record).unwrap();
        home.node("const r=await native.call('workspace.unmount',{...args,path:'/workspaces/fixture',discard:true}); assert.equal(r.refused?.code,'RecoveryRequired',JSON.stringify(r.refused));", None, Duration::from_secs(20));
    }
}
#[test]
fn close_records_the_terminal_answer_before_removing_the_workspace() {
    run(Mode::Closed)
}
#[test]
fn lost_close_reply_blocks_same_process_and_restart() {
    run(Mode::Lost)
}
#[test]
fn failed_intent_sync_never_sends_close() {
    run(Mode::IntentSync)
}
#[test]
fn failed_reply_sync_retains_the_answer_and_blocks_replay() {
    run(Mode::ReplySync)
}
#[test]
fn failed_workspace_removal_sync_retains_the_close_receipt() {
    run(Mode::RemoveSync)
}
#[test]
fn failed_completion_sync_never_replays_the_acknowledged_close() {
    run(Mode::CompleteSync)
}
#[test]
fn dirty_reply_allows_a_new_explicit_close() {
    run(Mode::Dirty)
}
#[test]
fn refused_close_with_a_missing_workspace_record_requires_recovery() {
    run(Mode::DirtyMissing)
}
#[test]
fn busy_reply_allows_a_new_explicit_close() {
    run(Mode::Busy)
}
#[test]
fn busy_without_the_exact_reversible_boundary_does_not_authorize_a_retry() {
    run(Mode::AmbiguousBusy)
}
#[test]
fn forced_close_is_refused_before_the_recorded_protocol() {
    run(Mode::Force)
}
