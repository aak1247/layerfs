//! External tests of the built Node binding. No private Rust source is included.
#![cfg(target_os = "linux")]
mod support;
use layerfs_bridge::control::SdkValue;
use std::{path::Path, process::Command, time::Duration};
use support::{engine::*, *};

#[test]
fn sandbox_capacity_is_read_from_the_record_and_legacy_defaults_to_sixteen() {
    for (stored, expected) in [(None, 16), (Some(SdkValue::Int(64)), 64)] {
        let home = Home::new();
        home.record_with_capacity(CID, [1; 32], [2; 32], stored);
        let running = exited()
            .replace("\"Running\":false", "\"Running\":true")
            .replace(
                "\"Ports\":{}",
                r#""Ports":{"30421/tcp":[{"HostIp":"127.0.0.1","HostPort":"32123"}]}"#,
            );
        let engine = engine(&home.0, 200, &running);
        home.node(
            &format!("assert.equal((await native.call('project.list_sandboxes',args)).value.items[0].maxWorkspaces, {expected});"),
            None,
            Duration::from_secs(5),
        );
        engine.join().unwrap();
    }
}

#[test]
fn sandbox_create_refuses_invalid_capacity_before_any_side_effect() {
    let home = Home::new();
    home.node(
        "for (const value of [0,-1,4294967296,'2',null]) { const result = await native.call('sandbox.create',{...args,maxWorkspaces:value}); assert.equal(result.refused.code,'InvalidInput'); }",
        None,
        Duration::from_secs(5),
    );
    assert!(!home.0.join("proj/create.intent").exists());
}

#[test]
fn project_init_refuses_invalid_store_profile_before_creating_a_project() {
    let home = Home::new();
    home.node(
        "for (const value of ['unknown',0,null]) { const result = await native.call('project.init',{...args,name:'new-project',path:'/tmp',storeProfile:value}); assert.equal(result.refused.code,'InvalidInput'); }",
        None,
        Duration::from_secs(5),
    );
    assert!(!home.0.join("new-project").exists());
}

fn prepare_durable_project(home: &Home) {
    std::fs::create_dir(home.0.join("source")).unwrap();
    std::fs::write(home.0.join("source/file"), b"original source").unwrap();
    std::fs::create_dir(home.0.join("volume")).unwrap();
    std::fs::create_dir(home.0.join("volume/global")).unwrap();
}

fn record_fault_shim(home: &Home) -> std::path::PathBuf {
    let shim = home.0.join("record-fault.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/durable_record_fault.c"))
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    shim
}

#[test]
fn durable_project_initialization_publishes_its_private_record() {
    let home = Home::new();
    prepare_durable_project(&home);
    home.node(
        "const r=await native.call('project.init',{...args,name:'durable-project',path:process.env.LAYERFS_HOME+'/source',storePath:process.env.LAYERFS_HOME+'/volume/global/store.sqlite',storeProfile:'durable',timeoutMs:15000}); if (process.getuid()===0) assert.equal(r.value?.kind,'Initialized',JSON.stringify(r.refused)); else { assert.equal(r.refused?.code,'Failed',JSON.stringify(r.refused)); assert.match(r.refused.message,/must run as user 0/); }",
        None,
        Duration::from_secs(20),
    );
    assert!(home.0.join("durable-project/project.record").is_file());
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(home.0.join("durable-project/project.record"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[test]
fn durable_project_record_sync_failure_preserves_unknown_publication() {
    let home = Home::new();
    prepare_durable_project(&home);
    let shim = record_fault_shim(&home);
    home.node_with_env(
        "const r=await native.call('project.init',{...args,name:'durable-project',path:process.env.LAYERFS_HOME+'/source',storePath:process.env.LAYERFS_HOME+'/volume/global/store.sqlite',storeProfile:'durable',timeoutMs:15000}); assert.equal(r.refused?.code,'Unknown',JSON.stringify(r.refused));",
        Some(&shim), &[("LAYERFS_TEST_RECORD_MODE", "sync")], Duration::from_secs(20),
    );
    assert!(home.0.join("record-sync-failed").is_file());
    assert!(home.0.join("durable-project/project.record").is_file());
    assert!(!home.0.join("volume/global/store.sqlite").exists());
}

#[test]
fn durable_project_directory_swap_never_redirects_its_record() {
    let home = Home::new();
    prepare_durable_project(&home);
    let shim = record_fault_shim(&home);
    home.node_with_env(
        "const r=await native.call('project.init',{...args,name:'durable-project',path:process.env.LAYERFS_HOME+'/source',storePath:process.env.LAYERFS_HOME+'/volume/global/store.sqlite',storeProfile:'durable',timeoutMs:15000}); assert.equal(r.refused?.code,'Unknown',JSON.stringify(r.refused));",
        Some(&shim), &[("LAYERFS_TEST_RECORD_MODE", "swap")], Duration::from_secs(20),
    );
    assert!(home.0.join("record-parent-swapped").is_file());
    assert_eq!(
        std::fs::read_dir(home.0.join("durable-project"))
            .unwrap()
            .count(),
        0
    );
    assert!(std::fs::read_dir(home.0.join("original-project"))
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("project.")));
    assert!(!home.0.join("volume/global/store.sqlite").exists());
}

#[test]
fn invalid_recorded_capacity_is_not_treated_as_a_legacy_record() {
    let home = Home::new();
    home.record_with_capacity(CID, [1; 32], [2; 32], Some(SdkValue::Int(0)));
    let running = exited()
        .replace("\"Running\":false", "\"Running\":true")
        .replace(
            "\"Ports\":{}",
            r#""Ports":{"30421/tcp":[{"HostIp":"127.0.0.1","HostPort":"32123"}]}"#,
        );
    let engine = engine(&home.0, 200, &running);
    home.node(
        "assert.equal((await native.call('project.list_sandboxes',args)).refused.code,'InvalidInput');",
        None,
        Duration::from_secs(5),
    );
    engine.join().unwrap();
}

#[test]
fn a_gone_container_is_reclaimed_by_removing_its_records() {
    let home = Home::new();
    let engine = engine(
        &home.0,
        404,
        &format!(r#"{{"message":"No such container: {CID}"}}"#),
    );
    home.node(
        "assert.deepEqual(await reclaim(), {value:{kind:'ok',reclaimed:['1']}});",
        None,
        Duration::from_secs(5),
    );
    assert!(engine
        .join()
        .unwrap()
        .starts_with(&format!("GET /v1.54/containers/{CID}/json")));
    assert!(
        !home.0.join("proj/sandboxes/1").exists(),
        "its number is free again"
    );
}

#[test]
fn a_gone_durable_sandbox_keeps_its_records_for_recovery() {
    let home = Home::new();
    home.record_with_durability(
        CID,
        [1; 32],
        [2; 32],
        None,
        Some(SdkValue::Bytes(vec![8; 32])),
    );
    let engine = engine(
        &home.0,
        404,
        &format!(r#"{{"message":"No such container: {CID}"}}"#),
    );
    home.node(
        "assert.equal((await reclaim()).refused.code, 'RecoveryRequired');",
        None,
        Duration::from_secs(5),
    );
    assert!(engine
        .join()
        .unwrap()
        .starts_with(&format!("GET /v1.54/containers/{CID}/json")));
    assert!(home.0.join("proj/sandboxes/1/sandbox.record").exists());
}

#[test]
fn a_stopped_durable_sandbox_is_neither_reported_as_disposable_nor_deleted() {
    let home = Home::new();
    home.record_with_durability(
        CID,
        [1; 32],
        [2; 32],
        None,
        Some(SdkValue::Bytes(vec![8; 32])),
    );
    let engine = engines(&home.0, vec![(200, exited()), (200, exited())]);
    home.node(
        "assert.equal((await native.call('sandbox.list_workspaces', args)).refused.code, 'RecoveryRequired'); assert.equal((await reclaim()).refused.code, 'RecoveryRequired');",
        None,
        Duration::from_secs(5),
    );
    assert_eq!(engine.join().unwrap().len(), 2);
    assert!(home.0.join("proj/sandboxes/1/sandbox.record").exists());
}

#[test]
fn a_malformed_durable_marker_does_not_fall_back_to_reclamation() {
    let home = Home::new();
    home.record_with_durability(
        CID,
        [1; 32],
        [2; 32],
        None,
        Some(SdkValue::Bytes(vec![0; 32])),
    );
    let engine = engine(
        &home.0,
        404,
        &format!(r#"{{"message":"No such container: {CID}"}}"#),
    );
    home.node(
        "assert.equal((await reclaim()).refused.code, 'InvalidInput');",
        None,
        Duration::from_secs(5),
    );
    engine.join().unwrap();
    assert!(home.0.join("proj/sandboxes/1/sandbox.record").exists());
}

#[test]
fn another_404_is_a_failure_that_removes_nothing() {
    let home = Home::new();
    let engine = engine(&home.0, 404, r#"{"message":"page not found"}"#);
    home.node(
        "assert.equal((await reclaim()).refused.code, 'Failed');",
        None,
        Duration::from_secs(5),
    );
    engine.join().unwrap();
    assert!(home.0.join("proj/sandboxes/1/sandbox.record").exists());
}

#[test]
fn a_number_started_again_with_another_container_is_left_alone() {
    let home = Home::new();
    let record = home.0.join("proj/sandboxes/1/sandbox.record");
    let engine = engines_before(
        &home.0,
        vec![(404, format!(r#"{{"message":"No such container: {CID}"}}"#))],
        move || {
            // The public call read its original record before inspecting it.
            let mut bytes = std::fs::read(&record).unwrap();
            let offset = bytes
                .windows(64)
                .position(|window| window == CID.as_bytes())
                .unwrap();
            bytes[offset..offset + 64].fill(b'c');
            std::fs::write(record, bytes).unwrap();
        },
    );
    home.node(
        "assert.deepEqual(await reclaim(), {value:{kind:'ok',reclaimed:[]}});",
        None,
        Duration::from_secs(5),
    );
    engine.join().unwrap();
    let record = std::fs::read(home.0.join("proj/sandboxes/1/sandbox.record")).unwrap();
    assert!(record.windows(64).any(|window| window == [b'c'; 64]));
}

#[test]
fn a_stopped_container_is_refused_as_gone_then_reclaimed_by_deleting_it() {
    let home = Home::new();
    let engine = engines(
        &home.0,
        vec![(200, exited()), (200, exited()), (204, String::new())],
    );
    home.node(
        r#"
        const first = await native.call('sandbox.list_workspaces', args);
        assert.equal(first.refused.code, 'NoSuchSandbox');
        assert.equal(first.refused.detail.containerGone, true);
        assert.equal(first.refused.detail.containerStopped, true);
        assert.equal(first.refused.detail.container, 'b'.repeat(64));
        assert.deepEqual(await reclaim(), {value:{kind:'ok',reclaimed:['1']}});
    "#,
        None,
        Duration::from_secs(5),
    );
    let requests = engine.join().unwrap();
    assert!(requests[0].starts_with(&format!("GET /v1.54/containers/{CID}/json")));
    assert!(requests[1].starts_with(&format!("GET /v1.54/containers/{CID}/json")));
    assert!(requests[2].starts_with(&format!(
        "DELETE /v1.54/containers/{CID}?force=false&v=false"
    )));
    assert!(
        !home.0.join("proj/sandboxes/1").exists(),
        "its number is free again"
    );
}

#[test]
fn a_stopped_container_the_engine_will_not_delete_keeps_its_records() {
    let home = Home::new();
    let engine = engines(
        &home.0,
        vec![
            (200, exited()),
            (
                409,
                r#"{"message":"cannot remove container: container is running"}"#.into(),
            ),
        ],
    );
    home.node(
        "assert.equal((await reclaim()).refused.code, 'Failed');",
        None,
        Duration::from_secs(5),
    );
    assert_eq!(engine.join().unwrap().len(), 2);
    assert!(home.0.join("proj/sandboxes/1/sandbox.record").exists());
}

#[test]
fn a_dropped_guard_releases_the_lock_while_a_forked_child_holds_a_copy() {
    let home = Home::new();
    let preload = home.0.join("fork-lock.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Werror"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/fork_lock.c"))
        .args(["-ldl", "-o"])
        .arg(&preload)
        .status()
        .unwrap()
        .success());
    std::fs::remove_file(home.0.join("proj/sandboxes/1/sandbox.record")).unwrap();
    // sandbox.close takes the project guard, then refuses the absent Sandbox.
    let call =
        "assert.equal((await native.call('sandbox.close',args)).refused.code, 'NoSuchSandbox');";
    home.node(call, Some(&preload), Duration::from_secs(5));
    let child = std::fs::read_to_string(home.0.join("fork.pid")).unwrap();
    let lock = home.0.join("proj/ownership.lock");
    let copied = std::fs::read_dir(format!("/proc/{child}/fd"))
        .unwrap()
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .any(|path| path == lock);
    assert!(copied, "child {child} holds no copy of {}", lock.display());
    home.node(call, None, Duration::from_secs(5));
}
