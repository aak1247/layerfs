//! Exact original Create observation never sends another lifecycle effect.
#[allow(dead_code)]
#[path = "support/engine.rs"]
mod engine;
use engine::{fixed, Peer};
use layerfs_sandbox::{backend::docker::SandboxRequest, ContainerId};
use std::fs::{self, File};

const CID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const IMAGE: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SECCOMP: &str = r#"seccomp={\"defaultAction\":\"SCMP_ACT_ALLOW\",\"syscalls\":[{\"names\":[\"unshare\",\"clone\"],\"action\":\"SCMP_ACT_ERRNO\",\"errnoRet\":1,\"args\":[{\"index\":0,\"value\":268435456,\"valueTwo\":268435456,\"op\":\"SCMP_CMP_MASKED_EQ\"}]},{\"names\":[\"clone3\"],\"action\":\"SCMP_ACT_ERRNO\",\"errnoRet\":38}]}"#;
fn request() -> SandboxRequest {
    SandboxRequest {
        image: IMAGE.into(),
        store_volume: "lfs-test-borrowed".into(),
        port: 30421,
        #[cfg(target_os = "linux")]
        host_export: None,
        labels: vec![("original.nonce".into(), "private-creation-scope".into())],
    }
}
fn volume() -> Vec<u8> {
    fixed(
        200,
        br#"{"Name":"lfs-test-borrowed","Driver":"local","Scope":"local","Options":null}"#,
    )
}
fn inspect() -> String {
    format!(
        r#"{{"Id":"{CID}","Image":"{IMAGE}","RestartCount":0,"State":{{"Status":"created","Running":false,"Paused":false,"Restarting":false,"OOMKilled":false,"Dead":false,"Pid":0,"ExitCode":0,"Error":"","StartedAt":"0001-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z"}},"Config":{{"Labels":{{"original.nonce":"private-creation-scope"}},"User":"0:0","Tty":false,"Entrypoint":["/usr/local/bin/layerfs-daemon"],"Cmd":["--config","/layerfs-local/config/daemon.setup"]}},"HostConfig":{{"Init":true,"Privileged":false,"CapAdd":["CAP_SYS_ADMIN"],"Devices":[{{"PathOnHost":"/dev/fuse","PathInContainer":"/dev/fuse","CgroupPermissions":"rwm"}}],"SecurityOpt":["no-new-privileges=true","apparmor=unconfined","{SECCOMP}"],"RestartPolicy":{{"Name":"no"}},"LogConfig":{{"Type":"json-file"}},"PortBindings":{{"30421/tcp":[{{"HostIp":"127.0.0.1","HostPort":""}}]}},"NetworkMode":"bridge","PidMode":"","IpcMode":"private","UsernsMode":"","UTSMode":"","AutoRemove":false,"PublishAllPorts":false,"ReadonlyRootfs":false,"Binds":null,"VolumesFrom":null,"ExtraHosts":null,"Links":null,"GroupAdd":null,"Tmpfs":null}},"Mounts":[{{"Type":"volume","Name":"lfs-test-borrowed","Destination":"/layerfs-store","RW":true,"Driver":"local"}}]}}"#
    )
}
fn exact() -> ContainerId {
    ContainerId::parse(CID).unwrap()
}

#[test]
fn declared_create_body_equals_the_single_original_wire_body() {
    let request = request();
    let declaration = request.create_body().unwrap();
    let (docker, peer) = Peer::new(
        vec![
            volume(),
            fixed(201, format!(r#"{{"Id":"{CID}"}}"#).as_bytes()),
        ],
        4096,
    );
    let owner = docker.create_sandbox(request).unwrap();
    assert_eq!(owner.identity(), exact());
    let calls = peer.finish();
    assert_eq!(calls.len(), 2);
    let create = &calls[1];
    assert!(create.starts_with(b"POST /v1.54/containers/create "));
    let header_end = create.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    assert_eq!(&create[header_end..], declaration);
    let mut invalid = owner.request().clone();
    invalid.image = "mutable:latest".into();
    assert!(invalid.create_body().is_err());
}

#[test]
fn observes_exact_created_owner_and_retains_unknown_upload_without_replay() {
    let (docker, peer) = Peer::new(
        vec![
            volume(),
            fixed(200, inspect().as_bytes()),
            fixed(500, b"{}"),
        ],
        4096,
    );
    let mut owner = docker.observe_created(exact(), request()).unwrap();
    assert_eq!(owner.identity(), exact());
    assert!(!owner.disposition().uploaded && !owner.disposition().started);
    let path = std::env::temp_dir().join(format!("sdk-created-upload-{}", std::process::id()));
    fs::write(&path, b"original executable").unwrap();
    let mut file = File::open(&path).unwrap();
    let first = owner.upload(&mut file, b"private config").unwrap_err();
    assert!(first.transfer.attempted);
    assert!(owner.upload(&mut file, b"private config").is_err());
    assert!(owner.start().is_err());
    fs::remove_file(path).unwrap();
    let calls = peer.finish();
    assert_eq!(calls.len(), 3);
    assert!(calls[0].starts_with(b"GET /v1.54/volumes/"));
    assert!(calls[1].starts_with(format!("GET /v1.54/containers/{CID}/json ").as_bytes()));
    assert!(calls[2].starts_with(format!("PUT /v1.54/containers/{CID}/archive?").as_bytes()));
    assert!(!calls
        .iter()
        .any(|v| v.starts_with(b"POST ") || v.starts_with(b"DELETE ")));
}

#[test]
fn rejects_changed_created_identity_state_scope_and_isolation_without_effects() {
    let original = inspect();
    for (from, to) in [
        (
            CID,
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        ),
        (
            IMAGE,
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        ),
        ("private-creation-scope", "different-creation-scope"),
        (r#""Status":"created""#, r#""Status":"exited""#),
        (r#""Pid":0"#, r#""Pid":321"#),
        (r#""Running":false"#, r#""Running":true"#),
        (r#""RestartCount":0"#, r#""RestartCount":1"#),
        (
            r#""StartedAt":"0001-01-01T00:00:00Z""#,
            r#""StartedAt":"2026-11-16T11:00:00Z""#,
        ),
        (r#""User":"0:0""#, r#""User":"1000:1000""#),
        (r#""Privileged":false"#, r#""Privileged":true"#),
        (r#""HostIp":"127.0.0.1""#, r#""HostIp":"0.0.0.0""#),
        (r#""HostPort":"""#, r#""HostPort":"9090""#),
        (r#""NetworkMode":"bridge""#, r#""NetworkMode":"host""#),
        (r#""PidMode":"""#, r#""PidMode":"host""#),
        (r#""IpcMode":"private""#, r#""IpcMode":"host""#),
        (r#""UsernsMode":"""#, r#""UsernsMode":"host""#),
        (r#""Binds":null"#, r#""Binds":["/:/host:rw"]"#),
        (
            r#""Name":"lfs-test-borrowed""#,
            r#""Name":"different-store""#,
        ),
    ] {
        assert!(original.contains(from), "{from}");
        let changed = original.replacen(from, to, 1);
        let (docker, peer) = Peer::new(vec![volume(), fixed(200, changed.as_bytes())], 4096);
        assert!(
            docker.observe_created(exact(), request()).is_err(),
            "{from}"
        );
        assert!(peer.finish().iter().all(|v| v.starts_with(b"GET ")));
    }
}

#[test]
fn missing_or_nonlocal_created_result_never_authorizes_another_create() {
    let (docker, peer) = Peer::new(
        vec![volume(), fixed(404, br#"{"message":"No such container"}"#)],
        4096,
    );
    assert!(docker.observe_created(exact(), request()).is_err());
    assert!(peer.finish().iter().all(|v| v.starts_with(b"GET ")));
    let altered = br#"{"Name":"lfs-test-borrowed","Driver":"nfs","Scope":"local","Options":null}"#;
    let (docker, peer) = Peer::new(vec![fixed(200, altered)], 4096);
    assert!(docker.observe_created(exact(), request()).is_err());
    assert_eq!(peer.finish().len(), 1);
}
