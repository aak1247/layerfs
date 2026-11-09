//! Native durable Sandbox creation with a controlled Engine and daemon peer.
#![cfg(target_os = "linux")]
mod support;
use layerfs_bridge::{
    control::{Answer, Call, DaemonPhase, DaemonStatus, Reply, Request, SdkValue},
    daemon_setup::DaemonSetup,
    duplex,
    provision::StoreManifest,
};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    os::unix::{fs::DirBuilderExt, net::UnixListener},
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};
use support::{engine::exited, *};

fn load(path: &Path) -> SdkValue {
    match Answer::decode(&fs::read(path).unwrap()).unwrap().reply {
        Reply::Sdk(value) => value,
        _ => panic!("unexpected record type"),
    }
}
fn bytes<'a>(value: &'a SdkValue, name: &str) -> &'a [u8] {
    match value.get(name) {
        Some(SdkValue::Bytes(value)) => value,
        _ => panic!("missing {name}"),
    }
}
fn setup_project(home: &Home) {
    for name in ["source", "volume", "volume/global"] {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(home.0.join(name))
            .unwrap();
    }
    fs::write(home.0.join("source/file"), b"fixture").unwrap();
    home.node("const r=await native.call('project.init',{...args,name:'durable-project',path:process.env.LAYERFS_HOME+'/source',storePath:process.env.LAYERFS_HOME+'/volume/global/store.sqlite',storeProfile:'durable',timeoutMs:15000}); assert.equal(r.refused?.code,'Failed',JSON.stringify(r.refused)); assert.match(r.refused.message,/must run as user 0/);", None, Duration::from_secs(20));
    let record = home.0.join("durable-project/project.record");
    let mut value = load(&record);
    let mut manifest = StoreManifest::decode(bytes(&value, "manifest")).unwrap();
    manifest.daemon_sqlite = Some(manifest.host_sqlite.clone());
    if let SdkValue::Map(fields) = &mut value {
        fields
            .iter_mut()
            .find(|(name, _)| name == "installed")
            .unwrap()
            .1 = SdkValue::Bytes(manifest.encode().unwrap());
    }
    fs::write(
        record,
        Answer {
            id: 1,
            reply: Reply::Sdk(value),
        }
        .encode()
        .unwrap(),
    )
    .unwrap();
    fs::write(home.0.join("fake-daemon"), b"test-only daemon binary").unwrap();
}
fn fixed(status: u16, body: &[u8]) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 {status} test\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}
fn logs() -> Vec<u8> {
    let marker = b"LAYERFS_DAEMON_LISTEN 0.0.0.0:30421\n";
    let mut frame = vec![1, 0, 0, 0];
    frame.extend_from_slice(&(marker.len() as u32).to_be_bytes());
    frame.extend_from_slice(marker);
    let mut response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/vnd.docker.multiplexed-stream\r\nContent-Length: {}\r\n\r\n", frame.len()).into_bytes();
    response.extend_from_slice(&frame);
    response
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Success,
    StartFailure,
    SyncFailure,
}
fn engine(home: &Path, port: u16, mode: Mode) -> thread::JoinHandle<Vec<String>> {
    let listener = UnixListener::bind(home.join("docker.sock")).unwrap();
    listener.set_nonblocking(true).unwrap();
    let home = home.to_owned();
    thread::spawn(move || {
        let mut requests = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(15);
        while requests.len()
            < match mode {
                Mode::Success => 7,
                Mode::StartFailure => 4,
                Mode::SyncFailure => 2,
            }
        {
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(e) => panic!("Engine fixture: {e}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut header = String::new();
            let mut length = 0usize;
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if let Some(value) = line.strip_prefix("Content-Length: ") {
                    length = value.trim().parse().unwrap();
                }
                header.push_str(&line);
                if line == "\r\n" {
                    break;
                }
            }
            assert!(length < 1024 * 1024);
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            if header.starts_with("GET /version ") {
                stream
                    .write_all(&fixed(200, br#"{"ApiVersion":"1.54"}"#))
                    .unwrap();
                continue;
            }
            let intent = load(&home.join("durable-project/create.intent"));
            let (response, phase) = match requests.len() {
                0 => (fixed(200, br#"{"Name":"lfs-test-borrowed","Driver":"local","Scope":"local","Options":null}"#), "BeforeCreate"),
                1 => (fixed(201, format!(r#"{{"Id":"{CID}","Warnings":[]}}"#).as_bytes()), "BeforeCreate"),
                2 => (fixed(200, b""), "Created"),
                3 if mode == Mode::StartFailure => (fixed(500, br#"{"message":"test start failed"}"#), "BeforeStart"),
                3 => (fixed(204, b""), "BeforeStart"),
                4 => (logs(), "Started"),
                5 | 6 => {
                    let labels = r#""Labels":{"layerfs.role":"sandbox","layerfs.project":"durable-project","layerfs.sandbox":"1"},"User":"0:0""#;
                    let inspect = exited().replace("\"Running\":false", "\"Running\":true")
                        .replace("\"User\":\"0:0\"", labels)
                        .replace("\"Ports\":{}", &format!(r#""Ports":{{"30421/tcp":[{{"HostIp":"127.0.0.1","HostPort":"{port}"}}]}}"#));
                    (fixed(200, inspect.as_bytes()), if requests.len() == 5 { "Started" } else { "Authenticated" })
                }
                _ => unreachable!(),
            };
            assert_eq!(intent.get_text("phase"), Some(phase));
            if requests.len() >= 2 {
                assert_eq!(intent.get_text("container"), Some(CID));
            }
            let follows_logs = header.contains("/logs?");
            requests.push(header);
            stream.write_all(&response).unwrap();
            if follows_logs {
                let mut byte = [0];
                let _ = stream.read(&mut byte);
            }
        }
        requests
    })
}
fn daemon(listener: TcpListener, intent: PathBuf) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let record = load(&intent);
        let setup = DaemonSetup::decode(bytes(&record, "setup")).unwrap();
        let controller: [u8; 32] = bytes(&record, "controller").try_into().unwrap();
        let fresh = setup.fresh_workspace.as_ref().unwrap();
        assert!(fresh.create_directories);
        assert_eq!(fresh.sandbox.as_slice(), bytes(&record, "durableSandbox"));
        assert_eq!(setup.limits.namespaces, 32);
        let mut connection = duplex::accept(
            stream,
            &setup.private_key,
            duplex::public_key(&controller).unwrap(),
        )
        .unwrap();
        let hello = Call::decode(connection.receive.receive().unwrap()).unwrap();
        assert!(matches!(hello.request, Request::Hello(request) if request.wait_for_store));
        connection
            .send
            .send(
                &Answer {
                    id: hello.id,
                    reply: Reply::Hello(DaemonStatus {
                        instance: [9; 32],
                        phase: DaemonPhase::ControlReady,
                        overlay_sqlite: Some("3.50.4".into()),
                        store_sqlite: Some("3.50.4".into()),
                    }),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
    })
}
fn run(mode: Mode) {
    let failed = mode != Mode::Success;
    let home = Home::new();
    setup_project(&home);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let engine = engine(&home.0, listener.local_addr().unwrap().port(), mode);
    let daemon = (!failed).then(|| daemon(listener, home.0.join("durable-project/create.intent")));
    let action = if failed {
        "const r=await native.call('sandbox.create',a); assert.equal(r.refused?.code,'Unknown',JSON.stringify(r.refused)); const next=await native.call('sandbox.create',a); assert.equal(next.refused?.code,'RecoveryRequired',JSON.stringify(next.refused));"
    } else {
        "const r=await native.call('sandbox.create',a); assert.equal(r.value?.sandbox,'1',JSON.stringify(r.refused)); assert.equal(r.value?.name,'layerfs-durable-project-1');"
    };
    let setup = "const a={...args,project:'durable-project',maxWorkspaces:32,deploy:{docker:process.env.LAYERFS_HOME+'/docker.sock',image:'sha256:'+'a'.repeat(64),volume:'lfs-test-borrowed',daemon:process.env.LAYERFS_HOME+'/fake-daemon'}};";
    let script = format!("{setup} {action}");
    if mode == Mode::SyncFailure {
        let shim = home.0.join("create-fault.so");
        assert!(Command::new("cc")
            .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/sandbox_record_fault.c"))
            .args(["-ldl", "-o"])
            .arg(&shim)
            .status()
            .unwrap()
            .success());
        home.node(&script, Some(&shim), Duration::from_secs(20));
        assert!(home.0.join("create-sync-failed").exists());
    } else {
        home.node(&script, None, Duration::from_secs(20));
    }
    if failed {
        home.node(&format!("{setup} const r=await native.call('sandbox.create',a); assert.equal(r.refused?.code,'RecoveryRequired',JSON.stringify(r.refused));"), None, Duration::from_secs(20));
    }
    if let Some(daemon) = daemon {
        daemon.join().unwrap();
    }
    let requests = engine.join().unwrap();
    assert_eq!(
        requests.len(),
        match mode {
            Mode::Success => 7,
            Mode::StartFailure => 4,
            Mode::SyncFailure => 2,
        }
    );
    assert!(!requests
        .iter()
        .any(|request| request.starts_with("DELETE ") || request.contains("/stop?")));
    let project = home.0.join("durable-project");
    if failed {
        let intent = load(&project.join("create.intent"));
        assert_eq!(
            intent.get_text("phase"),
            Some(if mode == Mode::StartFailure {
                "BeforeStart"
            } else {
                "BeforeCreate"
            })
        );
        assert_eq!(
            intent.get_text("container"),
            if mode == Mode::StartFailure {
                Some(CID)
            } else {
                None
            }
        );
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(project.join("create.intent"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let setup = DaemonSetup::decode(bytes(&intent, "setup")).unwrap();
        assert!(setup.fresh_workspace.unwrap().create_directories);
        assert!(!project.join("sandboxes/1/sandbox.record").exists());
    } else {
        assert!(!project.join("create.intent").exists());
        let saved = load(&project.join("sandboxes/1/sandbox.record"));
        let receipt = load(&project.join("sandboxes/1/create.receipt"));
        assert_eq!(
            bytes(&saved, "durableSandbox"),
            bytes(&receipt, "durableSandbox")
        );
        assert_eq!(saved.get_int("maxWorkspaces"), Some(32));
        assert_eq!(saved.get_text("container"), Some(CID));
    }
}
#[test]
fn durable_creation_records_all_milestones_before_next_effect() {
    run(Mode::Success)
}
#[test]
fn uncertain_start_keeps_its_exact_container_and_blocks_replay() {
    run(Mode::StartFailure)
}
#[test]
fn failed_created_receipt_sync_stops_before_upload_and_blocks_replay() {
    run(Mode::SyncFailure)
}
