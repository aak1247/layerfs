//! A real durable Store and authenticated control in a separate test process.
#![cfg(target_os = "linux")]

#[path = "../../layerfs-daemon/tests/support/installed_store.rs"]
#[allow(dead_code)]
mod support;

use layerfs_api::internal::{self, Control};
use layerfs_bridge::{
    control::{
        CaptureControl, CaptureIdentity, CapturePurpose, CommitControl, Reply, Request,
        UnmountControl, WorkspaceToken,
    },
    duplex,
};
use layerfs_content::filesystem::PathName;
use layerfs_daemon::{
    bootstrap::open_store,
    control::{CaptureSession, Service},
    store::{BindRequest, BoundWorkspace, StoreOperation},
    Owner,
};
use layerfs_history::WorkspaceId;
use layerfs_overlay::ProfileConfig;
use layerfs_storage::ReservationBlocks;
use layerfs_store_service::DurableWorkspace;
use layerfs_workspace::{Operation, Outcome, Position, SourceView, Time};
use std::{
    env, fs,
    net::{SocketAddr, TcpListener, TcpStream},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const CLIENT_KEY: [u8; 32] = [51; 32];
const SERVER_KEY: [u8; 32] = [52; 32];

struct TestChild(Child);
impl Drop for TestChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn token() -> WorkspaceToken {
    WorkspaceToken {
        workspace: WorkspaceId::from_authority([42; 32]).unwrap(),
        namespace: 1,
    }
}
fn capture() -> CaptureControl {
    CaptureControl {
        token: token(),
        identity: CaptureIdentity {
            id: [111; 32],
            scope_digest: [89; 32],
            purpose: CapturePurpose::ManagedWorkspace,
        },
    }
}
fn access<T>(
    operation: &StoreOperation,
    call: impl FnOnce(&StoreOperation, &SourceView) -> T,
) -> T {
    let overlay = operation.overlay();
    let route = operation.workspace().route();
    let source = overlay.acquire_base_source(route, 7).unwrap();
    let view = operation.workspace().view_for_source(source).unwrap();
    let result = call(operation, &view);
    drop(view);
    overlay.release_base_source(route, source).unwrap();
    result
}
fn write_bytes(operation: &StoreOperation, data: &[u8]) {
    access(operation, |operation, view| {
        let file = view
            .lookup(
                operation.overlay(),
                view.root_serial(),
                &PathName::new("file-000000").unwrap(),
            )
            .unwrap();
        let result = operation
            .workspace()
            .mutate(
                operation.overlay(),
                operation.ports(),
                view,
                Operation::Write {
                    serial: file.serial,
                    position: Position::At(0),
                    data: data.into(),
                },
                Time {
                    seconds: file.metadata.mtime_seconds,
                    nanoseconds: file.metadata.mtime_nanoseconds,
                },
            )
            .unwrap();
        let Outcome::Applied { publication, .. } = result else {
            panic!("write not applied")
        };
        operation.overlay().reply_attempted(publication).unwrap();
    });
}
fn original_bytes() -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..48 * 1024).map(|index| (index % 251) as u8).collect();
    bytes[0] = b'A';
    bytes
}
fn connect(address: SocketAddr) -> Control {
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let connection = duplex::initiate(
        stream,
        &CLIENT_KEY,
        duplex::public_key(&SERVER_KEY).unwrap(),
    )
    .unwrap();
    Control::new(connection)
}
fn accept(listener: &TcpListener) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match listener.accept() {
            Ok((stream, _)) => return stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "client did not connect");
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("accept failed: {error}"),
        }
    }
}

fn child(dir: &Path) {
    let fixture = support::Fixture::new_durable(2, "capture-process");
    let store = open_store(
        fixture.config.clone(),
        support::BINDING,
        support::CURSOR,
        16,
        2 * 1024 * 1024,
        ReservationBlocks::default(),
    )
    .unwrap();
    let initial = store
        .history()
        .branch_snapshot(fixture.branch)
        .unwrap()
        .unwrap();
    let durable_dir = fixture.directory.join("capture-process-durable");
    fs::create_dir(&durable_dir).unwrap();
    fs::set_permissions(&durable_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let path = durable_dir.join("workspace.sqlite");
    let owner = Owner::create_durable(DurableWorkspace {
        path: &path,
        profile: ProfileConfig::default(),
        identity: [42; 32],
        namespace: 1,
        expected_base_root: initial.effective_root.to_bytes(),
    })
    .unwrap();
    let workspace: BoundWorkspace = store
        .bind_durable(
            owner.client(),
            BindRequest {
                workspace: token().workspace,
                branch: fixture.branch,
            },
            initial,
        )
        .unwrap()
        .workspace;
    write_bytes(&workspace.operation().unwrap(), &original_bytes());
    let service = Service::new(store.clone(), &owner);
    assert_eq!(service.register_durable(workspace).unwrap(), token());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    fs::write(
        dir.join("ready.tmp"),
        listener.local_addr().unwrap().to_string(),
    )
    .unwrap();
    fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
    let mut advanced = false;
    for _ in 0..2 {
        let stream = accept(&listener);
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let mut channel = duplex::accept(
            stream,
            &SERVER_KEY,
            duplex::public_key(&CLIENT_KEY).unwrap(),
        )
        .unwrap();
        let mut session = CaptureSession::default();
        loop {
            let served = service
                .serve_one_control_session(&mut channel, &mut session)
                .unwrap();
            if !advanced && matches!(served.call.request, Request::Capture(_)) {
                assert!(served.outcome.is_ok());
                let operation = service.operation(token()).unwrap();
                write_bytes(&operation, b"B");
                drop(operation);
                assert!(matches!(
                    service
                        .execute_control(&Request::Commit(CommitControl {
                            token: token(),
                            force: false,
                            message: None,
                        }))
                        .unwrap()
                        .reply,
                    Reply::Committed(_)
                ));
                advanced = true;
            }
            if matches!(served.call.request, Request::EndSession) {
                break;
            }
        }
        drop(session);
        drop(channel);
    }
    assert!(advanced);
    drop(service);
    owner.stop().unwrap();
    drop(store);
    let sql = rusqlite::Connection::open(&fixture.config.path).unwrap();
    let count: i64 = sql
        .query_row("SELECT count(*) FROM checkpoint_receipt", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);
    drop(sql);
    fixture.cleanup();
    fs::write(dir.join("complete"), b"ok").unwrap();
}

#[test]
fn original_capture_read_handles_are_connection_local_across_processes() {
    if let Some(dir) = env::var_os("LAYERFS_CAPTURE_CHILD_DIR") {
        child(Path::new(&dir));
        return;
    }
    let dir: PathBuf =
        env::temp_dir().join(format!("layerfs-capture-process-{}", std::process::id()));
    fs::create_dir(&dir).unwrap();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
    let log = fs::File::create(dir.join("child.log")).unwrap();
    let mut process = TestChild(
        Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "original_capture_read_handles_are_connection_local_across_processes",
                "--nocapture",
            ])
            .env("LAYERFS_CAPTURE_CHILD_DIR", &dir)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !dir.join("ready").exists() {
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "child failed: {}",
            fs::read_to_string(dir.join("child.log")).unwrap()
        );
        assert!(Instant::now() < deadline, "child did not publish listener");
        thread::sleep(Duration::from_millis(10));
    }
    let address: SocketAddr = fs::read_to_string(dir.join("ready"))
        .unwrap()
        .parse()
        .unwrap();
    let mut first = connect(address);
    let proof = internal::workspace(&mut first)
        .capture_external(capture())
        .unwrap();
    let sha = proof.sha256().unwrap();
    assert_eq!(
        internal::workspace(&mut first)
            .observe_external_capture(capture())
            .unwrap(),
        layerfs_bridge::control::CaptureObservation::Published(Box::new(proof))
    );
    let first_handle = internal::workspace(&mut first)
        .open_captured(capture(), sha)
        .unwrap();
    assert_eq!(
        internal::workspace(&mut first)
            .read_captured(&first_handle, "file-000000", 0, 48 * 1024)
            .unwrap(),
        original_bytes()
    );
    assert!(internal::workspace(&mut first)
        .read_captured(&first_handle, "file-000000", 0, 48 * 1024 + 1)
        .is_err());
    assert!(internal::workspace(&mut first)
        .read_captured(&first_handle, "../file-000001", 0, 1)
        .is_err());
    assert_eq!(
        internal::workspace(&mut first)
            .read_captured(&first_handle, "file-000000", 0, 1)
            .unwrap(),
        b"A"
    );
    assert!(internal::workspace(&mut first)
        .open_captured(capture(), sha)
        .is_err());
    assert!(matches!(
        first.call(Request::EndSession).unwrap(),
        Reply::SessionEnded
    ));
    drop(first);

    let mut second = connect(address);
    let second_handle = internal::workspace(&mut second)
        .open_captured(capture(), sha)
        .unwrap();
    assert!(internal::workspace(&mut second)
        .read_captured(&first_handle, "file-000000", 0, 1)
        .is_err());
    assert_eq!(
        internal::workspace(&mut second)
            .read_captured(&second_handle, "file-000000", 0, 48 * 1024)
            .unwrap(),
        original_bytes()
    );
    internal::workspace(&mut second)
        .close_captured(&second_handle)
        .unwrap();
    assert!(internal::workspace(&mut second)
        .read_captured(&second_handle, "file-000000", 0, 1)
        .is_err());
    let third_handle = internal::workspace(&mut second)
        .open_captured(capture(), sha)
        .unwrap();
    assert!(matches!(
        second
            .call(Request::Unmount(UnmountControl {
                token: token(),
                discard: false
            }))
            .unwrap(),
        Reply::Unmounted(_)
    ));
    assert!(internal::workspace(&mut second)
        .read_captured(&third_handle, "file-000000", 0, 1)
        .is_err());
    assert!(matches!(
        second.call(Request::EndSession).unwrap(),
        Reply::SessionEnded
    ));
    drop(second);
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "child did not exit");
        thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.success(),
        "child failed: {}",
        fs::read_to_string(dir.join("child.log")).unwrap()
    );
    assert_eq!(fs::read(dir.join("complete")).unwrap(), b"ok");
    fs::remove_dir_all(dir).unwrap();
}
