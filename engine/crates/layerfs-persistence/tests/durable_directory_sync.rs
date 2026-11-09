//! A failed Store directory sync must not acknowledge a WAL transaction.
#![cfg(target_os = "linux")]
mod support;

use layerfs_content::{FinalizedObject, ObjectRole};
use layerfs_history::{HistoryCatalog, HistoryCatalogConfig};
use layerfs_persistence::{DurableBarrierError, Handles, PersistenceConfig, StorageProvider};
use layerfs_storage::{location::PackInfo, port::*, Storage, StoragePolicy};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    time::Duration,
};

fn authority() -> HistoryCatalogConfig {
    HistoryCatalogConfig {
        binding_key: b"durable-directory-sync".to_vec(),
        cursor_key: [73; 32],
        incarnation: 1,
    }
}
fn reserve() -> Reserve {
    Reserve {
        packs: 1,
        ordinals: 1,
    }
}
fn other_process_sees_writer_lock(directory: &Path) -> bool {
    let script = "import os,fcntl,sys\nf=os.open(sys.argv[1],os.O_RDONLY|os.O_DIRECTORY)\ntry:\n fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)\n sys.exit(1)\nexcept BlockingIOError:\n sys.exit(0)\n";
    Command::new("python3")
        .args(["-c", script])
        .arg(directory)
        .status()
        .unwrap()
        .success()
}
struct PanicPlan;
impl PackReadPlan for PanicPlan {
    fn select(&mut self, _: PackInfo, _: &[u8]) -> Result<PackReadChoice, PersistenceError> {
        panic!("injected consumer panic")
    }
}
fn panic_in_publication_lane(handles: &Handles) {
    let object = FinalizedObject::new(
        ObjectRole::FileState,
        layerfs_content::object::codec::encode_bytes_object(&vec![47; 32000]).unwrap(),
    )
    .unwrap();
    let storage = Storage::new(handles.storage.clone()).unwrap();
    let save = storage.begin_save().unwrap();
    save.accept(object.clone()).unwrap();
    save.finish().unwrap();
    drop(storage);
    let mut locations = Vec::new();
    handles
        .storage
        .locate(&[object.id()], &mut locations)
        .unwrap();
    let id = locations[0].pack.pack_id;
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _lane = handles.history.publish_lane().unwrap();
        let _ = handles.storage.read_pack_selection(id, &mut PanicPlan);
    }));
    assert!(outcome.is_err());
}

#[test]
fn failed_directory_sync_keeps_the_original_writer_and_never_acknowledges() {
    const CHILD: &str = "LAYERFS_STORE_DIRECTORY_SYNC_CHILD";
    if let Some(mode) = std::env::var_os(CHILD) {
        let directory = std::env::var_os("LAYERFS_TEST_DIR_SYNC_TARGET").unwrap();
        let directory = Path::new(&directory);
        let path = directory.join("store.sqlite");
        let handles = Handles::create(
            PersistenceConfig::sqlite(&path),
            StoragePolicy::frozen_default(),
            &authority(),
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        handles.storage.reserve(reserve()).unwrap();
        if mode == "seal-replace-dir" {
            let moved = directory.with_extension("moved");
            fs::rename(directory, &moved).unwrap();
            fs::create_dir(directory).unwrap();
            assert!(matches!(handles.seal(), Err(PersistenceError::Uncertain)));
            assert!(other_process_sees_writer_lock(&moved));
            let database = fs::canonicalize(moved.join("store.sqlite")).unwrap();
            assert!(fs::read_dir("/proc/self/fd").unwrap().any(|entry| {
                entry
                    .ok()
                    .and_then(|entry| fs::read_link(entry.path()).ok())
                    .is_some_and(|path| path == database)
            }));
            return;
        }
        if mode == "seal" {
            fs::write(directory.join("inject-directory"), b"1").unwrap();
            assert!(matches!(handles.seal(), Err(PersistenceError::Uncertain)));
            assert!(other_process_sees_writer_lock(directory));
            return;
        }
        if mode == "panic" {
            panic_in_publication_lane(&handles);
            assert!(matches!(
                handles.storage.reserve(reserve()),
                Err(PersistenceError::Uncertain)
            ));
            assert!(matches!(handles.seal(), Err(PersistenceError::Uncertain)));
            assert!(other_process_sees_writer_lock(directory));
            let database = fs::canonicalize(path).unwrap();
            assert!(fs::read_dir("/proc/self/fd").unwrap().any(|entry| {
                entry
                    .ok()
                    .and_then(|entry| fs::read_link(entry.path()).ok())
                    .is_some_and(|path| path == database)
            }));
            return;
        }
        let lane = if mode == "lane" {
            Some(handles.history.publish_lane().unwrap())
        } else {
            None
        };
        let (retained_path, lock_directory) = if mode == "privacy" {
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
            (path, directory.to_owned())
        } else if mode == "replace" {
            let retained = directory.join("retained.sqlite");
            fs::rename(&path, &retained).unwrap();
            (retained, directory.to_owned())
        } else if mode == "replace-dir" {
            let moved = directory.with_extension("moved");
            fs::rename(directory, &moved).unwrap();
            fs::create_dir(directory).unwrap();
            (moved.join("store.sqlite"), moved)
        } else {
            fs::write(directory.join("inject-directory"), b"1").unwrap();
            (path, directory.to_owned())
        };
        let outcome = if mode == "barrier-stop" {
            assert!(matches!(
                handles
                    .storage
                    .durable_barrier_cancellable(&|| directory.join("failed").exists()),
                Err(DurableBarrierError::Failed(PersistenceError::Uncertain))
            ));
            Err(PersistenceError::Uncertain)
        } else if mode == "barrier" || mode == "privacy" {
            handles.storage.durable_barrier()
        } else {
            handles.storage.reserve(reserve()).map(|_| ())
        };
        assert!(matches!(outcome, Err(PersistenceError::Uncertain)));
        drop(lane);
        assert!(matches!(
            handles.storage.reserve(reserve()),
            Err(PersistenceError::Uncertain)
        ));
        assert!(matches!(handles.seal(), Err(PersistenceError::Uncertain)));
        assert!(other_process_sees_writer_lock(&lock_directory));
        let database = fs::canonicalize(retained_path).unwrap();
        assert!(fs::read_dir("/proc/self/fd").unwrap().any(|entry| {
            entry
                .ok()
                .and_then(|entry| fs::read_link(entry.path()).ok())
                .is_some_and(|path| path == database)
        }));
        return;
    }

    let temp = support::Temp::new("durable-directory-sync");
    let shim = temp.join("sync-failure.so");
    assert!(Command::new("cc")
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/durable_dir_sync_failure.c"))
        .args(["-ldl", "-o"])
        .arg(&shim)
        .status()
        .unwrap()
        .success());
    for mode in [
        "ordinary",
        "lane",
        "barrier",
        "barrier-stop",
        "privacy",
        "seal",
        "replace",
        "replace-dir",
        "seal-replace-dir",
        "panic",
    ] {
        let directory = temp.join(mode);
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "failed_directory_sync_keeps_the_original_writer_and_never_acknowledges",
            ])
            .env(CHILD, mode)
            .env("LD_PRELOAD", &shim)
            .env(
                "LAYERFS_TEST_DIR_SYNC_TARGET",
                fs::canonicalize(&directory).unwrap(),
            )
            .env(
                "LAYERFS_TEST_DIR_SYNC_TRIGGER",
                directory.join("inject-directory"),
            )
            .env("LAYERFS_TEST_DIR_SYNC_RECEIPT", directory.join("failed"))
            .status()
            .unwrap();
        assert!(status.success(), "{mode} child: {status}");
        if mode == "replace" {
            assert!(directory.join("retained.sqlite").is_file());
            assert!(!directory.join("store.sqlite").exists());
            continue;
        }
        if mode == "replace-dir" || mode == "seal-replace-dir" {
            assert!(directory
                .with_extension("moved")
                .join("store.sqlite")
                .is_file());
            assert!(!directory.join("store.sqlite").exists());
            continue;
        }
        if mode == "privacy" {
            fs::set_permissions(
                directory.join("store.sqlite"),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        } else if mode != "panic" {
            assert!(!fs::read(directory.join("failed")).unwrap().is_empty());
        }
        let reopened = Handles::open_writable(
            PersistenceConfig::sqlite(directory.join("store.sqlite")),
            &authority().binding_key,
            authority().cursor_key,
        )
        .unwrap();
        reopened.storage.reserve(reserve()).unwrap();
    }
}

fn cancel_waiting_barrier(provider: Arc<StorageProvider>) {
    let stopped = Arc::new(AtomicBool::new(false));
    let thread_stopped = stopped.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result =
            provider.durable_barrier_cancellable(&|| thread_stopped.load(Ordering::Acquire));
        done_tx.send(result).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let premature = done_rx.recv_timeout(Duration::from_millis(100)).ok();
    stopped.store(true, Ordering::Release);
    let early = premature.is_some();
    let result = premature.unwrap_or_else(|| done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    waiter.join().unwrap();
    assert!(!early, "barrier escaped its held owner before cancellation");
    assert!(matches!(result, Err(DurableBarrierError::Cancelled)));
}

#[test]
fn barrier_cancels_before_attempt_while_waiting_for_directory_owner() {
    use nix::fcntl::{Flock, FlockArg};
    let temp = support::Temp::new("store-barrier-owner-stop");
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let handles = Handles::create(
        PersistenceConfig::sqlite(temp.join("store.sqlite")),
        StoragePolicy::frozen_default(),
        &authority(),
    )
    .unwrap();
    fs::set_permissions(temp.join("store.sqlite"), fs::Permissions::from_mode(0o600)).unwrap();
    let held = Flock::lock(
        fs::File::open(temp.path()).unwrap(),
        FlockArg::LockExclusiveNonblock,
    )
    .unwrap();
    cancel_waiting_barrier(handles.storage.clone());
    held.unlock().unwrap();
    handles.storage.durable_barrier().unwrap();
    handles.seal().unwrap();
}

#[test]
fn barrier_cancels_before_attempt_while_queued_for_a_publication_lane() {
    let temp = support::Temp::new("store-barrier-lane-stop");
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let handles = Handles::create(
        PersistenceConfig::sqlite(temp.join("store.sqlite")),
        StoragePolicy::frozen_default(),
        &authority(),
    )
    .unwrap();
    fs::set_permissions(temp.join("store.sqlite"), fs::Permissions::from_mode(0o600)).unwrap();
    std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let history = &handles.history;
        let owner = scope.spawn(move || {
            let lane = history.publish_lane().unwrap();
            ready_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            drop(lane);
        });
        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        cancel_waiting_barrier(handles.storage.clone());
        release_tx.send(()).unwrap();
        owner.join().unwrap();
    });
    handles.storage.durable_barrier().unwrap();
    handles.seal().unwrap();
}

#[test]
fn only_writable_durable_store_can_establish_a_barrier() {
    let temp = support::Temp::new("store-barrier-profile");
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let durable = PersistenceConfig::sqlite(temp.join("durable.sqlite"));
    let handles = Handles::create(
        durable.clone(),
        StoragePolicy::frozen_default(),
        &authority(),
    )
    .unwrap();
    assert!(!handles.storage.supports_durable_barrier());
    fs::set_permissions(&durable.path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(handles.storage.supports_durable_barrier());
    handles.storage.durable_barrier().unwrap();
    let reader =
        Handles::open_read_only(durable, &authority().binding_key, authority().cursor_key).unwrap();
    assert!(matches!(
        reader.storage.durable_barrier(),
        Err(PersistenceError::Malformed)
    ));
    drop(reader);
    handles.seal().unwrap();
    let disposable = PersistenceConfig::sqlite(temp.join("disposable.sqlite"))
        .with_sqlite_profile(layerfs_persistence::SqlitePersistenceProfile::Disposable);
    let handles =
        Handles::create(disposable, StoragePolicy::frozen_default(), &authority()).unwrap();
    assert!(matches!(
        handles.storage.durable_barrier(),
        Err(PersistenceError::Malformed)
    ));
    handles.seal().unwrap();
}
