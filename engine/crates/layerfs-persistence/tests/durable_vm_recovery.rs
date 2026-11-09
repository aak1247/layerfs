//! Guest-side probe for the explicit VM power-loss test in tools/.
#![cfg(target_os = "linux")]

use layerfs_content::{FinalizedObject, ObjectRole};
use layerfs_history::HistoryCatalogConfig;
use layerfs_persistence::{Handles, PersistenceConfig, SqlitePersistenceProfile};
use layerfs_storage::{Storage, StoragePolicy};
use std::{fs, path::Path, thread, time::Duration};

#[test]
#[ignore = "run only inside the owned VM power-loss harness"]
fn durable_save_survives_vm_power_loss() {
    let root = std::env::var("LAYERFS_DURABLE_VM_ROOT").expect("guest Store directory");
    let mode = std::env::var("LAYERFS_DURABLE_VM_MODE").expect("guest probe mode");
    let root = Path::new(&root);
    let config = PersistenceConfig::sqlite(root.join("store.sqlite"))
        .with_sqlite_profile(SqlitePersistenceProfile::Durable);
    let authority = HistoryCatalogConfig {
        binding_key: b"durable-vm-power-loss".to_vec(),
        cursor_key: [85; 32],
        incarnation: 1,
    };
    let object = FinalizedObject::new(
        ObjectRole::FileState,
        layerfs_content::object::codec::encode_bytes_object(b"durable VM recovery object").unwrap(),
    )
    .unwrap();
    match mode.as_str() {
        "write" => {
            fs::create_dir(root).unwrap();
            fs::File::open(root.parent().unwrap())
                .unwrap()
                .sync_all()
                .unwrap();
            let handles =
                Handles::create(config, StoragePolicy::frozen_default(), &authority).unwrap();
            // The test is for an acknowledged Store save, not directory creation.
            fs::File::open(root).unwrap().sync_all().unwrap();
            let storage = Storage::new(handles.storage.clone()).unwrap();
            let save = storage.begin_save().unwrap();
            save.accept(object).unwrap();
            save.finish().unwrap();
            let wal_size = fs::metadata(root.join("store.sqlite-wal")).unwrap().len();
            assert!(wal_size > 0, "the live Store must still have a WAL");
            eprintln!("LAYERFS_VM_ACK wal_bytes={wal_size}");
            // The host must kill the VM before a clean Store close/checkpoint.
            thread::sleep(Duration::from_secs(60));
            panic!("the host did not terminate the guest after acknowledgement");
        }
        "read" => {
            let handles =
                Handles::open_writable(config, &authority.binding_key, authority.cursor_key)
                    .unwrap();
            let storage = Storage::new(handles.storage.clone()).unwrap();
            assert_eq!(
                storage
                    .reader()
                    .unwrap()
                    .read_objects(&[object.id()])
                    .unwrap(),
                vec![object.canonical().to_vec()]
            );
            eprintln!("LAYERFS_VM_RECOVERED");
            thread::sleep(Duration::from_secs(60));
            panic!("the host did not terminate the guest after verification");
        }
        _ => panic!("unknown guest probe mode"),
    }
}
