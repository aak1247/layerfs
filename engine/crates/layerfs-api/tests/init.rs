//! SDK Init closes the Store and returns exact original outcomes.
use layerfs_api::{InitError, InitRequest, ProjectApi};
use layerfs_bridge::provision::{ProviderKind, StoreProfile};
use layerfs_content::{
    file::FileView,
    filesystem::{FilesystemRead, FilesystemRootId, LogicalPath},
};
use layerfs_history::{BranchId, HistoryCatalog, HistoryCatalogConfig, HistoryName, LayerStackId};
use layerfs_persistence::{Handles, PersistenceConfig, SqlitePersistenceProfile};
use layerfs_storage::StoragePolicy;
use layerfs_telemetry::timer::Timing;
use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

fn request(directory: &Path) -> InitRequest {
    InitRequest {
        source: directory.join("source"),
        store: PersistenceConfig::sqlite(directory.join("sealed.sqlite"))
            .with_sqlite_profile(SqlitePersistenceProfile::Disposable),
        locator: "/store/store.sqlite".into(),
        policy: StoragePolicy::frozen_default(),
        catalog: HistoryCatalogConfig {
            binding_key: b"sdk-init-proof".to_vec(),
            cursor_key: [29; 32],
            incarnation: 1,
        },
        stack: LayerStackId::from_authority([21; 16]),
        stack_name: HistoryName::new("project").unwrap(),
        branch: BranchId::from_authority([22; 16]),
        branch_name: HistoryName::new("main").unwrap(),
        scope_seed: [23; 32],
        deadline: Instant::now() + Duration::from_secs(5),
    }
}

#[test]
fn init_seals_complete_root_and_first_branch_without_retained_handles() {
    let directory = std::env::temp_dir().join(format!("layerfs-sdk-init-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    fs::create_dir(directory.join("source")).unwrap();
    fs::create_dir(directory.join("source/.git")).unwrap();
    let bytes = vec![0xa7; 70_000];
    fs::write(directory.join("source/file"), &bytes).unwrap();
    fs::hard_link(
        directory.join("source/file"),
        directory.join("source/alias"),
    )
    .unwrap();
    fs::write(directory.join("source/.git/index"), b"complete index").unwrap();
    std::os::unix::fs::symlink("../file", directory.join("source/.git/link")).unwrap();
    let config = request(&directory).store;
    let project = Timing::disabled("sdk.init", |timer| {
        ProjectApi::new().init(request(&directory), timer)
    })
    .0
    .unwrap();
    assert_eq!(project.initialized.entries, 6);
    assert_eq!(project.manifest.provider, ProviderKind::Sqlite);
    assert_eq!(project.manifest.profile, StoreProfile::Disposable);
    assert_eq!(project.manifest.locator, "/store/store.sqlite");
    assert_eq!(
        project.manifest.bytes,
        fs::metadata(&project.store.path).unwrap().len()
    );
    assert_eq!(project.manifest.host_sqlite, project.store.sqlite_version);
    assert_eq!(project.manifest.daemon_sqlite, None);
    project.manifest.check().unwrap();
    assert_eq!(project.branch.effective_root, project.initialized.root);
    assert_eq!(project.manifest.root, project.initialized.root.to_bytes());
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(!directory.join(format!("sealed.sqlite{suffix}")).exists());
    }
    // The SDK owns no handle. A separate oracle opens once, writer first for
    // Apple SQLite's sidecar-free WAL contract; no provider fallback occurs.
    let handles = Handles::open_writable(
        config,
        &project.manifest.binding,
        project.manifest.cursor_key,
    )
    .unwrap();
    assert_eq!(handles.profile().journal_mode, "wal");
    assert_eq!(handles.profile().synchronous, 0);
    assert_eq!(
        handles
            .history
            .branch_snapshot(project.branch.branch.id)
            .unwrap(),
        Some(project.branch.clone())
    );
    // Init recorded what it imported, once; `file` and `alias` are one identity.
    let totals = handles.history.statistics().unwrap();
    let init = totals.init.expect("Init totals");
    assert_eq!((init.entries, init.files, init.bytes), (6, 2, 70_014));
    assert!(init.stored > 0 && init.stored <= totals.stored);
    assert!(init.canonical > 0 && init.canonical <= totals.canonical);
    assert!(handles.history.record_init(&init).is_err());
    let storage = layerfs_storage::Storage::new(handles.storage.clone()).unwrap();
    {
        let reader = storage.reader().unwrap();
        let mut tree =
            FilesystemRead::new(&reader, FilesystemRootId(project.initialized.root)).unwrap();
        let file = tree.resolve(&LogicalPath::new("file").unwrap()).unwrap();
        assert_eq!(
            file,
            tree.resolve(&LogicalPath::new("alias").unwrap()).unwrap()
        );
        for (name, expected) in [
            ("file", bytes.as_slice()),
            ("alias", bytes.as_slice()),
            (".git/index", b"complete index"),
        ] {
            let item = tree.resolve(&LogicalPath::new(name).unwrap()).unwrap();
            Timing::disabled("oracle", |timer| {
                let view =
                    FileView::open(&reader, item.value.content_root, timer.child("open")).unwrap();
                let mut actual = Vec::new();
                view.read_range(&reader, 0..view.logical_len(), &mut actual, timer)
                    .unwrap();
                assert_eq!(actual, expected);
                Ok::<(), layerfs_content::ContentError>(())
            })
            .0
            .unwrap();
        }
        assert_eq!(
            tree.readlink(&LogicalPath::new(".git/link").unwrap())
                .unwrap()
                .as_bytes(),
            b"../file"
        );
        println!("SDK_INIT entries=6 full_file_oracle_bytes=140014 host_sqlite={} store_bytes={} profile=Disposable",project.manifest.host_sqlite,project.manifest.bytes);
    }
    drop(storage);
    handles.seal().unwrap();
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn refusal_preserves_existing_output_and_original_request() {
    let directory =
        std::env::temp_dir().join(format!("layerfs-sdk-refusal-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("sealed.sqlite"), b"original").unwrap();
    let failure = Timing::disabled("sdk.init", |timer| {
        ProjectApi::new().init(request(&directory), timer)
    })
    .0
    .unwrap_err();
    assert!(matches!(failure.error, InitError::Persistence(_)));
    assert!(failure.initialized.is_none() && failure.branch.is_none());
    assert_eq!(fs::read(&failure.request.store.path).unwrap(), b"original");
    let mut invalid = request(&directory);
    invalid.locator = "relative".into();
    let failure = Timing::disabled("sdk.init", |timer| ProjectApi::new().init(invalid, timer))
        .0
        .unwrap_err();
    assert!(matches!(
        failure.error,
        InitError::Manifest("Store locator")
    ));
    assert_eq!(
        fs::read(directory.join("sealed.sqlite")).unwrap(),
        b"original"
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn git_substitution_imports_a_wide_complete_tree_and_seals_the_selected_metadata() {
    let directory =
        std::env::temp_dir().join(format!("layerfs-sdk-git-wide-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    fs::create_dir(directory.join("source")).unwrap();
    fs::create_dir(directory.join("git-stage")).unwrap();
    fs::write(
        directory.join("source/.git"),
        b"gitdir: /outside/never-imported\n",
    )
    .unwrap();
    fs::write(directory.join("source/ignored-file"), b"ignored data").unwrap();
    fs::hard_link(
        directory.join("source/ignored-file"),
        directory.join("source/alias"),
    )
    .unwrap();
    std::os::unix::fs::symlink("ignored-file", directory.join("source/link")).unwrap();
    for n in 0..530 {
        fs::write(directory.join(format!("source/file-{n:04}")), b"data").unwrap();
    }
    fs::write(directory.join("git-stage/HEAD"), b"ref: refs/heads/main\n").unwrap();
    fs::write(directory.join("git-stage/index"), b"independent index").unwrap();
    let mut selected = request(&directory);
    selected.store = selected
        .store
        .with_sqlite_profile(SqlitePersistenceProfile::Durable);
    selected.deadline = Instant::now() + Duration::from_secs(30);
    let config = selected.store.clone();
    let project = Timing::disabled("sdk.git.init", |timer| {
        ProjectApi::new().init_with_git_directory(selected, directory.join("git-stage"), timer)
    })
    .0
    .unwrap();
    assert_eq!(project.manifest.profile, StoreProfile::Durable);
    assert_eq!(project.initialized.namespace_work.wide_directories, 1);
    let handles = Handles::open_writable(
        config,
        &project.manifest.binding,
        project.manifest.cursor_key,
    )
    .unwrap();
    let storage = layerfs_storage::Storage::new(handles.storage.clone()).unwrap();
    {
        let reader = storage.reader().unwrap();
        let mut tree =
            FilesystemRead::new(&reader, FilesystemRootId(project.initialized.root)).unwrap();
        let original = tree
            .resolve(&LogicalPath::new("ignored-file").unwrap())
            .unwrap();
        let alias = tree.resolve(&LogicalPath::new("alias").unwrap()).unwrap();
        assert_eq!(original.serial, alias.serial);
        assert_eq!(
            tree.readlink(&LogicalPath::new("link").unwrap())
                .unwrap()
                .as_bytes(),
            b"ignored-file"
        );
        for (name, expected) in [
            (".git/index", b"independent index".as_slice()),
            ("ignored-file", b"ignored data".as_slice()),
        ] {
            let inode = tree.resolve(&LogicalPath::new(name).unwrap()).unwrap();
            Timing::disabled("git.oracle", |timer| {
                let view =
                    FileView::open(&reader, inode.value.content_root, timer.child("open")).unwrap();
                let mut actual = Vec::new();
                view.read_range(&reader, 0..view.logical_len(), &mut actual, timer)
                    .unwrap();
                assert_eq!(actual, expected);
                Ok::<(), layerfs_content::ContentError>(())
            })
            .0
            .unwrap();
        }
    }
    drop(storage);
    handles.seal().unwrap();
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn git_substitution_refuses_overlap_and_store_inside_input_before_store_creation() {
    use layerfs_api::ProjectError;
    let directory =
        std::env::temp_dir().join(format!("layerfs-sdk-git-refusal-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    fs::create_dir(directory.join("source")).unwrap();
    fs::write(directory.join("source/.git"), b"gitdir: /outside\n").unwrap();
    fs::create_dir(directory.join("git-stage")).unwrap();
    let nested = directory.join("source/nested");
    fs::create_dir(&nested).unwrap();
    let failed = Timing::disabled("sdk.git.init", |timer| {
        ProjectApi::new().init_with_git_directory(request(&directory), nested.clone(), timer)
    })
    .0
    .unwrap_err();
    assert!(matches!(
        failed.original.error,
        InitError::Project(ProjectError::SourceDirectoriesOverlap)
    ));
    assert_eq!(failed.git_directory, nested);
    assert!(!directory.join("sealed.sqlite").exists());
    let mut inside = request(&directory);
    inside.store = PersistenceConfig::sqlite(directory.join("git-stage/sealed.sqlite"));
    let failed = Timing::disabled("sdk.git.init", |timer| {
        ProjectApi::new().init_with_git_directory(inside, directory.join("git-stage"), timer)
    })
    .0
    .unwrap_err();
    assert!(matches!(
        failed.original.error,
        InitError::Project(ProjectError::BackingInsideSource)
    ));
    assert!(!directory.join("git-stage/sealed.sqlite").exists());
    fs::remove_file(directory.join("source/.git")).unwrap();
    std::os::unix::fs::symlink("/outside", directory.join("source/.git")).unwrap();
    let failed = Timing::disabled("sdk.git.init", |timer| {
        ProjectApi::new().init_with_git_directory(
            request(&directory),
            directory.join("git-stage"),
            timer,
        )
    })
    .0
    .unwrap_err();
    assert!(matches!(
        failed.original.error,
        InitError::Project(ProjectError::InvalidInput { .. })
    ));
    assert!(!directory.join("sealed.sqlite").exists());
    fs::remove_dir_all(directory).unwrap();
}
