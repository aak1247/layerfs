//! Durable single-owner overlay transactions and conservative reopening.
//!
//! This is a database boundary, not a mount recovery protocol. Its caller
//! must fence the prior mount and its consumers and reconcile unknown external
//! publications before using the returned Workspace in another execution.
use crate::{Overlay, OverlayError, OverlayResult, ProfileConfig, StatementKind, WorkspaceState};
#[cfg(target_os = "linux")]
use std::cell::Cell;
#[cfg(not(target_os = "linux"))]
use std::path::Path;

#[cfg(target_os = "linux")]
const APPLICATION_ID: i64 = 1_279_676_211;

/// Reconciliation conditions shared by reopening and the recorded barrier.
pub(super) const BOUND_PENDING_SQL: &str =
    "SELECT EXISTS(SELECT 1 FROM fs_inode WHERE generation=-1)\
     OR EXISTS(SELECT 1 FROM fs_data WHERE generation=-1)\
     OR EXISTS(SELECT 1 FROM fs_data_ref WHERE generation=-1)\
     OR EXISTS(SELECT 1 FROM fs_truncate WHERE generation=-1)\
     OR EXISTS(SELECT 1 FROM commit_record)\
     OR EXISTS(SELECT 1 FROM cleanup_task WHERE ready=0)\
     OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind IN(1,2,6,7,8,9,10,11))\
     OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource>0)\
     OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource=0 AND target<=0)\
     OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind=3 AND resource=0 AND \
        target>(SELECT installed FROM durable_state WHERE id=1))";

/// The separate owner file is never deleted or replaced by this engine.
pub(crate) struct Lease {
    #[cfg(target_os = "linux")]
    file: Option<nix::fcntl::Flock<std::fs::File>>,
    #[cfg(target_os = "linux")]
    path: std::path::PathBuf,
    #[cfg(target_os = "linux")]
    directory: Option<std::fs::File>,
    #[cfg(target_os = "linux")]
    quarantine: Option<super::connection_owner::Quarantine>,
}
impl Lease {
    #[cfg(target_os = "linux")]
    fn track(&mut self, quarantine: super::connection_owner::Quarantine) {
        self.quarantine = Some(quarantine);
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if self.quarantine.as_ref().is_some_and(|state| state.get()) {
            if let Some(file) = self.file.take() {
                // Keep another owner out while an uncertain SQLite connection
                // is retained, even if the caller drops the original error.
                std::mem::forget(file);
            }
            if let Some(directory) = self.directory.take() {
                std::mem::forget(directory);
            }
        }
    }
}

/// Runtime custody that cannot yet be reconstructed after process loss.
pub(super) fn volatile(db: &Overlay) -> bool {
    let open = db.open.borrow();
    db.orphan_seen.get() || !open.kept_live.rows.is_empty() || open.operations.len() != 0
}

pub(super) fn same_persistent_state(a: Option<WorkspaceState>, b: Option<WorkspaceState>) -> bool {
    fn persistent(mut state: WorkspaceState) -> WorkspaceState {
        // These are process-local read windows, all fenced when the old owner
        // has stopped. They are never restored as live references.
        state.base_readers = 0;
        state
    }
    a.map(persistent) == b.map(persistent)
}

/// Joins lifecycle metadata to the same SQL transaction as filesystem rows.
pub(super) fn save(db: &Overlay, volatile: bool) -> OverlayResult<()> {
    let state = db.workspace.get().ok_or(OverlayError::Stale)?;
    let captured = state.captured.map(|generation| generation.0);
    let consolidating = state.consolidating.map(|generation| generation.0);
    let dirty_inodes = super::connection::integer(state.dirty_inodes)?;
    let dirty_entries = super::connection::integer(state.dirty_directory_entries)?;
    db.execute(
        StatementKind::Frontier,
        "INSERT INTO durable_state(id,active,captured,captured_revision,installed,revision,\
         base_root,dirty_inodes,dirty_entries,closed,consolidating,volatile_owners)\
         VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)\
         ON CONFLICT(id) DO UPDATE SET active=excluded.active,captured=excluded.captured,\
         captured_revision=excluded.captured_revision,installed=excluded.installed,\
         revision=excluded.revision,base_root=excluded.base_root,\
         dirty_inodes=excluded.dirty_inodes,dirty_entries=excluded.dirty_entries,\
         closed=excluded.closed,consolidating=excluded.consolidating,\
         volatile_owners=excluded.volatile_owners",
        &[
            &state.active.0,
            &captured,
            &state.captured_revision,
            &state.installed,
            &state.revision,
            &state.base_root.as_slice(),
            &dirty_inodes,
            &dirty_entries,
            &state.closed,
            &consolidating,
            &volatile,
        ],
        112,
    )?;
    Ok(())
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use crate::{DatabaseWork, Generation};
    use nix::fcntl::{Flock, FlockArg};
    use rusqlite::{Connection, OpenFlags};
    use std::{
        cell::RefCell,
        fs::{self, File, OpenOptions},
        io::{Read, Write},
        os::unix::fs::{MetadataExt, OpenOptionsExt},
        path::{Path, PathBuf},
        sync::Arc,
    };

    const STATE_SCHEMA: &str = "CREATE TABLE durable_state(\
      id INTEGER PRIMARY KEY CHECK(id=1), active INTEGER NOT NULL CHECK(active>0),\
      captured INTEGER CHECK(captured>0), captured_revision INTEGER CHECK(captured_revision>=0),\
      installed INTEGER NOT NULL CHECK(installed>=0), revision INTEGER NOT NULL CHECK(revision>=0),\
      base_root BLOB NOT NULL CHECK(length(base_root)=32),\
      dirty_inodes INTEGER NOT NULL CHECK(dirty_inodes>=0),\
      dirty_entries INTEGER NOT NULL CHECK(dirty_entries>=0),\
      closed INTEGER NOT NULL CHECK(closed IN(0,1)), consolidating INTEGER CHECK(consolidating>0),\
      volatile_owners INTEGER NOT NULL CHECK(volatile_owners IN(0,1)),\
      CHECK((captured IS NULL)=(captured_revision IS NULL)), CHECK(active>installed)\
      ) STRICT;";

    fn validate(path: &Path, config: ProfileConfig, identity: &[u8; 32]) -> OverlayResult<PathBuf> {
        if *identity == [0; 32] || config.max_pages.is_some() || config.pager_kib == 0 {
            return Err(OverlayError::Invalid("durable overlay identity or quota"));
        }
        let parent = path
            .parent()
            .ok_or(OverlayError::Invalid("durable overlay parent"))?
            .canonicalize()?;
        let name = path
            .file_name()
            .ok_or(OverlayError::Invalid("durable overlay name"))?;
        check_directory(&parent)?;
        let kind = nix::sys::statfs::statfs(&parent)
            .map_err(|error| std::io::Error::from_raw_os_error(error as i32))?
            .filesystem_type();
        if ![
            nix::sys::statfs::EXT4_SUPER_MAGIC,
            nix::sys::statfs::XFS_SUPER_MAGIC,
            nix::sys::statfs::BTRFS_SUPER_MAGIC,
        ]
        .contains(&kind)
        {
            return Err(OverlayError::Invalid(
                "durable overlay requires a local filesystem",
            ));
        }
        Ok(parent.join(name))
    }

    fn check_directory(parent: &Path) -> OverlayResult<()> {
        let metadata = fs::symlink_metadata(parent)?;
        let uid = fs::metadata("/proc/self")?.uid();
        if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
            return Err(OverlayError::Invalid(
                "durable overlay directory must be private",
            ));
        }
        Ok(())
    }

    fn check_directory_handle(directory: &File, parent: &Path) -> OverlayResult<()> {
        check_directory(parent)?;
        let opened = directory.metadata()?;
        let named = fs::symlink_metadata(parent)?;
        if !opened.is_dir()
            || opened.uid() != named.uid()
            || opened.mode() & 0o077 != 0
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
        {
            return Err(OverlayError::Invalid("durable overlay directory identity"));
        }
        Ok(())
    }

    fn owner_path(path: &Path) -> PathBuf {
        let mut name = path.as_os_str().to_owned();
        name.push(".owner");
        name.into()
    }

    fn check_file(file: &File, path: &Path) -> OverlayResult<()> {
        let opened = file.metadata()?;
        let named = fs::symlink_metadata(path)?;
        let uid = fs::metadata("/proc/self")?.uid();
        if !opened.is_file()
            || !named.is_file()
            || opened.nlink() != 1
            || named.nlink() != 1
            || opened.uid() != uid
            || opened.mode() & 0o077 != 0
            || opened.mode() & 0o600 != 0o600
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
        {
            return Err(OverlayError::Invalid("durable overlay file identity"));
        }
        Ok(())
    }

    impl Lease {
        fn acquire(path: &Path, identity: &[u8; 32], create: bool) -> OverlayResult<Self> {
            let parent = path.parent().ok_or(OverlayError::Invalid("owner parent"))?;
            let directory = File::open(parent)?;
            check_directory_handle(&directory, parent)?;
            let owner = owner_path(path);
            let mut options = OpenOptions::new();
            options
                .read(true)
                .write(true)
                .mode(0o600)
                .custom_flags(nix::libc::O_NOFOLLOW);
            if create {
                options.create_new(true);
            }
            let file = options.open(&owner)?;
            check_file(&file, &owner)?;
            let mut file = Flock::lock(file, FlockArg::LockExclusiveNonblock)
                .map_err(|(_, error)| std::io::Error::from_raw_os_error(error as i32))?;
            check_file(&file, &owner)?;
            if create {
                file.write_all(identity)?;
                file.sync_all()?;
                directory.sync_all()?;
                check_directory_handle(&directory, parent)?;
            } else {
                let mut stored = [0; 32];
                file.read_exact(&mut stored)?;
                if stored != *identity || file.read(&mut [0])? != 0 {
                    return Err(OverlayError::Invalid("durable overlay owner identity"));
                }
            }
            Ok(Self {
                file: Some(file),
                path: owner,
                directory: Some(directory),
                quarantine: None,
            })
        }

        pub(crate) fn verify(&self) -> OverlayResult<()> {
            let parent = self
                .path
                .parent()
                .ok_or(OverlayError::Invalid("owner parent"))?;
            check_directory_handle(self.directory.as_ref().expect("owned directory"), parent)?;
            check_file(self.file.as_ref().expect("owned lease"), &self.path)
        }

        pub(crate) fn sync_directory(&self) -> OverlayResult<()> {
            self.verify()?;
            self.directory
                .as_ref()
                .expect("owned directory")
                .sync_all()?;
            self.verify()
        }
    }

    pub(crate) fn create(
        path: &Path,
        config: ProfileConfig,
        identity: [u8; 32],
    ) -> OverlayResult<Overlay> {
        let path = validate(path, config, &identity)?;
        // An existing database is never converted or replaced.
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err(OverlayError::Invalid("durable overlay already exists")),
        }
        let mut lease = Lease::acquire(&path, &identity, true)?;
        let mut db = Overlay::create(&path, config)?;
        let mut calls = 0;
        let mut profile = super::super::profile::initialize_with(
            &db.connection,
            config,
            &db.work,
            &mut calls,
            super::super::profile::Persistence::Durable,
        )?;
        profile.schema_version = db.profile.schema_version;
        db.profile = profile;
        lease.track(db.quarantined.clone());
        db.quarantined.set_durable();
        db.durable = Some(lease);
        db.atomic(|| {
            db.execute(
                StatementKind::Startup,
                "CREATE TABLE durable_identity(id INTEGER PRIMARY KEY CHECK(id=1), \
                 identity BLOB NOT NULL CHECK(length(identity)=32)) STRICT",
                &[],
                0,
            )?;
            crate::diagnostics::startup::batch(&db.connection, &db.work, STATE_SCHEMA)?;
            db.execute(
                StatementKind::Startup,
                "CREATE TABLE durable_binding(id INTEGER PRIMARY KEY CHECK(id=1), \
                 root BLOB NOT NULL CHECK(length(root)=32), \
                 snapshot BLOB NOT NULL CHECK(length(snapshot)>0 AND length(snapshot)<=4096), \
                 recoverable INTEGER NOT NULL CHECK(recoverable IN(0,1))) STRICT",
                &[],
                0,
            )?;
            db.execute(
                StatementKind::Startup,
                "CREATE TABLE durable_route(id INTEGER PRIMARY KEY CHECK(id=1), \
                 high_namespace INTEGER NOT NULL CHECK(high_namespace>0)) STRICT",
                &[],
                0,
            )?;
            db.execute(StatementKind::Startup, "PRAGMA user_version=41", &[], 0)?;
            db.execute(
                StatementKind::Startup,
                "PRAGMA application_id=1279676211",
                &[],
                0,
            )?;
            db.execute(
                StatementKind::Startup,
                "INSERT INTO durable_identity(id,identity) VALUES(1,?1)",
                &[&identity.as_slice()],
                32,
            )?;
            Ok(())
        })?;
        db.profile.schema_version = 41;
        let sync = db.allocation.sync_durable_file().and_then(|_| {
            File::open(
                path.parent()
                    .ok_or(OverlayError::Invalid("overlay parent"))?,
            )?
            .sync_all()
            .map_err(OverlayError::Io)
        });
        if let Err(cause) = sync {
            db.quarantined.set(true);
            return Err(OverlayError::Uncertain {
                cause: Box::new(cause),
                completion: None,
            });
        }
        Ok(db)
    }

    pub(crate) fn recover(
        path: &Path,
        config: ProfileConfig,
        identity: [u8; 32],
    ) -> OverlayResult<Overlay> {
        let path = validate(path, config, &identity)?;
        let mut lease = Lease::acquire(&path, &identity, false)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(nix::libc::O_NOFOLLOW)
            .open(&path)?;
        check_file(&file, &path)?;
        let allocation = super::super::physical::Allocation::new(file, &path)?;
        let connection = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        allocation.state()?;
        let work = RefCell::new(DatabaseWork::default());
        let app: i64 =
            super::super::profile::readback(&connection, &work, "PRAGMA application_id", |r| {
                r.get(0)
            })?;
        if app != APPLICATION_ID {
            return Err(OverlayError::Invalid(
                "durable overlay application identity",
            ));
        }
        let stored: Vec<u8> = super::super::profile::readback(
            &connection,
            &work,
            "SELECT identity FROM durable_identity WHERE id=1",
            |r| r.get(0),
        )?;
        if stored != identity {
            return Err(OverlayError::Invalid("durable overlay workspace identity"));
        }
        let mut calls = 0;
        let mut profile = super::super::profile::initialize_with(
            &connection,
            config,
            &work,
            &mut calls,
            super::super::profile::Persistence::Durable,
        )?;
        profile.schema_version =
            super::super::profile::readback(&connection, &work, "PRAGMA user_version", |r| {
                r.get(0)
            })?;
        if ![39, 40, 41].contains(&profile.schema_version) {
            return Err(OverlayError::Invalid("durable overlay schema version"));
        }
        let integrity: String =
            super::super::profile::readback(&connection, &work, "PRAGMA quick_check(1)", |r| {
                r.get(0)
            })?;
        if integrity != "ok" {
            return Err(OverlayError::Invalid("durable overlay integrity check"));
        }
        let (state, held) = read_state(&connection, &work)?;
        // Ready retirement is safe only for an installed generation with a
        // matching recorded receipt. The caller still fences the old mount and
        // validates that receipt against the same authorized Store first.
        let bound = if matches!(profile.schema_version, 40 | 41) {
            super::super::profile::readback(
                &connection,
                &work,
                "SELECT EXISTS(SELECT 1 FROM durable_binding b JOIN durable_state s ON s.id=1 \
                 WHERE b.id=1 AND b.recoverable=1 AND b.root=s.base_root)",
                |r| r.get::<_, bool>(0),
            )?
        } else {
            false
        };
        let pending_sql = if bound {
            BOUND_PENDING_SQL
        } else {
            "SELECT EXISTS(SELECT 1 FROM fs_inode WHERE generation=-1)\
             OR EXISTS(SELECT 1 FROM fs_data WHERE generation=-1)\
             OR EXISTS(SELECT 1 FROM fs_data_ref WHERE generation=-1)\
             OR EXISTS(SELECT 1 FROM fs_truncate WHERE generation=-1)\
             OR EXISTS(SELECT 1 FROM commit_record)\
             OR EXISTS(SELECT 1 FROM cleanup_task WHERE kind NOT IN(4,5,12) OR ready<>1)"
        };
        let pending: bool =
            super::super::profile::readback(&connection, &work, pending_sql, |r| r.get(0))?;
        if state.closed
            || state.captured.is_some()
            || state.consolidating.is_some()
            || held
            || pending
        {
            return Err(OverlayError::Invalid(
                "durable overlay requires reconciliation",
            ));
        }
        // Only ready shrink cleanup and receipt-qualified retirement resume;
        // all other cleanup kinds remain fenced until separately qualified.
        let maintenance: bool = super::super::profile::readback(
            &connection,
            &work,
            "SELECT EXISTS(SELECT 1 FROM cleanup_task)",
            |r| r.get(0),
        )?;
        lease.verify()?;
        connection.set_prepared_statement_cache_capacity(256);
        let engine = super::super::startup::next_engine()?;
        let quarantine = super::super::connection_owner::Quarantine::default();
        quarantine.set_durable();
        lease.track(quarantine.clone());
        Ok(Overlay {
            identity: engine,
            connection: super::super::connection_owner::ConnectionOwner::new(
                connection,
                quarantine.clone(),
            ),
            open: super::super::connection_owner::OpenOwner::new(
                crate::open::OpenState::default(),
                quarantine.clone(),
            ),
            work,
            payload_work: Cell::new(crate::PayloadWork::default()),
            quarantined: quarantine,
            workspace: Cell::new(Some(state)),
            maintenance_ready: Cell::new(maintenance),
            orphan_seen: Cell::new(false),
            pieces_seen: Cell::new(true),
            release_step: Cell::new(false),
            transaction: Cell::new(super::super::connection::Transaction::None),
            tickets: Arc::new(crate::ReplyTickets::new(engine)),
            issued: Default::default(),
            profile,
            allocation,
            durable: Some(lease),
        })
    }

    struct StoredState {
        active: i64,
        captured: Option<i64>,
        captured_revision: Option<i64>,
        installed: i64,
        revision: i64,
        root: Vec<u8>,
        inodes: i64,
        entries: i64,
        closed: bool,
        consolidating: Option<i64>,
        held: bool,
    }

    fn read_state(
        c: &Connection,
        work: &RefCell<DatabaseWork>,
    ) -> OverlayResult<(WorkspaceState, bool)> {
        let StoredState {
            active, captured, captured_revision, installed, revision, root, inodes,
            entries, closed, consolidating, held,
        } = super::super::profile::readback(
            c,
            work,
            "SELECT active,captured,captured_revision,installed,revision,base_root,dirty_inodes,dirty_entries,closed,consolidating,volatile_owners FROM durable_state WHERE id=1",
            |r| Ok(StoredState {
                active: r.get(0)?, captured: r.get(1)?, captured_revision: r.get(2)?,
                installed: r.get(3)?, revision: r.get(4)?, root: r.get(5)?,
                inodes: r.get(6)?, entries: r.get(7)?, closed: r.get(8)?,
                consolidating: r.get(9)?, held: r.get(10)?,
            }),
        )?;
        if active <= installed
            || installed < 0
            || revision < 0
            || captured.is_some() != captured_revision.is_some()
        {
            return Err(OverlayError::Invalid("durable overlay state"));
        }
        let base_root = root
            .try_into()
            .map_err(|_| OverlayError::Invalid("durable overlay root"))?;
        let dirty_inodes = u64::try_from(inodes)
            .map_err(|_| OverlayError::Invalid("durable overlay inode count"))?;
        let dirty_directory_entries = u64::try_from(entries)
            .map_err(|_| OverlayError::Invalid("durable overlay name count"))?;
        Ok((
            WorkspaceState {
                active: Generation(active),
                captured: captured.map(Generation),
                captured_revision,
                installed,
                revision,
                base_root,
                dirty_inodes,
                dirty_directory_entries,
                closed,
                base_readers: 0,
                consolidating: consolidating.map(Generation),
            },
            held,
        ))
    }
}

#[cfg(not(target_os = "linux"))]
impl Lease {
    pub(crate) fn sync_directory(&self) -> OverlayResult<()> {
        Err(OverlayError::UnsupportedPlatform)
    }
}

#[cfg(not(target_os = "linux"))]
impl Lease {
    pub(crate) fn verify(&self) -> OverlayResult<()> {
        Err(OverlayError::UnsupportedPlatform)
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::{create, recover};

#[cfg(not(target_os = "linux"))]
pub(super) fn create(_: &Path, _: ProfileConfig, _: [u8; 32]) -> OverlayResult<Overlay> {
    Err(OverlayError::UnsupportedPlatform)
}

#[cfg(not(target_os = "linux"))]
pub(super) fn recover(_: &Path, _: ProfileConfig, _: [u8; 32]) -> OverlayResult<Overlay> {
    Err(OverlayError::UnsupportedPlatform)
}
