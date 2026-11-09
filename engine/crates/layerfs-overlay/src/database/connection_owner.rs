//! Retains an uncertain durable SQLite connection until process exit.
use rusqlite::Connection;
use std::{
    cell::RefCell,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Default)]
struct OwnerFlags {
    quarantined: AtomicBool,
    durable: AtomicBool,
}

#[derive(Clone, Default)]
pub(crate) struct Quarantine(Arc<OwnerFlags>);
impl Quarantine {
    pub fn get(&self) -> bool {
        self.0.quarantined.load(Ordering::Acquire)
    }
    pub fn set(&self, value: bool) {
        self.0.quarantined.store(value, Ordering::Release);
    }
    #[cfg(target_os = "linux")]
    pub fn set_durable(&self) {
        self.0.durable.store(true, Ordering::Release);
    }
    fn retain(&self) -> bool {
        if self.0.durable.load(Ordering::Acquire) && (self.get() || std::thread::panicking()) {
            self.set(true);
            true
        } else {
            false
        }
    }
}

pub(crate) struct ConnectionOwner {
    connection: Option<Connection>,
    quarantine: Quarantine,
}
impl ConnectionOwner {
    pub fn new(connection: Connection, quarantine: Quarantine) -> Self {
        Self {
            connection: Some(connection),
            quarantine,
        }
    }
    pub fn close(mut self) -> Result<(), (Connection, rusqlite::Error)> {
        self.connection.take().expect("owned connection").close()
    }
}
impl Deref for ConnectionOwner {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.connection.as_ref().expect("owned connection")
    }
}
impl Drop for ConnectionOwner {
    fn drop(&mut self) {
        if self.quarantine.retain() {
            if let Some(connection) = self.connection.take() {
                // SQLite's destructor could roll back an unknown publication.
                // Retain this exact connection until the OS ends the process.
                std::mem::forget(connection);
            }
        }
    }
}

/// Preserve in-memory operation, orphan and handle custody with its database.
pub(crate) struct OpenOwner {
    open: Option<RefCell<crate::open::OpenState>>,
    quarantine: Quarantine,
}
impl OpenOwner {
    pub fn new(open: crate::open::OpenState, quarantine: Quarantine) -> Self {
        Self {
            open: Some(RefCell::new(open)),
            quarantine,
        }
    }
    pub fn into_inner(mut self) -> crate::open::OpenState {
        self.open.take().expect("owned open state").into_inner()
    }
}
impl Deref for OpenOwner {
    type Target = RefCell<crate::open::OpenState>;

    fn deref(&self) -> &Self::Target {
        self.open.as_ref().expect("owned open state")
    }
}
impl Drop for OpenOwner {
    fn drop(&mut self) {
        if self.quarantine.retain() {
            if let Some(open) = self.open.take() {
                std::mem::forget(open);
            }
        }
    }
}
