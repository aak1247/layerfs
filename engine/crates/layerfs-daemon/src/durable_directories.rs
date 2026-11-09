//! One-shot creation of private durable directories on an authenticated volume.
use nix::{
    fcntl::{openat, OFlag},
    sys::{
        stat::{mkdirat, Mode},
        statfs,
    },
};
use std::{
    ffi::OsStr,
    fmt::{self, Write as _},
    fs::{self, File, OpenOptions},
    io,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

const SHARED: &str = "layerfs-durable";
const CATALOG: &str = "catalog";
const OVERLAYS: &str = "overlays";

/// The last original boundary attempted during one directory creation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectoryPhase {
    OpenRoot,
    SharedCreate,
    SharedOpen,
    SharedSync,
    SandboxCreate,
    SandboxOpen,
    SandboxSync,
    CatalogCreate,
    CatalogOpen,
    OverlayCreate,
    OverlayOpen,
    DirectoriesSync,
    Verify,
}

/// The original cause and every successfully acquired directory descriptor.
/// An attempted create can have an unknown effect even when it returned an
/// error; the caller keeps this receipt and never silently retries it.
pub struct DirectoryFailure {
    pub phase: DirectoryPhase,
    pub cause: io::Error,
    pub volume: PathBuf,
    pub sandbox: [u8; 32],
    pub store_device: u64,
    pub sandbox_created: bool,
    pub catalog_created: bool,
    pub overlay_created: bool,
    _root: Option<File>,
    _shared: Option<File>,
    _directory: Option<File>,
    _catalog: Option<File>,
    _overlays: Option<File>,
}
impl fmt::Debug for DirectoryFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectoryFailure")
            .field("phase", &self.phase)
            .field("cause", &self.cause)
            .field("volume", &self.volume)
            .field("sandbox", &self.sandbox)
            .field("store_device", &self.store_device)
            .field("sandbox_created", &self.sandbox_created)
            .field("catalog_created", &self.catalog_created)
            .field("overlay_created", &self.overlay_created)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for DirectoryFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable directories {:?}: {}", self.phase, self.cause)
    }
}
impl std::error::Error for DirectoryFailure {}

/// The original directory inodes and their private named placement.
/// Retain this owner until the catalog and Overlay services take custody.
pub struct FreshDirectories {
    root_path: PathBuf,
    root: File,
    shared: File,
    directory: File,
    catalog: File,
    overlays: File,
    name: String,
    device: u64,
}
impl FreshDirectories {
    pub fn catalog_path(&self) -> PathBuf {
        self.root_path.join(SHARED).join(&self.name).join(CATALOG)
    }
    pub fn overlay_path(&self) -> PathBuf {
        self.root_path.join(SHARED).join(&self.name).join(OVERLAYS)
    }
    pub(crate) fn catalog_descriptor_path(&self) -> PathBuf {
        descriptor_path(&self.catalog)
    }
    pub(crate) fn overlay_descriptor_path(&self) -> PathBuf {
        descriptor_path(&self.overlays)
    }
    /// Verifies the original root and each child against its retained parent.
    pub fn verify(&self) -> io::Result<()> {
        check_root(&self.root_path, &self.root, self.device)?;
        check_child(&self.root, SHARED, &self.shared, self.device)?;
        check_child(&self.shared, &self.name, &self.directory, self.device)?;
        check_child(&self.directory, CATALOG, &self.catalog, self.device)?;
        check_child(&self.directory, OVERLAYS, &self.overlays, self.device)
    }
}

/// Creates directories for a new globally fresh Sandbox identity. The caller
/// authenticates the physical Store, supplies its actual device, and writes a
/// durable creation intent before this one attempt. Existing Sandbox names are
/// never adopted, replaced or deleted. The volume must be local to one kernel.
pub fn create_fresh_directories(
    volume: &Path,
    sandbox: [u8; 32],
    store_device: u64,
) -> Result<FreshDirectories, Box<DirectoryFailure>> {
    let mut phase = DirectoryPhase::OpenRoot;
    let mut root = None;
    let mut shared = None;
    let mut directory = None;
    let mut catalog = None;
    let mut overlays = None;
    let mut sandbox_created = false;
    let mut catalog_created = false;
    let mut overlay_created = false;
    let mut root_path = PathBuf::new();
    let name = sandbox_name(sandbox);
    let result = (|| -> io::Result<()> {
        if sandbox == [0; 32] || !volume.is_absolute() {
            return Err(invalid("fresh Sandbox identity or volume"));
        }
        if !fs::symlink_metadata(volume)?.is_dir() {
            return Err(invalid("durable volume is not a directory"));
        }
        root = Some(open_root(volume, store_device)?);
        let parent = root.as_ref().expect("volume held");
        root_path = volume.canonicalize()?;
        check_root(&root_path, parent, store_device)?;
        phase = DirectoryPhase::SharedCreate;
        match make(parent, SHARED) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
            Err(error) => return Err(error),
        }
        phase = DirectoryPhase::SharedOpen;
        shared = Some(open_child(parent, SHARED, store_device)?);
        let shared_dir = shared.as_ref().expect("shared directory held");
        phase = DirectoryPhase::SharedSync;
        check_root(&root_path, parent, store_device)?;
        shared_dir.sync_all()?;
        parent.sync_all()?;
        check_child(parent, SHARED, shared_dir, store_device)?;

        phase = DirectoryPhase::SandboxCreate;
        make(shared_dir, &name)?;
        sandbox_created = true;
        phase = DirectoryPhase::SandboxOpen;
        directory = Some(open_child(shared_dir, &name, store_device)?);
        let sandbox_dir = directory.as_ref().expect("sandbox directory held");
        phase = DirectoryPhase::SandboxSync;
        shared_dir.sync_all()?;
        check_child(shared_dir, &name, sandbox_dir, store_device)?;

        phase = DirectoryPhase::CatalogCreate;
        make(sandbox_dir, CATALOG)?;
        catalog_created = true;
        phase = DirectoryPhase::CatalogOpen;
        catalog = Some(open_child(sandbox_dir, CATALOG, store_device)?);
        phase = DirectoryPhase::OverlayCreate;
        make(sandbox_dir, OVERLAYS)?;
        overlay_created = true;
        phase = DirectoryPhase::OverlayOpen;
        overlays = Some(open_child(sandbox_dir, OVERLAYS, store_device)?);
        phase = DirectoryPhase::DirectoriesSync;
        catalog.as_ref().expect("catalog held").sync_all()?;
        overlays.as_ref().expect("overlays held").sync_all()?;
        sandbox_dir.sync_all()?;
        phase = DirectoryPhase::Verify;
        check_root(&root_path, parent, store_device)?;
        check_child(parent, SHARED, shared_dir, store_device)?;
        check_child(shared_dir, &name, sandbox_dir, store_device)?;
        check_child(
            sandbox_dir,
            CATALOG,
            catalog.as_ref().expect("catalog held"),
            store_device,
        )?;
        check_child(
            sandbox_dir,
            OVERLAYS,
            overlays.as_ref().expect("overlays held"),
            store_device,
        )
    })();
    if let Err(cause) = result {
        return Err(Box::new(DirectoryFailure {
            phase,
            cause,
            volume: volume.to_owned(),
            sandbox,
            store_device,
            sandbox_created,
            catalog_created,
            overlay_created,
            _root: root,
            _shared: shared,
            _directory: directory,
            _catalog: catalog,
            _overlays: overlays,
        }));
    }
    Ok(FreshDirectories {
        root_path,
        root: root.expect("volume held"),
        shared: shared.expect("shared directory held"),
        directory: directory.expect("sandbox directory held"),
        catalog: catalog.expect("catalog held"),
        overlays: overlays.expect("overlays held"),
        name,
        device: store_device,
    })
}

fn sandbox_name(sandbox: [u8; 32]) -> String {
    let mut name = String::with_capacity(64);
    for byte in sandbox {
        write!(&mut name, "{byte:02x}").expect("writing to String cannot fail");
    }
    name
}
fn open_root(path: &Path, device: u64) -> io::Result<File> {
    let root = OpenOptions::new()
        .read(true)
        .custom_flags((OFlag::O_NOFOLLOW | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC).bits())
        .open(path)?;
    check_root(path, &root, device)?;
    let kind = statfs::fstatfs(&root).map_err(errno)?.filesystem_type();
    if ![
        statfs::EXT4_SUPER_MAGIC,
        statfs::XFS_SUPER_MAGIC,
        statfs::BTRFS_SUPER_MAGIC,
    ]
    .contains(&kind)
    {
        return Err(invalid("durable volume requires a local filesystem"));
    }
    Ok(root)
}
fn make(parent: &File, name: &str) -> io::Result<()> {
    mkdirat(parent, name, Mode::from_bits_truncate(0o700)).map_err(errno)
}
fn open_child(parent: &File, name: &str, device: u64) -> io::Result<File> {
    let file = openat(
        parent,
        name,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(errno)?;
    check_child(parent, name, &file, device)?;
    Ok(file)
}
fn check_root(path: &Path, root: &File, device: u64) -> io::Result<()> {
    check_metadata(
        &root.metadata()?,
        &fs::symlink_metadata(path)?,
        device,
        false,
    )
}
fn descriptor_path(directory: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}/.", directory.as_raw_fd()))
}
fn check_child(parent: &File, name: &str, child: &File, device: u64) -> io::Result<()> {
    let path =
        PathBuf::from(format!("/proc/self/fd/{}", parent.as_raw_fd())).join(OsStr::new(name));
    check_metadata(
        &child.metadata()?,
        &fs::symlink_metadata(path)?,
        device,
        true,
    )
}
fn check_metadata(
    opened: &fs::Metadata,
    named: &fs::Metadata,
    device: u64,
    private: bool,
) -> io::Result<()> {
    let uid = nix::unistd::geteuid().as_raw();
    let forbidden = if private { 0o077 } else { 0o022 };
    if !opened.is_dir()
        || !named.is_dir()
        || opened.uid() != uid
        || named.uid() != uid
        || opened.mode() & forbidden != 0
        || named.mode() & forbidden != 0
        || opened.mode() & 0o700 != 0o700
        || named.mode() & 0o700 != 0o700
        || opened.dev() != device
        || named.dev() != device
        || opened.ino() != named.ino()
    {
        return Err(invalid("durable directory identity, device or privacy"));
    }
    Ok(())
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn errno(error: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}
