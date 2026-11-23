//! Bounded metadata and directory pages within one captured root.
use crate::{
    control::{CaptureReadHandle, ControlError},
    control_capture_read::{handle, put_handle},
    wire::{Reader, Writer},
};
use layerfs_content::filesystem::{LogicalPath, PathName, SymlinkTarget};

/// Maximum directory entries returned in one captured-root page.
pub const CAPTURE_LIST_LIMIT: u16 = 64;

/// One canonical path under the connection's captured root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturePath {
    /// Exact connection-local handle.
    pub handle: CaptureReadHandle,
    /// Canonical relative path; empty denotes the captured root.
    pub path: String,
}

/// The portable kind of a captured inode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureKind {
    /// A directory.
    Directory,
    /// A regular file.
    File,
    /// An opaque symbolic link.
    Symlink,
}

/// Portable metadata for the path without following its last link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureStat {
    /// Original selected path and handle.
    pub path: CapturePath,
    /// Stored inode kind.
    pub kind: CaptureKind,
    /// Portable permission bits.
    pub mode: u32,
    /// Modification time in whole seconds.
    pub mtime_seconds: i64,
    /// Modification time fractional nanoseconds.
    pub mtime_nanoseconds: u32,
    /// Number of namespace bindings; the filesystem root has zero.
    pub namespace_references: u64,
    /// File or link length, or zero for a directory.
    pub size: u64,
    /// Whether the inode carries only portable attribute domains.
    pub portable_attributes_only: bool,
}

/// One directory page, optionally resuming this exact directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureList {
    /// Original selected directory path and handle.
    pub path: CapturePath,
    /// Opaque continuation returned by the same connection.
    pub cursor: Option<[u8; 32]>,
    /// Maximum number of names, from one through [`CAPTURE_LIST_LIMIT`].
    pub limit: u16,
}

/// Ordered names from one original directory page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturePage {
    /// The original request.
    pub request: CaptureList,
    /// Names in strict byte order.
    pub names: Vec<String>,
    /// Continuation for this same handle and directory.
    pub continuation: Option<[u8; 32]>,
}

/// Exact stored bytes of a selected symbolic link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureLink {
    /// Original selected path and handle.
    pub path: CapturePath,
    /// Opaque stored link target bytes.
    pub target: Vec<u8>,
}

fn valid_path(path: &str) -> Result<(), ControlError> {
    LogicalPath::new(path)
        .map(|_| ())
        .map_err(|_| ControlError("capture path"))
}
fn put_optional_id(out: &mut Writer, id: Option<[u8; 32]>) -> Result<(), ControlError> {
    match id {
        None => out.byte(0),
        Some(id) if id != [0; 32] => {
            out.byte(1)?;
            out.put(&id)
        }
        Some(_) => Err(ControlError("capture cursor")),
    }
}
fn optional_id(input: &mut Reader<'_>) -> Result<Option<[u8; 32]>, ControlError> {
    match input.byte()? {
        0 => Ok(None),
        1 => {
            let id = input.array()?;
            if id == [0; 32] {
                return Err(ControlError("capture cursor"));
            }
            Ok(Some(id))
        }
        _ => Err(ControlError("capture cursor")),
    }
}
pub(crate) fn put_path(out: &mut Writer, value: &CapturePath) -> Result<(), ControlError> {
    valid_path(&value.path)?;
    put_handle(out, value.handle)?;
    out.blob(value.path.as_bytes())
}
pub(crate) fn path(input: &mut Reader<'_>) -> Result<CapturePath, ControlError> {
    let handle = handle(input)?;
    let path = input.text(4096)?;
    valid_path(&path)?;
    Ok(CapturePath { handle, path })
}
pub(crate) fn put_list(out: &mut Writer, value: &CaptureList) -> Result<(), ControlError> {
    if value.limit == 0 || value.limit > CAPTURE_LIST_LIMIT {
        return Err(ControlError("capture list limit"));
    }
    put_path(out, &value.path)?;
    put_optional_id(out, value.cursor)?;
    out.put(&value.limit.to_be_bytes())
}
pub(crate) fn list(input: &mut Reader<'_>) -> Result<CaptureList, ControlError> {
    let path = path(input)?;
    let cursor = optional_id(input)?;
    let limit = u16::from_be_bytes(input.array()?);
    if limit == 0 || limit > CAPTURE_LIST_LIMIT {
        return Err(ControlError("capture list limit"));
    }
    Ok(CaptureList {
        path,
        cursor,
        limit,
    })
}
pub(crate) fn put_page(out: &mut Writer, value: &CapturePage) -> Result<(), ControlError> {
    if value.names.len() > usize::from(value.request.limit)
        || (value.names.is_empty() && value.continuation.is_some())
    {
        return Err(ControlError("capture page length"));
    }
    put_list(out, &value.request)?;
    out.put(&(value.names.len() as u16).to_be_bytes())?;
    let mut previous: Option<&[u8]> = None;
    for name in &value.names {
        PathName::new(name).map_err(|_| ControlError("capture page name"))?;
        if previous.is_some_and(|old| old >= name.as_bytes()) {
            return Err(ControlError("capture page order"));
        }
        previous = Some(name.as_bytes());
        out.blob(name.as_bytes())?;
    }
    put_optional_id(out, value.continuation)
}
pub(crate) fn page(input: &mut Reader<'_>) -> Result<CapturePage, ControlError> {
    let request = list(input)?;
    let count = usize::from(u16::from_be_bytes(input.array()?));
    if count > usize::from(request.limit) {
        return Err(ControlError("capture page length"));
    }
    let mut names = Vec::with_capacity(count);
    for _ in 0..count {
        names.push(input.text(255)?);
    }
    let continuation = optional_id(input)?;
    let value = CapturePage {
        request,
        names,
        continuation,
    };
    // Reuse all canonical checks without retaining an alternate representation.
    put_page(&mut Writer::new(&[]), &value)?;
    Ok(value)
}
fn validate_stat(value: &CaptureStat) -> Result<(), ControlError> {
    let mask = match value.kind {
        CaptureKind::Directory => 0o1777,
        _ => 0o777,
    };
    let references_valid = if value.path.path.is_empty() {
        value.kind == CaptureKind::Directory && value.namespace_references == 0
    } else {
        value.namespace_references >= 1
            && (value.kind == CaptureKind::File || value.namespace_references == 1)
    };
    if value.mode & !mask != 0
        || (value.kind == CaptureKind::Symlink && value.mode != 0o777)
        || value.mtime_nanoseconds >= 1_000_000_000
        || (value.kind == CaptureKind::Directory && value.size != 0)
        || (value.kind == CaptureKind::Symlink && value.size > 4096)
        || !references_valid
    {
        return Err(ControlError("capture metadata"));
    }
    Ok(())
}
pub(crate) fn put_stat(out: &mut Writer, value: &CaptureStat) -> Result<(), ControlError> {
    validate_stat(value)?;
    put_path(out, &value.path)?;
    out.byte(match value.kind {
        CaptureKind::Directory => 1,
        CaptureKind::File => 2,
        CaptureKind::Symlink => 3,
    })?;
    out.put(&value.mode.to_be_bytes())?;
    out.put(&value.mtime_seconds.to_be_bytes())?;
    out.put(&value.mtime_nanoseconds.to_be_bytes())?;
    out.put(&value.namespace_references.to_be_bytes())?;
    out.put(&value.size.to_be_bytes())?;
    out.byte(u8::from(value.portable_attributes_only))
}
pub(crate) fn stat(input: &mut Reader<'_>) -> Result<CaptureStat, ControlError> {
    let path = path(input)?;
    let kind = match input.byte()? {
        1 => CaptureKind::Directory,
        2 => CaptureKind::File,
        3 => CaptureKind::Symlink,
        _ => return Err(ControlError("capture kind")),
    };
    let mode = u32::from_be_bytes(input.array()?);
    let mtime_seconds = i64::from_be_bytes(input.array()?);
    let mtime_nanoseconds = u32::from_be_bytes(input.array()?);
    let namespace_references = u64::from_be_bytes(input.array()?);
    let size = u64::from_be_bytes(input.array()?);
    let portable_attributes_only = match input.byte()? {
        0 => false,
        1 => true,
        _ => return Err(ControlError("capture attributes")),
    };
    let value = CaptureStat {
        path,
        kind,
        mode,
        mtime_seconds,
        mtime_nanoseconds,
        namespace_references,
        size,
        portable_attributes_only,
    };
    validate_stat(&value)?;
    Ok(value)
}
pub(crate) fn put_link(out: &mut Writer, value: &CaptureLink) -> Result<(), ControlError> {
    SymlinkTarget::new(value.target.clone()).map_err(|_| ControlError("capture link"))?;
    put_path(out, &value.path)?;
    out.blob(&value.target)
}
pub(crate) fn link(input: &mut Reader<'_>) -> Result<CaptureLink, ControlError> {
    let path = path(input)?;
    let target = input.blob(4096)?.to_vec();
    SymlinkTarget::new(target.clone()).map_err(|_| ControlError("capture link"))?;
    Ok(CaptureLink { path, target })
}
