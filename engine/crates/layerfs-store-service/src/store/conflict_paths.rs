//! A bounded byte-page encoding of the original ordered merge paths.
use super::CommitError;
use layerfs_overlay::{conflict_page_digest, CONFLICT_PAGE_BYTES};

const MAGIC: &[u8] = b"LFCM\x01";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ConflictDescription {
    pub paths: u64,
    pub bytes: u64,
    pub digest: [u8; 32],
}

/// Each pass keeps one byte page and never holds an Owner or Store lane.
/// Path order and duplicates are unchanged; lengths are UTF-8 byte lengths.
pub(super) fn pages(
    paths: &[String],
    mut emit: impl FnMut(u64, Vec<u8>) -> Result<(), CommitError>,
) -> Result<ConflictDescription, CommitError> {
    let count = u64::try_from(paths.len())
        .ok()
        .filter(|value| *value > 0)
        .ok_or(CommitError::Context("conflict path count"))?;
    let mut total =
        u64::try_from(MAGIC.len()).map_err(|_| CommitError::Context("conflict path length"))?;
    for path in paths {
        let length =
            u64::try_from(path.len()).map_err(|_| CommitError::Context("conflict path length"))?;
        total = total
            .checked_add(8)
            .and_then(|n| n.checked_add(length))
            .ok_or(CommitError::Context("conflict path length"))?;
    }
    if i64::try_from(total).is_err() || i64::try_from(count).is_err() {
        return Err(CommitError::Context("conflict path length"));
    }
    let mut page = Vec::with_capacity(CONFLICT_PAGE_BYTES);
    let mut number = 0_u64;
    let mut digest = [0_u8; 32];
    fn add(
        mut bytes: &[u8],
        page: &mut Vec<u8>,
        number: &mut u64,
        digest: &mut [u8; 32],
        emit: &mut impl FnMut(u64, Vec<u8>) -> Result<(), CommitError>,
    ) -> Result<(), CommitError> {
        while !bytes.is_empty() {
            let count = (CONFLICT_PAGE_BYTES - page.len()).min(bytes.len());
            page.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if page.len() == CONFLICT_PAGE_BYTES {
                *digest = conflict_page_digest(*digest, *number, page)
                    .map_err(|_| CommitError::Context("conflict page digest"))?;
                let full = std::mem::replace(page, Vec::with_capacity(CONFLICT_PAGE_BYTES));
                emit(*number, full)?;
                *number = number
                    .checked_add(1)
                    .ok_or(CommitError::Context("conflict page count"))?;
            }
        }
        Ok(())
    }
    add(MAGIC, &mut page, &mut number, &mut digest, &mut emit)?;
    for path in paths {
        let len =
            u64::try_from(path.len()).map_err(|_| CommitError::Context("conflict path length"))?;
        add(
            &len.to_be_bytes(),
            &mut page,
            &mut number,
            &mut digest,
            &mut emit,
        )?;
        add(
            path.as_bytes(),
            &mut page,
            &mut number,
            &mut digest,
            &mut emit,
        )?;
    }
    if !page.is_empty() {
        digest = conflict_page_digest(digest, number, &page)
            .map_err(|_| CommitError::Context("conflict page digest"))?;
        emit(number, page)?;
    }
    Ok(ConflictDescription {
        paths: count,
        bytes: total,
        digest,
    })
}

/// Rebuilds paths from bounded pages only after verifying their original order,
/// exact lengths and chain. A declared path length never causes a reservation.
pub(super) struct ConflictDecoder {
    expected: ConflictDescription,
    received: u64,
    page: u64,
    digest: [u8; 32],
    magic: usize,
    length: [u8; 8],
    length_bytes: usize,
    remaining: Option<u64>,
    current: Vec<u8>,
    paths: Vec<String>,
}

impl ConflictDecoder {
    pub(super) fn new(expected: ConflictDescription) -> Result<Self, &'static str> {
        if expected.paths == 0
            || expected.bytes < MAGIC.len() as u64
            || i64::try_from(expected.bytes).is_err()
            || i64::try_from(expected.paths).is_err()
        {
            return Err("conflict description size");
        }
        Ok(Self {
            expected,
            received: 0,
            page: 0,
            digest: [0; 32],
            magic: 0,
            length: [0; 8],
            length_bytes: 0,
            remaining: None,
            current: Vec::new(),
            paths: Vec::new(),
        })
    }

    pub(super) fn push(&mut self, number: u64, data: &[u8]) -> Result<(), &'static str> {
        let left = self
            .expected
            .bytes
            .checked_sub(self.received)
            .ok_or("conflict byte count")?;
        let wanted = left.min(CONFLICT_PAGE_BYTES as u64);
        if number != self.page || wanted == 0 || usize::try_from(wanted).ok() != Some(data.len()) {
            return Err("conflict page order or size");
        }
        let digest =
            conflict_page_digest(self.digest, number, data).map_err(|_| "conflict page digest")?;
        let mut offset = 0_usize;
        while offset < data.len() {
            if self.magic < MAGIC.len() {
                let n = (MAGIC.len() - self.magic).min(data.len() - offset);
                if data[offset..offset + n] != MAGIC[self.magic..self.magic + n] {
                    return Err("conflict path encoding");
                }
                self.magic += n;
                offset += n;
                continue;
            }
            if self.paths.len() as u64 >= self.expected.paths {
                return Err("conflict trailing bytes");
            }
            if let Some(remaining) = self.remaining {
                let n = usize::try_from(remaining.min((data.len() - offset) as u64))
                    .map_err(|_| "conflict path length")?;
                self.current.extend_from_slice(&data[offset..offset + n]);
                offset += n;
                let rest = remaining - n as u64;
                if rest == 0 {
                    let bytes = std::mem::take(&mut self.current);
                    self.paths
                        .push(String::from_utf8(bytes).map_err(|_| "conflict path UTF-8")?);
                    self.remaining = None;
                } else {
                    self.remaining = Some(rest);
                }
                continue;
            }
            let n = (8 - self.length_bytes).min(data.len() - offset);
            self.length[self.length_bytes..self.length_bytes + n]
                .copy_from_slice(&data[offset..offset + n]);
            self.length_bytes += n;
            offset += n;
            if self.length_bytes == 8 {
                let length = u64::from_be_bytes(self.length);
                self.length_bytes = 0;
                let consumed = self
                    .received
                    .checked_add(offset as u64)
                    .ok_or("conflict byte count")?;
                if length
                    > self
                        .expected
                        .bytes
                        .checked_sub(consumed)
                        .ok_or("conflict byte count")?
                {
                    return Err("conflict path length");
                }
                if length == 0 {
                    self.paths.push(String::new());
                } else {
                    self.remaining = Some(length);
                }
            }
        }
        self.received = self
            .received
            .checked_add(data.len() as u64)
            .ok_or("conflict byte count")?;
        self.page = self.page.checked_add(1).ok_or("conflict page count")?;
        self.digest = digest;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Vec<String>, &'static str> {
        if self.received != self.expected.bytes
            || self.digest != self.expected.digest
            || self.magic != MAGIC.len()
            || self.length_bytes != 0
            || self.remaining.is_some()
            || !self.current.is_empty()
            || self.paths.len() as u64 != self.expected.paths
        {
            return Err("conflict path completion");
        }
        Ok(self.paths)
    }
}
