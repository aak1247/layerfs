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
