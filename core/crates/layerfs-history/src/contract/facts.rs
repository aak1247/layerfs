//! Stable encoding of persisted merge decisions and units.
use crate::error::{HistoryError, HistoryResult};
use layerfs_content::merge::{LwwDecision, MergeUnit};

/// Marks the merge units after the LWW decisions: no path is that long, so
/// a blob written before units reads as having none (#352 F-057).
const UNITS: [u8; 4] = [0xFF; 4];
fn put(out: &mut Vec<u8>, count: usize) -> HistoryResult<()> {
    let count = u32::try_from(count)
        .ok()
        .filter(|count| *count != u32::MAX)
        .ok_or(HistoryError::Capacity("history fact text"))?;
    out.extend_from_slice(&count.to_be_bytes());
    Ok(())
}
/// Encodes the complete history merge facts in their existing stable representation.
pub fn encode(values: &[LwwDecision], units: &[MergeUnit]) -> HistoryResult<Vec<u8>> {
    let mut out = Vec::new();
    for value in values {
        for text in [&value.path, &value.rule] {
            put(&mut out, text.len())?;
            out.extend_from_slice(text.as_bytes());
        }
        out.push(u8::from(value.kept_ours) | (u8::from(value.deleted) << 1));
    }
    if !units.is_empty() {
        out.extend_from_slice(&UNITS);
        put(&mut out, units.len())?;
        for unit in units {
            put(&mut out, unit.name.len())?;
            out.extend_from_slice(unit.name.as_bytes());
            put(&mut out, unit.patterns.len())?;
            for pattern in &unit.patterns {
                put(&mut out, pattern.len())?;
                out.extend_from_slice(pattern.as_bytes());
            }
        }
    }
    Ok(out)
}
/// Decodes history merge facts written by the existing representation.
pub fn decode(mut bytes: &[u8]) -> HistoryResult<(Vec<LwwDecision>, Vec<MergeUnit>)> {
    fn count(bytes: &mut &[u8]) -> HistoryResult<usize> {
        let count = bytes
            .get(..4)
            .ok_or(HistoryError::Integrity("history fact length"))?;
        let count = u32::from_be_bytes(
            count
                .try_into()
                .map_err(|_| HistoryError::Integrity("history fact length"))?,
        ) as usize;
        *bytes = &bytes[4..];
        Ok(count)
    }
    fn text(bytes: &mut &[u8]) -> HistoryResult<String> {
        let count = bytes
            .get(..4)
            .ok_or(HistoryError::Integrity("history fact length"))?;
        let length = u32::from_be_bytes(
            count
                .try_into()
                .map_err(|_| HistoryError::Integrity("history fact length"))?,
        ) as usize;
        *bytes = &bytes[4..];
        let text = bytes
            .get(..length)
            .ok_or(HistoryError::Integrity("history fact text"))?;
        let out = std::str::from_utf8(text)
            .map_err(|_| HistoryError::Integrity("history fact UTF-8"))?
            .to_owned();
        *bytes = &bytes[length..];
        Ok(out)
    }
    let mut out = Vec::new();
    let mut units = Vec::new();
    while !bytes.is_empty() {
        if bytes.starts_with(&UNITS) {
            bytes = &bytes[4..];
            for _ in 0..count(&mut bytes)? {
                let name = text(&mut bytes)?;
                let patterns = (0..count(&mut bytes)?)
                    .map(|_| text(&mut bytes))
                    .collect::<HistoryResult<_>>()?;
                units.push(MergeUnit { name, patterns });
            }
            if !bytes.is_empty() || units.is_empty() {
                return Err(HistoryError::Integrity("history fact units"));
            }
            break;
        }
        let path = text(&mut bytes)?;
        let rule = text(&mut bytes)?;
        let flags = *bytes
            .first()
            .ok_or(HistoryError::Integrity("history fact flags"))?;
        if flags > 3 {
            return Err(HistoryError::Integrity("history fact flags"));
        }
        bytes = &bytes[1..];
        out.push(LwwDecision {
            path,
            rule,
            kept_ours: flags & 1 != 0,
            deleted: flags & 2 != 0,
        });
    }
    Ok((out, units))
}
