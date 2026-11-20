//! Reclaim one original staged conflict page per maintenance transaction.
use crate::{
    maintenance::Item, Overlay, OverlayError, OverlayResult, StatementKind, CONFLICT_PAGE_BYTES,
};

impl Overlay {
    pub(crate) fn clean_conflict_pages(&self, item: &Item) -> OverlayResult<(u64, u64, bool)> {
        if self.profile().schema_version != 44
            || item.resource != 0
            || item.target <= 0
            || item.cursor < 0
            || !matches!(item.phase, 0 | 1)
        {
            return Err(OverlayError::Invalid("conflict cleanup item"));
        }
        let generation = item.target;
        let rows:Vec<(Vec<u8>,i64,i64,i64,i64,bool)>=self.query(StatementKind::Reclaim,
            "SELECT attempt,next_page,written_bytes,page_count,byte_count,EXISTS(SELECT 1 FROM durable_binding WHERE conflict_generation=?1) FROM durable_conflict_plan WHERE generation=?1 AND state=2",
            &[&generation],80,|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
        let [(attempt, next, written, pages, bytes, false)] = rows.as_slice() else {
            return Err(OverlayError::Invalid("conflict cleanup predecessor"));
        };
        if attempt.len() != 32
            || *pages <= 0
            || *bytes <= 0
            || *pages != 1 + (*bytes - 1) / CONFLICT_PAGE_BYTES as i64
            || *next < 0
            || *next > *pages
            || item.cursor > *next
        {
            return Err(OverlayError::Invalid("conflict cleanup progress"));
        }
        let expected_written = if next == pages {
            *bytes
        } else {
            next.checked_mul(CONFLICT_PAGE_BYTES as i64)
                .ok_or(OverlayError::Invalid("conflict cleanup progress"))?
        };
        if *written != expected_written {
            return Err(OverlayError::Invalid("conflict cleanup progress"));
        }
        if item.phase == 0 {
            if item.aux != -1 || !item.name.is_empty() {
                return Err(OverlayError::Invalid("conflict cleanup item"));
            }
            let pending:Vec<(i64,Option<i64>,Option<i64>)>=self.query(StatementKind::Reclaim,
                "SELECT phase,conflict_generation,conflict_cleanup_completed FROM durable_checkpoint_attempt WHERE id=1 AND attempt=?1 AND generation=?2",
                &[&attempt,&generation],24,|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
            if pending.as_slice() != [(3, Some(generation), Some(0))] {
                return Err(OverlayError::Invalid("conflict cleanup owner"));
            }
        } else {
            crate::database::durable_conflict_stage::check_retired_active(
                self, generation, item.aux, &item.name,
            )?;
        }
        let stale: Vec<bool> = self.query(
            StatementKind::Reclaim,
            "SELECT EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation=?1 AND page_no<?2)",
            &[&generation, &item.cursor],
            8,
            |r| r.get(0),
        )?;
        if stale.as_slice() != [false] {
            return Err(OverlayError::Invalid("conflict cleanup cursor"));
        }
        if item.cursor < *next {
            let size: Vec<i64> = self.query(
                StatementKind::Reclaim,
                "SELECT length(data) FROM durable_conflict_page WHERE generation=?1 AND page_no=?2",
                &[&generation, &item.cursor],
                16,
                |r| r.get(0),
            )?;
            let offset = item
                .cursor
                .checked_mul(CONFLICT_PAGE_BYTES as i64)
                .ok_or(OverlayError::Invalid("conflict cleanup progress"))?;
            let expected = if item.cursor + 1 < *pages {
                CONFLICT_PAGE_BYTES as i64
            } else {
                *bytes - offset
            };
            if size.as_slice() != [expected]
                || expected <= 0
                || expected > CONFLICT_PAGE_BYTES as i64
            {
                return Err(OverlayError::Invalid("conflict cleanup page"));
            }
            if self.execute(
                StatementKind::Reclaim,
                "DELETE FROM durable_conflict_page WHERE generation=?1 AND page_no=?2",
                &[&generation, &item.cursor],
                16,
            )? != 1
            {
                return Err(OverlayError::Invalid("conflict cleanup page"));
            }
            self.advance_item(item, item.phase, item.cursor + 1, item.aux, &item.name)?;
            return Ok((1, expected as u64, false));
        }
        let extra: Vec<bool> = self.query(
            StatementKind::Reclaim,
            "SELECT EXISTS(SELECT 1 FROM durable_conflict_page WHERE generation=?1)",
            &[&generation],
            8,
            |r| r.get(0),
        )?;
        if extra.as_slice() != [false] {
            return Err(OverlayError::Invalid("conflict cleanup extra page"));
        }
        if self.execute(
            StatementKind::Reclaim,
            "DELETE FROM durable_conflict_plan WHERE generation=?1 AND attempt=?2 AND state=2",
            &[&generation, &attempt],
            40,
        )? != 1
        {
            return Err(OverlayError::Invalid("conflict cleanup predecessor"));
        }
        if item.phase == 0 {
            if self.execute(StatementKind::Frontier,
                "UPDATE durable_checkpoint_attempt SET conflict_cleanup_completed=1 WHERE id=1 AND attempt=?1 AND generation=?2 AND phase=3 AND conflict_generation=?2 AND conflict_cleanup_completed=0",
                &[&attempt,&generation],40)?!=1 {
                return Err(OverlayError::Invalid("conflict cleanup owner"));
            }
        }
        self.finish_item(item)?;
        Ok((0, 0, true))
    }
}
