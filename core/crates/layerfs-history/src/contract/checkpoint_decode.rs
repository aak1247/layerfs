//! Strict decoding of the frozen checkpoint request, never a publication call.
use crate::{
    encode_checkpoint_request, facts, BranchId, CommitId, CommitMeta, CommitRequest,
    CommitStatistics, CommitVia, HistoryError, HistoryResult, LayerId, NodeId,
    MAXIMUM_MESSAGE_BYTES,
};
use layerfs_content::ObjectId;

struct Reader<'a> {
    rest: &'a [u8],
}
impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> HistoryResult<&'a [u8]> {
        let bytes = self
            .rest
            .get(..length)
            .ok_or(HistoryError::Integrity("checkpoint request length"))?;
        self.rest = &self.rest[length..];
        Ok(bytes)
    }
    fn byte(&mut self) -> HistoryResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn boolean(&mut self) -> HistoryResult<bool> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(HistoryError::Integrity("checkpoint request boolean")),
        }
    }
    fn option<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> HistoryResult<T>,
    ) -> HistoryResult<Option<T>> {
        if self.boolean()? {
            Ok(Some(read(self)?))
        } else {
            Ok(None)
        }
    }
    fn number(&mut self) -> HistoryResult<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| HistoryError::Integrity("checkpoint request number"))?;
        Ok(u64::from_be_bytes(bytes))
    }
    fn blob(&mut self) -> HistoryResult<&'a [u8]> {
        let length = usize::try_from(self.number()?)
            .map_err(|_| HistoryError::Integrity("checkpoint request length"))?;
        self.take(length)
    }
    fn object(&mut self) -> HistoryResult<ObjectId> {
        ObjectId::from_bytes(self.take(32)?)
            .map_err(|_| HistoryError::Integrity("checkpoint request object"))
    }
    fn branch(&mut self) -> HistoryResult<BranchId> {
        BranchId::from_slice(self.take(17)?)
    }
    fn commit(&mut self) -> HistoryResult<CommitId> {
        CommitId::from_slice(self.take(33)?)
    }
    fn node(&mut self) -> HistoryResult<NodeId> {
        NodeId::from_slice(self.take(33)?)
    }
}

/// Parses a complete canonical request frozen before one checkpoint publication.
/// The decoded data is not a receipt and does not authorize a send or recovery.
pub fn decode_checkpoint_request(bytes: &[u8]) -> HistoryResult<CommitRequest> {
    let mut input = Reader { rest: bytes };
    if input.take(5)? != b"LFCP\x01" {
        return Err(HistoryError::Integrity("checkpoint request version"));
    }
    let branch = input.branch()?;
    let expected_head = input.option(Reader::commit)?;
    let expected_base = LayerId::from_slice(input.take(33)?)?;
    let expected_root = input.object()?;
    let candidate_root = input.object()?;
    let profile = input.object()?;
    let scope = input.object()?;
    let second_parent = input.option(Reader::node)?;
    let meta_branch = input.option(Reader::branch)?;
    let via = CommitVia::from_code(input.byte()?)?;
    let message = input.option(|reader| {
        let text = reader.blob()?;
        if text.len() > MAXIMUM_MESSAGE_BYTES {
            return Err(HistoryError::Integrity("checkpoint request message"));
        }
        Ok(std::str::from_utf8(text)
            .map_err(|_| HistoryError::Integrity("checkpoint request message UTF-8"))?
            .to_owned())
    })?;
    let merged_from = input.option(Reader::node)?;
    let lww_truncated = input.boolean()?;
    let (lww, units) = facts::decode(input.blob()?)?;
    let statistics = CommitStatistics {
        logical: input.number()?,
        new_canonical: input.number()?,
        new_stored: input.number()?,
        new_metadata: input.number()?,
    };
    let displaced = input.option(Reader::commit)?;
    let time_ms = i64::from_be_bytes(
        input
            .take(8)?
            .try_into()
            .map_err(|_| HistoryError::Integrity("checkpoint request time"))?,
    );
    let force = input.boolean()?;
    if !input.rest.is_empty() {
        return Err(HistoryError::Integrity("checkpoint request trailing bytes"));
    }
    let result = CommitRequest {
        branch,
        expected_head,
        expected_base,
        expected_root,
        candidate_root,
        profile,
        scope,
        second_parent,
        meta: CommitMeta {
            branch: meta_branch,
            via,
            message,
            merged_from,
            lww,
            lww_truncated,
            units,
            statistics,
            displaced,
            time_ms,
        },
        force,
    };
    if encode_checkpoint_request(&result)
        .map_err(|_| HistoryError::Integrity("checkpoint request fields"))?
        != bytes
    {
        return Err(HistoryError::Integrity("noncanonical checkpoint request"));
    }
    Ok(result)
}
