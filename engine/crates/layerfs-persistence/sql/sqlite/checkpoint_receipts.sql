CREATE TABLE checkpoint_receipt (
    attempt_id BLOB PRIMARY KEY CHECK(length(attempt_id)=32),
    request_digest BLOB NOT NULL CHECK(length(request_digest)=32),
    branch_id BLOB NOT NULL REFERENCES branch(branch_id)
        CHECK(length(branch_id)=17 AND substr(branch_id,1,1)=X'11'),
    base_layer_id BLOB NOT NULL REFERENCES layer(layer_id)
        CHECK(length(base_layer_id)=33 AND substr(base_layer_id,1,1)=X'32'),
    outcome INTEGER NOT NULL CHECK(outcome IN (1,2)),
    result_commit_id BLOB REFERENCES "commit"(commit_id)
        CHECK(result_commit_id IS NULL OR (length(result_commit_id)=33 AND substr(result_commit_id,1,1)=X'12')),
    root_id BLOB NOT NULL CHECK(length(root_id)=32),
    displaced_id BLOB REFERENCES "commit"(commit_id)
        CHECK(displaced_id IS NULL OR (length(displaced_id)=33 AND substr(displaced_id,1,1)=X'12')),
    lww_truncated INTEGER CHECK(lww_truncated IN (0,1)),
    CHECK((outcome=1 AND result_commit_id IS NOT NULL AND lww_truncated IS NOT NULL)
       OR (outcome=2 AND displaced_id IS NULL AND lww_truncated IS NULL))
) STRICT, WITHOUT ROWID;

CREATE TRIGGER checkpoint_receipt_no_update BEFORE UPDATE ON checkpoint_receipt BEGIN
 SELECT RAISE(ABORT,'checkpoint receipt is immutable');
END;
CREATE TRIGGER checkpoint_receipt_no_delete BEFORE DELETE ON checkpoint_receipt BEGIN
 SELECT RAISE(ABORT,'checkpoint receipt is immutable');
END;
