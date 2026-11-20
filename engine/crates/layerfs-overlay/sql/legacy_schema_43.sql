-- Exact schema created by SDK 6e5c6003e0fcbd8e0569ef3b0788e4ddb0d7539a.
CREATE TABLE cleanup_task (
    kind INTEGER NOT NULL CHECK(kind BETWEEN 1 AND 12),
    resource INTEGER NOT NULL CHECK(resource>=0),
    target INTEGER NOT NULL CHECK(target<>0),
    phase INTEGER NOT NULL DEFAULT 0 CHECK(phase>=0),
    cursor INTEGER NOT NULL DEFAULT 0 CHECK(cursor>=-1),
    aux INTEGER NOT NULL DEFAULT -1 CHECK(aux>=-1),
    name BLOB NOT NULL DEFAULT X'' CHECK(length(name)<=255),
    ready INTEGER NOT NULL DEFAULT 1 CHECK(ready IN(0,1)),
    PRIMARY KEY(kind,resource,target)
) STRICT, WITHOUT ROWID;
CREATE TABLE commit_record (
    operation INTEGER NOT NULL CHECK(operation>0),
    file_scope BLOB NOT NULL CHECK(length(file_scope)=8),
    kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 4294967295),
    key BLOB NOT NULL CHECK(length(key)=32),
    value BLOB NOT NULL CHECK(length(value)<=65536),
    PRIMARY KEY(operation,file_scope,kind,key)
) STRICT, WITHOUT ROWID;
CREATE TABLE durable_binding(id INTEGER PRIMARY KEY CHECK(id=1), root BLOB NOT NULL CHECK(length(root)=32), snapshot BLOB NOT NULL CHECK(length(snapshot)>0 AND length(snapshot)<=4096), recoverable INTEGER NOT NULL CHECK(recoverable IN(0,1))) STRICT;
CREATE TABLE durable_checkpoint_attempt(id INTEGER PRIMARY KEY CHECK(id=1), attempt BLOB NOT NULL UNIQUE CHECK(length(attempt)=32), workspace BLOB NOT NULL CHECK(length(workspace)=32), catalog BLOB NOT NULL CHECK(length(catalog)=32), incarnation INTEGER NOT NULL CHECK(incarnation>0), namespace INTEGER NOT NULL CHECK(namespace>0), generation INTEGER NOT NULL CHECK(generation>0), revision INTEGER NOT NULL CHECK(revision>=0), base_root BLOB NOT NULL CHECK(length(base_root)=32), snapshot BLOB NOT NULL CHECK(length(snapshot)>0 AND length(snapshot)<=4096), phase INTEGER NOT NULL CHECK(phase IN(1,2,3,4)), request BLOB, request_digest BLOB, candidate_root BLOB, target_snapshot BLOB, outcome_kind INTEGER, outcome_head BLOB, outcome_displaced BLOB, installed_snapshot BLOB, reader_owner INTEGER CHECK(reader_owner>0), reader_request INTEGER, reader_floor INTEGER, reader_released INTEGER, operation_owner INTEGER CHECK(operation_owner>0), operation_released INTEGER, operation_cleanup_completed INTEGER, fold_completed INTEGER, retire_completed INTEGER, CHECK((reader_owner IS NULL AND reader_request IS NULL AND reader_floor IS NULL AND reader_released IS NULL) OR (reader_owner IS NOT NULL AND reader_request IS NOT NULL AND reader_request=generation AND reader_floor IS NOT NULL AND reader_floor>=0 AND reader_floor<generation AND reader_released IS NOT NULL AND reader_released IN(0,1))), CHECK((operation_owner IS NULL AND operation_released IS NULL AND operation_cleanup_completed IS NULL) OR (operation_owner IS NOT NULL AND operation_released IS NOT NULL AND operation_released IN(0,1) AND operation_cleanup_completed IS NOT NULL AND operation_cleanup_completed IN(0,1) AND (operation_cleanup_completed=0 OR operation_released=1) AND reader_owner IS NOT NULL)), CHECK((phase IN(1,2) AND fold_completed IS NULL AND retire_completed IS NULL) OR (phase=3 AND fold_completed IS NOT NULL AND fold_completed IN(0,1) AND retire_completed IS NOT NULL AND retire_completed IN(0,1) AND (retire_completed=0 OR fold_completed=1)) OR (phase=4 AND fold_completed IS NULL AND retire_completed IS NOT NULL AND retire_completed IN(0,1))), CHECK((phase IN(1,3) AND request IS NULL AND request_digest IS NULL AND candidate_root IS NULL AND target_snapshot IS NULL AND outcome_kind IS NULL AND outcome_head IS NULL AND outcome_displaced IS NULL AND installed_snapshot IS NULL) OR (phase=2 AND request IS NOT NULL AND request_digest IS NOT NULL AND length(request_digest)=32 AND candidate_root IS NOT NULL AND length(candidate_root)=32 AND target_snapshot IS NOT NULL AND length(target_snapshot)>0 AND length(target_snapshot)<=4096 AND outcome_kind IS NULL AND outcome_head IS NULL AND outcome_displaced IS NULL AND installed_snapshot IS NULL) OR (phase=4 AND request IS NOT NULL AND request_digest IS NOT NULL AND length(request_digest)=32 AND candidate_root IS NOT NULL AND length(candidate_root)=32 AND target_snapshot IS NOT NULL AND length(target_snapshot)>0 AND length(target_snapshot)<=4096 AND installed_snapshot IS NOT NULL AND length(installed_snapshot)>0 AND length(installed_snapshot)<=4096 AND outcome_kind IS NOT NULL AND ((outcome_kind=1 AND outcome_head IS NOT NULL AND length(outcome_head)=33 AND (outcome_displaced IS NULL OR length(outcome_displaced)=33)) OR (outcome_kind=2 AND (outcome_head IS NULL OR length(outcome_head)=33) AND outcome_displaced IS NULL))))) STRICT;
CREATE TABLE durable_custody(id INTEGER PRIMARY KEY CHECK(id=1), orphan_owners INTEGER NOT NULL CHECK(orphan_owners IN(0,1)), operation_owners INTEGER NOT NULL CHECK(operation_owners IN(0,1)), kept_count INTEGER NOT NULL CHECK(kept_count>=0)) STRICT;
CREATE TABLE durable_identity(id INTEGER PRIMARY KEY CHECK(id=1), identity BLOB NOT NULL CHECK(length(identity)=32)) STRICT;
CREATE TABLE durable_kept_live(serial INTEGER PRIMARY KEY CHECK(serial>0), generation INTEGER NOT NULL CHECK(generation>0)) STRICT;
CREATE TABLE durable_operation_owner(owner INTEGER PRIMARY KEY CHECK(owner>0), namespace INTEGER NOT NULL CHECK(namespace>=0), request INTEGER NOT NULL CHECK(request>0), purpose INTEGER NOT NULL CHECK(purpose IN(0,1,2)), capture_generation INTEGER, capture_revision INTEGER, capture_root BLOB, capture_floor INTEGER, reader_owner INTEGER, UNIQUE(namespace,request), CHECK(namespace>0 OR purpose=0), CHECK((purpose=2 AND capture_generation IS NOT NULL AND capture_generation>0 AND capture_revision IS NOT NULL AND capture_revision>=0 AND capture_root IS NOT NULL AND length(capture_root)=32 AND capture_floor IS NOT NULL AND capture_floor>=0 AND reader_owner IS NOT NULL AND reader_owner>0) OR (purpose<>2 AND capture_generation IS NULL AND capture_revision IS NULL AND capture_root IS NULL AND capture_floor IS NULL AND reader_owner IS NULL))) STRICT;
CREATE TABLE durable_orphan_owner(serial INTEGER PRIMARY KEY CHECK(serial>0), root BLOB NOT NULL CHECK(length(root)=32), top INTEGER NOT NULL CHECK(top>=0), floor INTEGER NOT NULL CHECK(floor>=0 AND top>=floor)) STRICT;
CREATE TABLE durable_owner_counts(id INTEGER PRIMARY KEY CHECK(id=1), operation_count INTEGER NOT NULL CHECK(operation_count>=0), orphan_count INTEGER NOT NULL CHECK(orphan_count>=0)) STRICT;
CREATE TABLE durable_route(id INTEGER PRIMARY KEY CHECK(id=1), high_namespace INTEGER NOT NULL CHECK(high_namespace>0)) STRICT;
CREATE TABLE durable_state(id INTEGER PRIMARY KEY CHECK(id=1), active INTEGER NOT NULL CHECK(active>0),captured INTEGER CHECK(captured>0), captured_revision INTEGER CHECK(captured_revision>=0),installed INTEGER NOT NULL CHECK(installed>=0), revision INTEGER NOT NULL CHECK(revision>=0),base_root BLOB NOT NULL CHECK(length(base_root)=32),dirty_inodes INTEGER NOT NULL CHECK(dirty_inodes>=0),dirty_entries INTEGER NOT NULL CHECK(dirty_entries>=0),closed INTEGER NOT NULL CHECK(closed IN(0,1)), consolidating INTEGER CHECK(consolidating>0),volatile_owners INTEGER NOT NULL CHECK(volatile_owners IN(0,1)),CHECK((captured IS NULL)=(captured_revision IS NULL)), CHECK(active>installed)) STRICT;
CREATE TABLE fs_data (
    rowid INTEGER PRIMARY KEY,
    inode INTEGER NOT NULL CHECK(inode>0),
    generation INTEGER NOT NULL CHECK(generation=-1 OR generation>0),
    cell_offset INTEGER NOT NULL CHECK(cell_offset>=0 AND cell_offset%4096=0),
    epoch INTEGER NOT NULL CHECK(epoch>=0),
    -- One cell at most, or a dense run of whole cells inside one aligned 32768-byte slot.
    data BLOB NOT NULL CHECK(length(data) BETWEEN 1 AND 32768 AND (length(data)<=4096 OR
        (validity IS NULL AND length(data)%4096=0 AND cell_offset/32768=(cell_offset+length(data)-1)/32768))),
    validity BLOB CHECK(validity IS NULL OR length(validity)=(length(data)+7)/8),
    UNIQUE(inode,generation,cell_offset)
) STRICT;
CREATE TABLE fs_data_ref (
    generation INTEGER NOT NULL CHECK(generation=-1 OR generation>0),
    inode INTEGER NOT NULL CHECK(inode>0),
    file_offset INTEGER NOT NULL CHECK(file_offset>=0 AND file_offset%4096=0),
    length INTEGER NOT NULL CHECK(length>0),
    epoch INTEGER NOT NULL CHECK(epoch>=0),
    source BLOB NOT NULL CHECK(length(source)=32),
    source_offset INTEGER NOT NULL CHECK(source_offset>=0),
    PRIMARY KEY(generation,inode,file_offset)
) STRICT, WITHOUT ROWID;
CREATE TABLE fs_digest (
    inode INTEGER PRIMARY KEY CHECK(inode>0),
    sequence INTEGER NOT NULL CHECK(sequence>0),
    length INTEGER NOT NULL CHECK(length>=0),
    digest BLOB NOT NULL CHECK(length(digest)=32)
) STRICT;
CREATE TABLE fs_entry (
    parent INTEGER NOT NULL CHECK(parent>0),
    name BLOB NOT NULL CHECK(length(name)>0 AND length(name)<=255),
    generation INTEGER NOT NULL CHECK(generation>0),
    inode INTEGER CHECK(inode>0),
    inherited INTEGER NOT NULL CHECK(inherited IN(0,1)),
    PRIMARY KEY(parent,name,generation)
) STRICT, WITHOUT ROWID;
CREATE TABLE fs_inode (
    inode INTEGER NOT NULL CHECK(inode>0),
    generation INTEGER NOT NULL CHECK(generation=-1 OR generation>0),
    kind INTEGER NOT NULL CHECK(kind BETWEEN 1 AND 3),
    mode INTEGER NOT NULL CHECK(mode>=0 AND ((kind=1 AND mode<=511) OR (kind=2 AND mode<=1023) OR (kind=3 AND mode=511))),
    mtime_seconds INTEGER NOT NULL,
    mtime_nanoseconds INTEGER NOT NULL CHECK(mtime_nanoseconds>=0 AND mtime_nanoseconds<1000000000),
    nlink INTEGER NOT NULL CHECK(nlink>=0),
    size INTEGER NOT NULL CHECK(size>=0),
    inherited_cutoff INTEGER NOT NULL CHECK(inherited_cutoff>=0),
    born INTEGER NOT NULL CHECK(born>=0 AND (generation=-1 OR born<=generation)),
    entries INTEGER NOT NULL CHECK(entries>=0 AND (kind=2 OR entries=0)),
    subdirs INTEGER NOT NULL CHECK(subdirs>=0 AND (kind=2 OR subdirs=0)),
    epoch INTEGER NOT NULL CHECK(epoch>=0),
    height INTEGER NOT NULL CHECK(height>=0 AND height<=epoch),
    -- Whether this layer of the file has ever held a reference piece.
    referenced INTEGER NOT NULL DEFAULT 0 CHECK(referenced IN(0,1)),
    -- Content sequence: the Workspace revision that last wrote this row.
    sequence INTEGER NOT NULL DEFAULT 0 CHECK(sequence>=0),
    PRIMARY KEY(inode,generation)
) STRICT, WITHOUT ROWID;
CREATE TABLE fs_predecessor (
    inode INTEGER PRIMARY KEY CHECK(inode>0),
    predecessor INTEGER NOT NULL CHECK(predecessor>0 AND predecessor<>inode)
) STRICT;
CREATE TABLE fs_truncate (
    inode INTEGER NOT NULL CHECK(inode>0),
    generation INTEGER NOT NULL CHECK(generation=-1 OR generation>0),
    depth INTEGER NOT NULL CHECK(depth>0),
    cell_offset INTEGER NOT NULL CHECK(cell_offset>=0 AND cell_offset%4096=0),
    epoch INTEGER NOT NULL CHECK(epoch>0),
    PRIMARY KEY(inode,generation,depth)
) STRICT, WITHOUT ROWID;
CREATE INDEX cleanup_task_generation ON cleanup_task(target,kind,resource);
CREATE INDEX cleanup_task_ready ON cleanup_task(ready,kind,resource,target);
CREATE INDEX fs_data_generation ON fs_data(generation,inode,cell_offset);
CREATE INDEX fs_entry_capture ON fs_entry(generation,parent,name);
CREATE INDEX fs_inode_capture ON fs_inode(generation,inode);
CREATE INDEX fs_truncate_generation ON fs_truncate(generation,inode,depth);
CREATE TRIGGER durable_checkpoint_attempt_owner_immutable BEFORE UPDATE OF reader_owner,operation_owner ON durable_checkpoint_attempt WHEN (OLD.reader_owner IS NOT NULL AND NEW.reader_owner IS NOT OLD.reader_owner) OR (OLD.operation_owner IS NOT NULL AND NEW.operation_owner IS NOT OLD.operation_owner) BEGIN SELECT RAISE(ABORT,'checkpoint owner identity is immutable'); END;
