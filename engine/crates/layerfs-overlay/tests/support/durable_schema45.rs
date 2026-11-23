//! A test-owned empty extension can be removed to exercise old fault fixtures.
use std::path::Path;

pub fn convert(path: &Path) {
    let mut sql = rusqlite::Connection::open(path).unwrap();
    let tx = sql.transaction().unwrap();
    assert_eq!(
        tx.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        46
    );
    assert_eq!(
        tx.query_row("SELECT count(*) FROM durable_external_capture", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        0
    );
    tx.execute_batch("DROP TABLE durable_external_capture; PRAGMA user_version=45;")
        .unwrap();
    let mut statement = tx
        .prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
        .unwrap();
    let rows: Vec<(String, String, String, Option<String>)> = statement
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    drop(statement);
    assert_eq!(rows.len(), 49);
    let mut hash = blake3::Hasher::new();
    hash.update(b"layerfs:durable-schema:v1\0");
    for (kind, name, table, sql) in rows {
        for value in [&kind, &name, &table] {
            hash.update(&(value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        match sql {
            Some(value) => {
                hash.update(&[1]);
                hash.update(&(value.len() as u64).to_be_bytes());
                hash.update(value.as_bytes());
            }
            None => {
                hash.update(&[0]);
            }
        }
    }
    assert_eq!(
        hash.finalize().to_hex().as_str(),
        "5549263837481e14f268f9544984f32d113f9eeaaac1d80587c931fcc2fdd53e"
    );
    tx.commit().unwrap();
}
