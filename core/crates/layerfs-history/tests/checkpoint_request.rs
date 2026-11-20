use layerfs_content::{
    merge::{LwwDecision, MergeUnit},
    ObjectId,
};
use layerfs_history::{
    checkpoint_request_digest, decode_checkpoint_request, encode_checkpoint_request, facts,
    BranchId, CommitId, CommitMeta, CommitRequest, CommitVia, HistoryError, LayerId, NodeId,
};

fn branch(value: u8) -> BranchId {
    let mut bytes = [value; 17];
    bytes[0] = 0x11;
    BranchId::from_bytes(bytes).unwrap()
}
fn commit(value: u8) -> CommitId {
    let mut bytes = [value; 33];
    bytes[0] = 0x12;
    CommitId::from_bytes(bytes).unwrap()
}
fn layer(value: u8) -> LayerId {
    let mut bytes = [value; 33];
    bytes[0] = 0x32;
    LayerId::from_bytes(bytes).unwrap()
}
fn object(value: u8) -> ObjectId {
    ObjectId::from_bytes(&[value; 32]).unwrap()
}
fn request() -> CommitRequest {
    CommitRequest {
        branch: branch(1),
        expected_head: Some(commit(2)),
        expected_base: layer(3),
        expected_root: object(4),
        candidate_root: object(5),
        profile: object(6),
        scope: object(7),
        second_parent: Some(NodeId::Layer(layer(8))),
        meta: CommitMeta::new(CommitVia::Workspace),
        force: false,
    }
}

#[test]
fn complete_checkpoint_request_changes_for_each_publication_field() {
    let original = request();
    let bytes = encode_checkpoint_request(&original).unwrap();
    assert_eq!(decode_checkpoint_request(&bytes).unwrap(), original);
    assert_eq!(bytes, encode_checkpoint_request(&original).unwrap());
    let encoded_hex: String = bytes.iter().map(|value| format!("{value:02x}")).collect();
    assert_eq!(
        encoded_hex,
        concat!(
            "4c4643500111010101010101010101010101010101010112020202020202020202020202",
            "020202020202020202020202020202020202020232030303030303030303030303030303",
            "030303030303030303030303030303030304040404040404040404040404040404040404",
            "040404040404040404040404040505050505050505050505050505050505050505050505",
            "050505050505050505060606060606060606060606060606060606060606060606060606",
            "060606060607070707070707070707070707070707070707070707070707070707070707",
            "070132080808080808080808080808080808080808080808080808080808080808080800",
            "010000000000000000000000000000000000000000000000000000000000000000000000",
            "000000000000000000000000000000000000"
        )
    );
    let digest = checkpoint_request_digest(&bytes);
    let changes: &[fn(&mut CommitRequest)] = &[
        |r| r.branch = branch(9),
        |r| r.expected_head = None,
        |r| r.expected_base = layer(9),
        |r| r.expected_root = object(9),
        |r| r.candidate_root = object(9),
        |r| r.profile = object(9),
        |r| r.scope = object(9),
        |r| r.second_parent = None,
        |r| r.meta.branch = Some(branch(9)),
        |r| r.meta.via = CommitVia::Files,
        |r| r.meta.message = Some("checkpoint".into()),
        |r| r.meta.merged_from = Some(NodeId::Commit(commit(9))),
        |r| r.meta.lww_truncated = true,
        |r| {
            r.meta.lww.push(LwwDecision {
                path: "a".into(),
                rule: "r".into(),
                kept_ours: true,
                deleted: false,
            })
        },
        |r| {
            r.meta.units.push(MergeUnit {
                name: "u".into(),
                patterns: vec!["p".into()],
            })
        },
        |r| r.meta.statistics.logical = 1,
        |r| r.meta.statistics.new_canonical = 1,
        |r| r.meta.statistics.new_stored = 1,
        |r| r.meta.statistics.new_metadata = 1,
        |r| r.meta.displaced = Some(commit(9)),
        |r| r.meta.time_ms = 1,
        |r| r.force = true,
    ];
    for change in changes {
        let mut changed = original.clone();
        change(&mut changed);
        let encoded = encode_checkpoint_request(&changed).unwrap();
        assert_ne!(encoded, bytes);
        assert_ne!(checkpoint_request_digest(&encoded), digest);
        assert_eq!(decode_checkpoint_request(&encoded).unwrap(), changed);
    }
}

#[test]
fn checkpoint_decoder_preserves_full_metadata_and_rejects_invalid_input() {
    let mut original = request();
    original.meta.branch = Some(branch(9));
    original.meta.message = Some("checkpoint <测试>\nready".into());
    original.meta.merged_from = Some(NodeId::Commit(commit(9)));
    original.meta.lww.push(LwwDecision {
        path: "path".into(),
        rule: "rule".into(),
        kept_ours: true,
        deleted: true,
    });
    original.meta.units.push(MergeUnit {
        name: "group".into(),
        patterns: vec!["*.rs".into(), "src/**".into()],
    });
    original.meta.lww_truncated = true;
    original.meta.statistics.logical = u64::MAX;
    original.meta.statistics.new_canonical = 17;
    original.meta.statistics.new_stored = 18;
    original.meta.statistics.new_metadata = 19;
    original.meta.displaced = Some(commit(10));
    original.meta.time_ms = -123456789;
    original.force = true;
    let bytes = encode_checkpoint_request(&original).unwrap();
    assert_eq!(decode_checkpoint_request(&bytes).unwrap(), original);
    for end in 0..bytes.len() {
        assert!(
            decode_checkpoint_request(&bytes[..end]).is_err(),
            "truncation at {end}"
        );
    }
    let mut invalid = bytes.clone();
    invalid.push(0);
    assert!(decode_checkpoint_request(&invalid).is_err());
    invalid = bytes.clone();
    invalid[0] ^= 1;
    assert!(decode_checkpoint_request(&invalid).is_err());
    invalid = bytes.clone();
    invalid[5] = 0;
    assert!(decode_checkpoint_request(&invalid).is_err());
    invalid = bytes.clone();
    invalid[22] = 2;
    assert!(decode_checkpoint_request(&invalid).is_err());
    invalid = bytes.clone();
    *invalid.last_mut().unwrap() = 2;
    assert!(decode_checkpoint_request(&invalid).is_err());
    invalid = bytes.clone();
    let offset = bytes
        .windows("checkpoint <测试>\nready".len())
        .position(|value| value == "checkpoint <测试>\nready".as_bytes())
        .unwrap();
    invalid[offset] = 0;
    assert!(decode_checkpoint_request(&invalid).is_err());
    invalid[offset] = 0xff;
    assert_eq!(
        decode_checkpoint_request(&invalid),
        Err(HistoryError::Integrity("checkpoint request message UTF-8"))
    );
}

#[test]
fn merge_facts_refuse_counts_that_cannot_fit_the_encoded_bytes() {
    assert!(facts::decode(&[255, 255, 255, 255, 255, 255, 255, 254]).is_err());
    assert!(
        facts::decode(&[255, 255, 255, 255, 0, 0, 0, 1, 0, 0, 0, 0, 255, 255, 255, 254]).is_err()
    );
}

#[test]
fn merge_facts_keep_the_existing_persisted_representation() {
    let decisions = vec![LwwDecision {
        path: "a".into(),
        rule: "r".into(),
        kept_ours: true,
        deleted: false,
    }];
    let units = vec![MergeUnit {
        name: "u".into(),
        patterns: vec!["p".into()],
    }];
    let expected = vec![
        0, 0, 0, 1, b'a', 0, 0, 0, 1, b'r', 1, 255, 255, 255, 255, 0, 0, 0, 1, 0, 0, 0, 1, b'u', 0,
        0, 0, 1, 0, 0, 0, 1, b'p',
    ];
    assert_eq!(facts::encode(&decisions, &units).unwrap(), expected);
    assert_eq!(facts::decode(&expected).unwrap(), (decisions, units));
}
