//! Captured-root windows remain bounded and correlated on the control channel.
use layerfs_bridge::{control::*, duplex::MAX_PLAINTEXT_BYTES};
use layerfs_history::WorkspaceId;

fn handle() -> CaptureReadHandle {
    CaptureReadHandle {
        id: [1; 32],
        token: WorkspaceToken {
            workspace: WorkspaceId::from_authority([2; 32]).unwrap(),
            namespace: 7,
        },
        receipt_sha256: [3; 32],
    }
}

#[test]
fn captured_read_records_are_bounded_canonical_and_preserve_identity() {
    let handle = handle();
    let read = CaptureReadRequest {
        handle,
        path: "repo-9/src/file".into(),
        offset: 5,
        length: CAPTURE_READ_LIMIT,
    };
    let request = Call {
        id: 1,
        request: Request::ReadCapture(read.clone()),
    };
    let encoded = request.encode().unwrap();
    assert_eq!(Call::decode(&encoded).unwrap(), request);
    for end in 0..encoded.len() {
        assert!(Call::decode(&encoded[..end]).is_err());
    }
    let answer = Answer {
        id: 1,
        reply: Reply::CaptureRead(CaptureReadResult {
            handle,
            offset: read.offset,
            bytes: vec![b'A'; CAPTURE_READ_LIMIT as usize],
        }),
    };
    let encoded_answer = answer.encode().unwrap();
    assert!(encoded_answer.len() <= MAX_PLAINTEXT_BYTES);
    assert_eq!(Answer::decode(&encoded_answer).unwrap(), answer);

    for bad in [
        CaptureReadRequest {
            path: "../outside".into(),
            ..read.clone()
        },
        CaptureReadRequest {
            path: "/absolute".into(),
            ..read.clone()
        },
        CaptureReadRequest {
            length: CAPTURE_READ_LIMIT + 1,
            ..read.clone()
        },
        CaptureReadRequest {
            offset: u64::MAX,
            length: 1,
            ..read.clone()
        },
    ] {
        assert!(Call {
            id: 2,
            request: Request::ReadCapture(bad)
        }
        .encode()
        .is_err());
    }
    assert!(Answer {
        id: 2,
        reply: Reply::CaptureRead(CaptureReadResult {
            handle,
            offset: 0,
            bytes: vec![0; CAPTURE_READ_LIMIT as usize + 1],
        })
    }
    .encode()
    .is_err());
}

#[test]
fn captured_tree_records_validate_paths_pages_and_inode_metadata() {
    let path = CapturePath {
        handle: handle(),
        path: "".into(),
    };
    let request = CaptureList {
        path: path.clone(),
        cursor: None,
        limit: 2,
    };
    let calls = [
        Request::StatCapture(path.clone()),
        Request::ListCapture(request.clone()),
        Request::ReadlinkCapture(CapturePath {
            path: "link".into(),
            ..path.clone()
        }),
    ];
    for request in calls {
        let call = Call { id: 1, request };
        let bytes = call.encode().unwrap();
        assert_eq!(Call::decode(&bytes).unwrap(), call);
        for end in 0..bytes.len() {
            assert!(Call::decode(&bytes[..end]).is_err());
        }
    }
    let stat = CaptureStat {
        path: path.clone(),
        kind: CaptureKind::Directory,
        mode: 0o755,
        mtime_seconds: -12,
        mtime_nanoseconds: 9,
        namespace_references: 0,
        size: 0,
        portable_attributes_only: true,
    };
    let page = CapturePage {
        request,
        names: vec!["a".into(), "b".into()],
        continuation: Some([7; 32]),
    };
    let link = CaptureLink {
        path: CapturePath {
            path: "link".into(),
            ..path.clone()
        },
        target: b"../opaque-\xff".to_vec(),
    };
    for reply in [
        Reply::CaptureStat(stat.clone()),
        Reply::CapturePage(page.clone()),
        Reply::CaptureLink(link),
    ] {
        let answer = Answer { id: 1, reply };
        let bytes = answer.encode().unwrap();
        assert!(bytes.len() <= MAX_PLAINTEXT_BYTES);
        assert_eq!(Answer::decode(&bytes).unwrap(), answer);
    }
    for invalid in [
        CaptureStat {
            namespace_references: 1,
            ..stat.clone()
        },
        CaptureStat {
            path: CapturePath {
                path: "file".into(),
                ..path.clone()
            },
            kind: CaptureKind::File,
            namespace_references: 0,
            ..stat.clone()
        },
        CaptureStat {
            mtime_nanoseconds: 1_000_000_000,
            ..stat.clone()
        },
    ] {
        assert!(Answer {
            id: 2,
            reply: Reply::CaptureStat(invalid)
        }
        .encode()
        .is_err());
    }
    assert!(Answer {
        id: 2,
        reply: Reply::CapturePage(CapturePage {
            names: vec!["b".into(), "a".into()],
            ..page.clone()
        })
    }
    .encode()
    .is_err());
    assert!(Answer {
        id: 2,
        reply: Reply::CapturePage(CapturePage {
            names: vec![],
            continuation: Some([8; 32]),
            ..page
        })
    }
    .encode()
    .is_err());
}
