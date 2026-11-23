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
