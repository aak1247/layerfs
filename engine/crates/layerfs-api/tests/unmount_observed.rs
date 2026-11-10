//! One authenticated Unmount preserves both the original reply and journal error.
use layerfs_api::{
    internal::Control,
    sandbox::{UnmountAttempt, UnmountMilestone},
    Unmounted,
};
use layerfs_bridge::{
    control::{
        Answer, Call, ControlCode, ControlRefusal, DirtyReport, Reply, Request, UnmountControl,
        WorkspaceToken,
    },
    duplex,
};
use layerfs_history::WorkspaceId;
use std::{
    io,
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

fn token() -> WorkspaceToken {
    WorkspaceToken {
        workspace: WorkspaceId::from_authority([3; 32]).unwrap(),
        namespace: 7,
    }
}

fn pair(reply: Option<Option<Reply>>, discard: bool) -> (Control, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client =
        TcpStream::connect_timeout(&listener.local_addr().unwrap(), Duration::from_secs(2))
            .unwrap();
    let (server, _) = listener.accept().unwrap();
    for socket in [&client, &server] {
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
    }
    let worker = thread::spawn(move || {
        let mut channel =
            duplex::accept(server, &[52; 32], duplex::public_key(&[51; 32]).unwrap()).unwrap();
        if let Some(reply) = reply {
            let call = Call::decode(channel.receive.receive().unwrap()).unwrap();
            assert_eq!(
                call.request,
                Request::Unmount(UnmountControl {
                    token: token(),
                    discard
                })
            );
            if let Some(reply) = reply {
                channel
                    .send
                    .send(&Answer { id: call.id, reply }.encode().unwrap())
                    .unwrap();
                assert!(
                    channel.receive.receive().is_err(),
                    "no operation was replayed"
                );
            }
        } else {
            assert!(channel.receive.receive().is_err(), "no operation was sent");
        }
    });
    let channel =
        duplex::initiate(client, &[51; 32], duplex::public_key(&[52; 32]).unwrap()).unwrap();
    (Control::new(channel), worker)
}

#[test]
fn failed_intent_recording_sends_no_unmount() {
    let (mut control, worker) = pair(None, false);
    let failure = layerfs_api::internal::workspace(&mut control)
        .unmount_observed(token(), false, |at| {
            assert_eq!(at.milestone, UnmountMilestone::BeforeUnmount);
            assert_eq!(at.token, token());
            assert!(!at.discard && at.attempt.is_none());
            Err(io::Error::other("intent sync failed"))
        })
        .unwrap_err();
    assert_eq!(failure.milestone, UnmountMilestone::BeforeUnmount);
    assert!(failure.attempt.is_none());
    assert_eq!(
        failure.observer_error.unwrap().to_string(),
        "intent sync failed"
    );
    drop(control);
    worker.join().unwrap();
}

#[test]
fn failed_receipt_recording_retains_the_original_close_acknowledgement() {
    let (mut control, worker) = pair(Some(Some(Reply::Unmounted(token()))), true);
    let mut seen = Vec::new();
    let failure = layerfs_api::internal::workspace(&mut control)
        .unmount_observed(token(), true, |at| {
            seen.push(at.milestone);
            if at.milestone == UnmountMilestone::Finished {
                assert!(matches!(
                    at.attempt,
                    Some(UnmountAttempt::Acknowledged(Unmounted::Closed))
                ));
                Err(io::Error::other("receipt sync failed"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(
        seen,
        [UnmountMilestone::BeforeUnmount, UnmountMilestone::Finished]
    );
    assert!(matches!(
        failure.attempt,
        Some(UnmountAttempt::Acknowledged(Unmounted::Closed))
    ));
    assert_eq!(
        failure.observer_error.unwrap().to_string(),
        "receipt sync failed"
    );
    drop(control);
    worker.join().unwrap();
}

#[test]
fn dirty_refusal_is_recorded_without_discard_or_replay() {
    let report = DirtyReport {
        token: token(),
        paths: vec!["draft".into()],
        inodes: 1,
        entries: 0,
        pending_merge: false,
        truncated: false,
    };
    let (mut control, worker) = pair(Some(Some(Reply::Dirty(Box::new(report.clone())))), false);
    let mut seen = Vec::new();
    let result = layerfs_api::internal::workspace(&mut control)
        .unmount_observed(token(), false, |at| {
            seen.push(at.milestone);
            if let Some(UnmountAttempt::Acknowledged(Unmounted::Dirty(value))) = at.attempt {
                assert_eq!(value.as_ref(), &report);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(result, Unmounted::Dirty(Box::new(report)));
    assert_eq!(
        seen,
        [UnmountMilestone::BeforeUnmount, UnmountMilestone::Finished]
    );
    drop(control);
    worker.join().unwrap();
}

#[test]
fn remote_refusal_and_lost_reply_keep_the_original_failure() {
    let refusal = ControlRefusal {
        code: ControlCode::Busy,
        phase: "unmount:probe".into(),
        moved: None,
        published: None,
        detail: "original mount retained".into(),
    };
    for reply in [Some(Reply::Refused(refusal)), None] {
        let (mut control, worker) = pair(Some(reply), false);
        let failure = layerfs_api::internal::workspace(&mut control)
            .unmount_observed(token(), false, |at| {
                if at.milestone == UnmountMilestone::Finished {
                    assert!(matches!(at.attempt, Some(UnmountAttempt::Failed(_))));
                    return Err(io::Error::other("failed result sync"));
                }
                Ok(())
            })
            .unwrap_err();
        assert_eq!(failure.milestone, UnmountMilestone::Finished);
        assert!(matches!(failure.attempt, Some(UnmountAttempt::Failed(_))));
        assert_eq!(
            failure.observer_error.unwrap().to_string(),
            "failed result sync"
        );
        drop(control);
        worker.join().unwrap();
    }
}

#[test]
fn wrong_workspace_receipt_never_confirms_close_or_dirty_state() {
    let mut wrong = token();
    wrong.namespace += 1;
    let dirty = DirtyReport {
        token: wrong,
        paths: vec!["other".into()],
        inodes: 1,
        entries: 0,
        pending_merge: false,
        truncated: false,
    };
    for reply in [Reply::Unmounted(wrong), Reply::Dirty(Box::new(dirty))] {
        let (mut control, worker) = pair(Some(Some(reply)), false);
        let failure = layerfs_api::internal::workspace(&mut control)
            .unmount_observed(token(), false, |_| Ok(()))
            .unwrap_err();
        assert!(matches!(failure.attempt, Some(UnmountAttempt::Failed(_))));
        assert!(failure.observer_error.is_none());
        drop(control);
        worker.join().unwrap();
    }
}
