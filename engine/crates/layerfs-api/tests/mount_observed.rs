//! Exact one-attempt mount boundaries through the real authenticated channel.
use layerfs_api::{
    internal::Control,
    sandbox::{MountCause, MountMilestone},
    BoundWorkspace, MountedWorkspace,
};
use layerfs_bridge::{
    control::{Answer, Call, MountReceipt, ReadyMount, Reply, Request, WorkspaceToken},
    duplex,
};
use layerfs_content::ObjectId;
use layerfs_history::{
    BranchId, BranchRecord, BranchSnapshot, HistoryName, LayerId, LayerStackId, WorkspaceId,
};
use std::{io, net::TcpListener, net::TcpStream, thread, time::Duration};

fn fixture() -> MountedWorkspace {
    let token = WorkspaceToken {
        workspace: WorkspaceId::from_authority([4; 32]).unwrap(),
        namespace: 7,
    };
    let root = ObjectId::for_bytes(b"mount observation root");
    MountedWorkspace {
        bound: BoundWorkspace {
            token,
            binding: BranchSnapshot {
                branch: BranchRecord {
                    id: BranchId::from_authority([1; 16]),
                    stack: LayerStackId::from_authority([2; 16]),
                    name: HistoryName::new("main").unwrap(),
                    base_layer: LayerId::from_bytes([0x32; 33]).unwrap(),
                    head_commit: None,
                    forked_from: None,
                },
                head_root: None,
                base_root: root,
                effective_root: root,
                scope: ObjectId::for_bytes(b"scope"),
                profile: layerfs_content::filesystem::profile_id(),
            },
        },
        ready: ReadyMount {
            token,
            directory: "/workspaces/observed".into(),
            receipt: MountReceipt {
                mount: 2,
                root: 7,
                mount_id: 911,
                device_major: 0,
                device_minor: 64,
                abi_major: 7,
                abi_minor: 41,
                offered: 1,
                selected: 1,
                max_write: 131072,
                max_readahead: 65536,
                max_background: 12,
                congestion_threshold: 9,
                page_size: 4096,
                loops: 2,
                abort_bound: true,
            },
        },
    }
}

fn pair(requests: usize) -> (Control, thread::JoinHandle<()>) {
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
        let mount = fixture();
        for index in 0..requests {
            let call = Call::decode(channel.receive.receive().unwrap()).unwrap();
            let reply = match index {
                0 => {
                    assert_eq!(
                        call.request,
                        Request::Mount {
                            workspace: mount.bound.token.workspace,
                            branch: mount.bound.binding.branch.id,
                        }
                    );
                    Reply::Bound {
                        token: mount.bound.token,
                        binding: mount.bound.binding.clone(),
                    }
                }
                1 => {
                    assert_eq!(call.request, Request::Attach(mount.bound.token));
                    Reply::Ready(Box::new(mount.ready.clone()))
                }
                _ => panic!("unexpected third mount operation"),
            };
            channel
                .send
                .send(&Answer { id: call.id, reply }.encode().unwrap())
                .unwrap();
        }
        assert!(
            channel.receive.receive().is_err(),
            "no later operation was sent"
        );
    });
    let channel =
        duplex::initiate(client, &[51; 32], duplex::public_key(&[52; 32]).unwrap()).unwrap();
    (Control::new(channel), worker)
}

#[test]
fn observer_failure_before_bind_sends_nothing() {
    let (mut control, worker) = pair(0);
    let expected = fixture();
    let failure = layerfs_api::internal::workspace(&mut control)
        .mount_observed(
            expected.bound.token.workspace,
            expected.bound.binding.branch.id,
            |_| Err(io::Error::other("intent sync failed")),
        )
        .unwrap_err();
    assert_eq!(failure.milestone, MountMilestone::BeforeBind);
    assert!(matches!(failure.cause, MountCause::Observer(_)));
    assert!(failure.bound.is_none());
    assert!(failure.ready.is_none());
    drop(control);
    worker.join().unwrap();
}

#[test]
fn observer_failure_after_binding_keeps_original_and_sends_no_attach() {
    for stop in [MountMilestone::Bound, MountMilestone::BeforeAttach] {
        let (mut control, worker) = pair(1);
        let expected = fixture();
        let mut reached = Vec::new();
        let failure = layerfs_api::internal::workspace(&mut control)
            .mount_observed(
                expected.bound.token.workspace,
                expected.bound.binding.branch.id,
                |at| {
                    reached.push(at.milestone);
                    if at.milestone == stop {
                        Err(io::Error::other("binding sync failed"))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
        assert_eq!(failure.milestone, stop);
        assert_eq!(failure.bound, Some(expected.bound));
        assert!(failure.ready.is_none());
        assert_eq!(reached.last(), Some(&stop));
        drop(control);
        worker.join().unwrap();
    }
}

#[test]
fn observer_failure_after_attach_keeps_the_exact_kernel_receipt() {
    let (mut control, worker) = pair(2);
    let expected = fixture();
    let mut reached = Vec::new();
    let failure = layerfs_api::internal::workspace(&mut control)
        .mount_observed(
            expected.bound.token.workspace,
            expected.bound.binding.branch.id,
            |at| {
                reached.push(at.milestone);
                if at.milestone == MountMilestone::Attached {
                    Err(io::Error::other("ready sync failed"))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
    assert_eq!(
        reached,
        [
            MountMilestone::BeforeBind,
            MountMilestone::Bound,
            MountMilestone::BeforeAttach,
            MountMilestone::Attached
        ]
    );
    assert_eq!(failure.bound, Some(expected.bound));
    assert_eq!(failure.ready, Some(expected.ready));
    assert!(matches!(failure.cause, MountCause::Observer(_)));
    drop(control);
    worker.join().unwrap();
}

#[test]
fn acknowledged_mount_returns_the_original_receipts() {
    let (mut control, worker) = pair(2);
    let expected = fixture();
    let mounted = layerfs_api::internal::workspace(&mut control)
        .mount_observed(
            expected.bound.token.workspace,
            expected.bound.binding.branch.id,
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(mounted, expected);
    drop(control);
    worker.join().unwrap();
}
