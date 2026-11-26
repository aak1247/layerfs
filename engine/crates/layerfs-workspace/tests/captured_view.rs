//! A nonpublishing reader sees the original base plus its sealed overlay.
mod common;
mod harness;

use common::name;
use harness::*;
use layerfs_content::{filesystem::SymlinkTarget, ContentError};
use layerfs_overlay::MaintenanceCursor;
use layerfs_workspace::{Operation, WorkspaceError};

fn names(view: &layerfs_workspace::CapturedView<'_, layerfs_overlay::Overlay>) -> Vec<String> {
    let mut result = Vec::new();
    let mut after = None;
    loop {
        let page = view.list(1, after.as_deref()).unwrap();
        result.extend(
            page.entries
                .into_iter()
                .map(|(name, _)| String::from_utf8(name).unwrap()),
        );
        match page.continuation {
            Some(next) => {
                assert!(after
                    .as_ref()
                    .is_none_or(|previous: &Vec<u8>| previous < &next));
                after = Some(next);
            }
            None => return result,
        }
    }
}

fn drain(b: &Bench) {
    let mut cursor = MaintenanceCursor::default();
    for _ in 0..2000 {
        match b.overlay.maintain(cursor).unwrap() {
            Some(step) => cursor = step.cursor,
            None => return,
        }
    }
    panic!("original capture maintenance did not finish");
}

#[test]
fn captured_view_stays_on_original_base_and_generation_until_release() {
    let b = Bench::new("captured-view");
    let root = b.fixture.root.0.to_bytes();
    let created = b.applied(create(1, "new"), T1).unwrap().serial;
    b.applied(write(created, 0, b"before"), T1);
    b.applied(link(created, 1, "hard"), T1);
    b.applied(rename((1, "file"), (1, "renamed"), false, None), T1);
    b.applied(unlink(1, "alias"), T1);
    let link = b
        .applied(
            Operation::Symlink {
                parent: 1,
                name: name("snapshot-link"),
                target: SymlinkTarget::new(b"original-target".to_vec()).unwrap(),
            },
            T1,
        )
        .unwrap()
        .serial;
    b.applied(
        Operation::SetAttributes {
            serial: created,
            mode: Some(0o600),
            mtime: Some(T1),
            size: None,
        },
        T1,
    );
    b.applied(write(8, 0, b"old"), T1);

    let source = b.overlay.acquire_base_source(90_001).unwrap();
    let source_view = b.workspace.view_for_source(source).unwrap();
    let capture = b.overlay.capture().unwrap();
    let reader = b.overlay.acquire_captured_reader(capture, 90_002).unwrap();
    let snapshot = source_view.captured(&b.overlay, reader).unwrap();
    assert_eq!(snapshot.root_serial(), 1);
    let original_names = names(&snapshot);
    assert!(original_names.contains(&"new".to_string()));
    assert!(original_names.contains(&"hard".to_string()));
    assert!(original_names.contains(&"renamed".to_string()));
    assert!(!original_names.contains(&"alias".to_string()));
    let stat = snapshot.lookup(1, &name("new")).unwrap();
    assert_eq!(
        (
            stat.serial,
            stat.metadata.mode,
            stat.metadata.mtime_seconds,
            stat.metadata.mtime_nanoseconds
        ),
        (created, 0o600, T1.seconds, T1.nanoseconds)
    );
    assert_eq!(snapshot.lookup(1, &name("hard")).unwrap().serial, created);
    assert_eq!(
        snapshot.readlink(link).unwrap().as_bytes(),
        b"original-target"
    );
    let mut original = Vec::new();
    snapshot.read(created, 0, 64, &mut original).unwrap();
    assert_eq!(original, b"before");
    let mut inherited = Vec::new();
    snapshot.read(8, 0, 16, &mut inherited).unwrap();
    let mut expected = b.fixture.bytes[..16].to_vec();
    expected[..3].copy_from_slice(b"old");
    assert_eq!(inherited, expected);

    // Later published changes modify the original working root, never the
    // retained reader or its base.
    b.applied(write(created, 0, b"after!"), T2);
    b.applied(write(8, 0, b"new"), T2);
    b.applied(unlink(1, "hard"), T2);
    b.applied(rename((1, "new"), (1, "newer"), false, None), T2);
    b.applied(create(1, "later"), T2);
    b.applied(unlink(1, "snapshot-link"), T2);
    b.applied(
        Operation::SetAttributes {
            serial: created,
            mode: Some(0o644),
            mtime: Some(T2),
            size: None,
        },
        T2,
    );
    assert_eq!(names(&snapshot), original_names);
    let mut old = Vec::new();
    snapshot.read(created, 0, 64, &mut old).unwrap();
    assert_eq!(old, b"before");
    let mut old_base = Vec::new();
    snapshot.read(8, 0, 16, &mut old_base).unwrap();
    assert_eq!(old_base, expected);
    assert_eq!(
        snapshot.readlink(link).unwrap().as_bytes(),
        b"original-target"
    );
    assert_eq!(snapshot.stat(created).unwrap().metadata.mode, 0o600);
    assert!(b.lookup(1, "new").is_none());
    assert_eq!(b.lookup(1, "newer").unwrap().serial, created);
    assert_eq!(b.read_at(created, 0, 64), b"after!");
    assert_eq!(b.overlay.state().unwrap().base_root, root);

    // Nonpublication retains original rows while a reader is active. Folding
    // is allowed only after the exact reader and source have been released.
    b.overlay.resolve_failed_capture(capture).unwrap();
    drain(&b);
    assert_eq!(names(&snapshot), original_names);
    drop(snapshot);
    b.overlay.release_captured_reader(reader).unwrap();
    assert!(source_view.captured(&b.overlay, reader).is_err());
    b.overlay.release_base_source(source).unwrap();
    drain(&b);
    assert!(b.overlay.capture_ready().unwrap());
    assert_eq!(b.overlay.state().unwrap().base_root, root);
    assert_eq!(b.read_at(created, 0, 64), b"after!");
}

#[test]
fn captured_view_rejects_another_engine_and_a_released_source() {
    let first = Bench::new("captured-view-first");
    let second = Bench::new("captured-view-second");
    let source = first.overlay.acquire_base_source(91_001).unwrap();
    let view = first.workspace.view_for_source(source).unwrap();
    let capture = first.overlay.capture().unwrap();
    let reader = first
        .overlay
        .acquire_captured_reader(capture, 91_002)
        .unwrap();
    let other = second.overlay.acquire_base_source(91_003).unwrap();
    let other_view = second.workspace.view_for_source(other).unwrap();
    assert!(matches!(
        other_view.captured(&second.overlay, reader),
        Err(WorkspaceError::Overlay(
            layerfs_overlay::OverlayError::Stale
        ))
    ));
    first.overlay.release_base_source(source).unwrap();
    assert!(view.captured(&first.overlay, reader).is_err());
    first.overlay.release_captured_reader(reader).unwrap();
    first.overlay.resolve_failed_capture(capture).unwrap();
    second.overlay.release_base_source(other).unwrap();
    // Verify a normal, missing base name still means logical absence.
    assert!(matches!(
        second.window(|view| view.lookup(&second.overlay, 1, &name("absent"))),
        Err(WorkspaceError::Content(ContentError::PathNotFound))
    ));
}

#[test]
fn snapshot_pin_is_atomic_exact_and_prevents_early_release_or_close() {
    let b = Bench::new("snapshot-pin");
    let file = b.applied(create(1, "pinned"), T1).unwrap().serial;
    b.applied(write(file, 0, b"before"), T1);
    let before = b.overlay.state().unwrap();
    assert!(b.overlay.pin_snapshot(0).is_err());
    assert!(b.overlay.pin_snapshot(u64::MAX).is_err());
    assert_eq!(b.overlay.state().unwrap(), before);

    // An unrelated existing capture refuses before the new attempt changes
    // either the frontier or any of its owners.
    let other = b.overlay.capture().unwrap();
    let held = b.overlay.state().unwrap();
    assert!(b.overlay.pin_snapshot(77_001).is_err());
    assert_eq!(b.overlay.state().unwrap(), held);
    assert!(b.overlay.retained_snapshot_pin(77_001).unwrap().is_none());
    b.overlay.resolve_failed_capture(other).unwrap();
    drain(&b);

    let pin = b.overlay.pin_snapshot(77_001).unwrap();
    let source_view = b.workspace.view_for_source(pin.source()).unwrap();
    let snapshot = source_view.captured(&b.overlay, pin.reader()).unwrap();
    assert_eq!(
        b.overlay.retained_snapshot_pin(pin.request()).unwrap(),
        Some(pin)
    );
    assert!(b.overlay.retained_snapshot_pin(77_002).unwrap().is_none());
    let pinned = b.overlay.state().unwrap();
    assert!(b.overlay.pin_snapshot(77_002).is_err());
    assert!(b
        .overlay
        .acquire_captured_reader(pin.capture(), 77_002)
        .is_err());
    assert!(b.overlay.release_base_source(pin.source()).is_err());
    assert!(b.overlay.release_captured_reader(pin.reader()).is_err());
    assert!(b.overlay.resolve_failed_capture(pin.capture()).is_err());
    assert!(b.overlay.close().is_err());
    assert_eq!(b.overlay.state().unwrap(), pinned);

    b.applied(write(file, 0, b"after!"), T2);
    let mut captured = Vec::new();
    snapshot.read(file, 0, 64, &mut captured).unwrap();
    assert_eq!(captured, b"before");
    assert_eq!(b.read_at(file, 0, 64), b"after!");
    drop(snapshot);
    b.overlay.release_snapshot_pin(pin).unwrap();
    assert!(b
        .overlay
        .retained_snapshot_pin(pin.request())
        .unwrap()
        .is_none());
    assert!(source_view.captured(&b.overlay, pin.reader()).is_err());
    assert!(b.overlay.release_snapshot_pin(pin).is_err());
    drain(&b);
    assert!(b.overlay.capture_ready().unwrap());
    assert_eq!(b.read_at(file, 0, 64), b"after!");
}

#[test]
fn snapshot_pin_rolls_back_frontier_and_source_when_reader_request_collides() {
    let b = Bench::new("snapshot-pin-collision");
    let original = b.overlay.capture().unwrap();
    let reader = b.overlay.acquire_captured_reader(original, 88_001).unwrap();
    // A reader remains independently owned after a known install. It can
    // collide only after the new capture and source have been prepared.
    b.overlay
        .install(original, b.fixture.root.0.to_bytes())
        .unwrap();
    let before = b.overlay.state().unwrap();
    assert!(b.overlay.pin_snapshot(88_001).is_err());
    assert_eq!(b.overlay.state().unwrap(), before);
    assert_eq!(
        b.overlay.retained_captured_reader(88_001).unwrap(),
        Some(reader)
    );
    assert!(b.overlay.retained_snapshot_pin(88_001).unwrap().is_none());
    b.overlay.release_captured_reader(reader).unwrap();
    drain(&b);
    let pin = b.overlay.pin_snapshot(88_002).unwrap();
    b.overlay.release_snapshot_pin(pin).unwrap();
    drain(&b);
    assert!(b.overlay.capture_ready().unwrap());
}
