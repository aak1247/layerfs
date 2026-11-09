//! Read-only verification of an externally fenced SDK mount attempt.
#![cfg(target_os = "linux")]
use layerfs_bridge::control::{Answer, Reply, SdkValue};
use std::{fs, path::Path};

fn record(path: &Path) -> SdkValue {
    match Answer::decode(&fs::read(path).unwrap()).unwrap().reply {
        Reply::Sdk(value) => value,
        other => panic!("unexpected SDK record: {other:?}"),
    }
}

fn bytes<'a>(value: &'a SdkValue, key: &str) -> &'a [u8] {
    match value.get(key) {
        Some(SdkValue::Bytes(value)) => value,
        _ => panic!("missing bytes {key}"),
    }
}

#[test]
#[ignore = "requires an isolated, externally fenced SDK process and its retained test records"]
fn retained_pre_sync_ready_matches_the_original_mount_intent() {
    let directory = std::env::var_os("LAYERFS_MOUNT_PROOF").expect("explicit proof directory");
    let directory = Path::new(&directory);
    let paths = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    let selected = |temporary: bool| {
        paths
            .iter()
            .filter(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                name.starts_with("mount-intent-")
                    && if temporary {
                        name.contains(".record.")
                    } else {
                        name.ends_with(".record")
                    }
            })
            .collect::<Vec<_>>()
    };
    let initial = selected(false);
    let replacement = selected(true);
    assert_eq!(initial.len(), 1);
    assert_eq!(replacement.len(), 1);
    let initial = record(initial[0]);
    let replacement = record(replacement[0]);
    assert_eq!(initial.get_text("phase"), Some("beforeAttach"));
    assert_eq!(replacement.get_text("phase"), Some("attached"));
    for key in ["sandbox", "workspace", "branchId", "bound"] {
        assert_eq!(bytes(&initial, key), bytes(&replacement, key));
    }
    assert_eq!(initial.get_text("branch"), replacement.get_text("branch"));
    let bound = Answer::decode(bytes(&initial, "bound")).unwrap();
    let ready = Answer::decode(bytes(&replacement, "ready")).unwrap();
    assert_eq!(bound.id, 1);
    assert_eq!(ready.id, 1);
    let Reply::Bound { token, binding } = bound.reply else {
        panic!("the original intent did not preserve Bound");
    };
    let Reply::Ready(ready) = ready.reply else {
        panic!("the unsynced replacement did not preserve Ready");
    };
    assert_eq!(token, ready.token);
    assert_eq!(token.workspace.as_slice(), bytes(&initial, "workspace"));
    assert_eq!(binding.branch.id.as_slice(), bytes(&initial, "branchId"));
    assert!(!paths.iter().any(|path| path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("workspace-")));
    println!("original mount intent and exact Ready reply retained before reply sync");
}
