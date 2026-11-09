//! Read-only verification of an externally fenced SDK close attempt.
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
fn retained_pre_sync_reply_matches_the_original_close_intent() {
    let directory = std::env::var_os("LAYERFS_CLOSE_PROOF").expect("explicit proof directory");
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
                name.starts_with("close-intent-")
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
    assert_eq!(initial.get_text("phase"), Some("beforeUnmount"));
    assert_eq!(replacement.get_text("phase"), Some("closed"));
    assert_eq!(bytes(&initial, "sandbox"), bytes(&replacement, "sandbox"));
    assert_eq!(
        bytes(&initial, "workspace"),
        bytes(&replacement, "workspace")
    );
    assert_eq!(
        initial.get_int("namespace"),
        replacement.get_int("namespace")
    );
    assert_eq!(initial.get_text("path"), replacement.get_text("path"));
    let answer = Answer::decode(bytes(&replacement, "answer")).unwrap();
    let Reply::Unmounted(token) = answer.reply else {
        panic!("the unsynced replacement did not preserve Unmounted");
    };
    assert_eq!(answer.id, 1);
    assert_eq!(token.workspace.as_slice(), bytes(&initial, "workspace"));
    assert_eq!(Some(token.namespace), initial.get_int("namespace"));
    let workspace = format!(
        "workspace-{}.record",
        layerfs_bridge::sdk_values::hex(token.workspace.as_slice())
    );
    assert!(directory.join(workspace).is_file());
    println!("original close intent and exact Unmounted reply retained before reply sync");
}
