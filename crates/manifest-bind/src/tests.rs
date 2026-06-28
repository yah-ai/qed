use std::fs;

use tempfile::TempDir;

use crate::intent::{Intent, IntentKeyword};
use crate::types::{BindSpec, OutputMap, OutputRef, OutputValue};
use crate::value_type::ValueType;
use crate::{apply_binds, path_resolver};

fn workload_toml() -> &'static str {
    r#"name = "whisper"
image = "python:3.12-slim"

[[asset]]
filename = "whisper.tar.gz"
blake3 = "0000000000000000000000000000000000000000000000000000000000000000"

[asset.derive.fetch]
url = "https://example.invalid/whisper.tar.gz"
blake3 = "0000000000000000000000000000000000000000000000000000000000000000"
"#
}

const HASH_A: &str =
    "fb0afc9f3d966f5347c6dfd335adab12f1dc8ee6df18cf9e9ff90fe86f0416c0";
const HASH_B: &str =
    "050ffe562134208781dc316181b146a725821fff005fb4ffb6de2a6ada334a9b";

fn write_workload(dir: &TempDir) -> std::path::PathBuf {
    let p = dir.path().join("workload.toml");
    fs::write(&p, workload_toml()).unwrap();
    p
}

#[test]
fn path_get_array_by_key() {
    let doc: toml_edit::DocumentMut = workload_toml().parse().unwrap();
    let v = path_resolver::toml_get(&doc, "asset[filename='whisper.tar.gz'].blake3").unwrap();
    assert_eq!(v.as_deref(), Some(&"0".repeat(64)[..]));
}

/// W212/R518: a bind into the `[asset.derive.lock]` table resolves and writes
/// through the same array-by-key + nested-table path the fetch/asset binds use.
/// Proves the apply-time path resolution the per-service card's lock binds rely
/// on (the table must pre-exist as sentinels — manifest-bind never creates it).
#[test]
fn apply_binds_writes_derive_lock_input_hash() {
    let toml = r#"name = "whisper"

[[asset]]
filename = "whisper.tar.gz"
blake3 = "0000000000000000000000000000000000000000000000000000000000000000"

[asset.derive.fetch]
url = "https://example.invalid/whisper.tar.gz"
blake3 = "0000000000000000000000000000000000000000000000000000000000000000"

[asset.derive.lock]
input_hash = "0000000000000000000000000000000000000000000000000000000000000000"
output_blake3 = "0000000000000000000000000000000000000000000000000000000000000000"
"#;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("workload.toml");
    fs::write(&path, toml).unwrap();

    let mut outputs = OutputMap::new();
    outputs.insert(
        "apply",
        "discovered_input_hash:whisper.tar.gz",
        OutputValue::new(ValueType::Blake3Hex, HASH_A),
    );
    let binds = vec![BindSpec {
        file: "workload.toml".into(),
        path: "asset[filename='whisper.tar.gz'].derive.lock.input_hash".into(),
        from: OutputRef::parse("apply.outputs.discovered_input_hash:whisper.tar.gz").unwrap(),
        intent: Intent::Keyword(IntentKeyword::Latest),
        cross_workspace: false,
        schema: None,
    }];

    let applied = apply_binds(&outputs, &binds, dir.path()).unwrap();
    assert_eq!(applied.len(), 1);
    assert!(applied[0].changed);
    assert_eq!(applied[0].new, HASH_A);

    // Sentinel rewritten on disk under the nested lock table.
    let doc: toml_edit::DocumentMut = fs::read_to_string(&path).unwrap().parse().unwrap();
    let got = path_resolver::toml_get(
        &doc,
        "asset[filename='whisper.tar.gz'].derive.lock.input_hash",
    )
    .unwrap();
    assert_eq!(got.as_deref(), Some(HASH_A));
}

#[test]
fn path_get_nested_table() {
    let doc: toml_edit::DocumentMut = workload_toml().parse().unwrap();
    let v = path_resolver::toml_get(
        &doc,
        "asset[filename='whisper.tar.gz'].derive.fetch.url",
    )
    .unwrap();
    assert_eq!(v.as_deref(), Some("https://example.invalid/whisper.tar.gz"));
}

#[test]
fn path_get_top_level_scalar() {
    let doc: toml_edit::DocumentMut = workload_toml().parse().unwrap();
    let v = path_resolver::toml_get(&doc, "image").unwrap();
    assert_eq!(v.as_deref(), Some("python:3.12-slim"));
}

#[test]
fn intent_pin_rejects() {
    let v = OutputValue::new(ValueType::Blake3Hex, HASH_A);
    assert!(!Intent::Keyword(IntentKeyword::Pin).accepts(&v));
}

#[test]
fn intent_latest_accepts() {
    let v = OutputValue::new(ValueType::Blake3Hex, HASH_A);
    assert!(Intent::Keyword(IntentKeyword::Latest).accepts(&v));
}

#[test]
fn type_validation_rejects_short_blake3() {
    let v = OutputValue::new(ValueType::Blake3Hex, "abc");
    assert!(v.validate_type().is_err());
}

#[test]
fn apply_binds_writes_array_by_key() {
    let dir = TempDir::new().unwrap();
    write_workload(&dir);

    let mut outputs = OutputMap::new();
    outputs.insert(
        "apply",
        "discovered_asset_blake3",
        OutputValue::new(ValueType::Blake3Hex, HASH_A),
    );
    outputs.insert(
        "apply",
        "discovered_fetch_blake3",
        OutputValue::new(ValueType::Blake3Hex, HASH_B),
    );

    let binds = vec![
        BindSpec {
            file: "workload.toml".into(),
            path: "asset[filename='whisper.tar.gz'].blake3".into(),
            from: OutputRef::parse("apply.outputs.discovered_asset_blake3").unwrap(),
            intent: Intent::Keyword(IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        },
        BindSpec {
            file: "workload.toml".into(),
            path: "asset[filename='whisper.tar.gz'].derive.fetch.blake3".into(),
            from: OutputRef::parse("apply.outputs.discovered_fetch_blake3").unwrap(),
            intent: Intent::Keyword(IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        },
    ];

    let applied = apply_binds(&outputs, &binds, dir.path()).unwrap();
    assert_eq!(applied.len(), 2);
    assert!(applied.iter().all(|a| a.changed));

    let after = fs::read_to_string(dir.path().join("workload.toml")).unwrap();
    assert!(after.contains(HASH_A));
    assert!(after.contains(HASH_B));

    // Second run is a no-op.
    let applied2 = apply_binds(&outputs, &binds, dir.path()).unwrap();
    assert_eq!(applied2.len(), 2);
    assert!(applied2.iter().all(|a| !a.changed));
}

#[test]
fn apply_binds_pin_default_rejects_changes() {
    let dir = TempDir::new().unwrap();
    write_workload(&dir);

    let mut outputs = OutputMap::new();
    outputs.insert(
        "apply",
        "discovered_asset_blake3",
        OutputValue::new(ValueType::Blake3Hex, HASH_A),
    );

    let binds = vec![BindSpec {
        file: "workload.toml".into(),
        path: "asset[filename='whisper.tar.gz'].blake3".into(),
        from: OutputRef::parse("apply.outputs.discovered_asset_blake3").unwrap(),
        intent: Intent::Keyword(IntentKeyword::Pin),
        cross_workspace: false,
        schema: None,
    }];

    let applied = apply_binds(&outputs, &binds, dir.path()).unwrap();
    assert!(applied.is_empty()); // predicate rejected; no entry recorded
    let after = fs::read_to_string(dir.path().join("workload.toml")).unwrap();
    assert!(!after.contains(HASH_A));
    assert!(after.contains(&"0".repeat(64)));
}

#[test]
fn output_ref_parse_canonical() {
    let r = OutputRef::parse("apply.outputs.foo").unwrap();
    assert_eq!(
        r,
        OutputRef::StepOutput {
            step: "apply".into(),
            key: "foo".into()
        }
    );
}

#[test]
fn output_ref_parse_uri() {
    let r = OutputRef::parse("registry://python:3.12-slim").unwrap();
    assert!(matches!(r, OutputRef::Uri(_)));
}

#[test]
fn output_ref_parse_bad() {
    assert!(OutputRef::parse("just.a.dotted.thing").is_err());
}
