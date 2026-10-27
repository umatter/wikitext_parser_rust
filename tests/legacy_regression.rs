mod common;

use std::path::Path;
use std::process::Command;
use wikitext_components::corpus::{self, LegacyOut};

fn run(bin: &str, args: &[&str]) {
    let out = Command::new(bin).args(args).output().expect("spawn");
    assert!(
        out.status.success(),
        "{bin} {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Runs the current legacy binaries over the inputs in `dir` (see `write_legacy_inputs`).
fn run_legacy_in(dir: &Path) -> Vec<LegacyOut> {
    let p = |n: &str| dir.join(n).to_str().unwrap().to_string();
    run(
        env!("CARGO_BIN_EXE_parse_single"),
        &["--input", &p("single.parquet"), "--output", &p("single.dirty.parquet"), "--timeout", "0"],
    );
    run(
        env!("CARGO_BIN_EXE_clean_parsed"),
        &["--input", &p("single.dirty.parquet"), "--output", &p("single.clean.parquet")],
    );
    run(
        env!("CARGO_BIN_EXE_wikitext_parser_rust"),
        &["--input", &p("pair.parquet"), "--output", &p("pair.dirty.parquet"), "--timeout", "0"],
    );
    run(
        env!("CARGO_BIN_EXE_clean_parsed"),
        &["--input", &p("pair.dirty.parquet"), "--output", &p("pair.clean.parquet")],
    );
    run(
        env!("CARGO_BIN_EXE_export_parsed"),
        &[&p("pair.clean.parquet"), &p("export/official"), &p("export/clone")],
    );
    corpus::collect_legacy(dir).expect("collect")
}

fn run_legacy(
    selection: (Vec<&corpus::CorpusRow>, Vec<(&corpus::CorpusRow, &corpus::CorpusRow)>),
    scratch: &Path,
) -> Vec<LegacyOut> {
    corpus::write_legacy_inputs(scratch, &selection.0, &selection.1).expect("inputs");
    run_legacy_in(scratch)
}

fn scratch(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("legacy_regression_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[test]
fn legacy_public_outputs_are_byte_identical_to_v010() {
    let rows = common::public_corpus();
    let dir = scratch("public");
    let got = run_legacy(corpus::public_legacy_selection(&rows), &dir);
    let _ = std::fs::remove_dir_all(&dir);
    let want = corpus::read_legacy_long(Path::new("tests/expected/legacy_wikipedia_v1.parquet"))
        .expect("expected file");
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(g, w, "{} {} {} {}", w.page_id, w.version, w.binary, w.stage);
    }
}

#[test]
fn legacy_private_outputs_match_committed_hashes() {
    let Some(ruwiki) = common::private_corpus("legacy_private_outputs_match_committed_hashes")
    else {
        return;
    };
    let public = common::public_corpus();
    let dir = scratch("private");
    let got = run_legacy(corpus::private_legacy_selection(&public, &ruwiki), &dir);
    let _ = std::fs::remove_dir_all(&dir);
    let want = std::fs::read_to_string("tests/expected/legacy_private.sha256").unwrap();
    assert_eq!(corpus::hash_lines(&got), want);
}
