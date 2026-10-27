mod common;

use wikitext_components::corpus;

#[test]
fn public_corpus_has_only_wikipedia_versions() {
    let rows = common::public_corpus();
    assert_eq!(rows.len(), 180);
    assert!(rows.iter().all(|r| !r.version.starts_with("ruwiki")));
    let pages: std::collections::BTreeSet<_> = rows.iter().map(|r| &r.page_id).collect();
    assert_eq!(pages.len(), 105);
    assert!(rows
        .iter()
        .all(|r| matches!(r.case.as_deref(), None | Some("slow") | Some("largest"))));
}

#[test]
fn public_corpus_hash_matches_committed() {
    let want = std::fs::read_to_string("tests/corpus/wikipedia_v1.sha256").unwrap();
    assert_eq!(
        corpus::sha256_file("tests/corpus/wikipedia_v1.parquet".as_ref()).unwrap(),
        want.trim()
    );
}

#[test]
fn private_corpus_is_ruwiki_only_or_skipped() {
    if let Some(rows) = common::private_corpus("private_corpus_is_ruwiki_only_or_skipped") {
        assert_eq!(rows.len(), 75);
        assert!(rows.iter().all(|r| r.version == "ruwiki_2025"));
    }
}

#[test]
fn public_corpus_case_counts() {
    let rows = common::public_corpus();
    let n = |c: &str| rows.iter().filter(|r| r.case.as_deref() == Some(c)).count();
    assert_eq!(n("slow"), 10);
    assert_eq!(n("largest"), 9);
}
