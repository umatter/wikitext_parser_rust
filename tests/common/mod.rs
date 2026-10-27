#![allow(dead_code)]

use std::path::{Path, PathBuf};
use wikitext_components::corpus::{self, CorpusRow};

pub const DEFAULT_PRIVATE: &str = "../ruwiki_replication_package_sources/data/results/sources/test_corpus/parser_test_corpus_v1.parquet";

pub fn public_corpus() -> Vec<CorpusRow> {
    corpus::load(Path::new("tests/corpus/wikipedia_v1.parquet")).expect("public corpus loads")
}

/// Ruwiki rows of the private corpus, or `None` when the file is absent.
pub fn private_corpus(test: &str) -> Option<Vec<CorpusRow>> {
    let path = PathBuf::from(
        std::env::var("WIKITEXT_PRIVATE_CORPUS").unwrap_or_else(|_| DEFAULT_PRIVATE.to_string()),
    );
    if !path.exists() {
        eprintln!("SKIPPED (private corpus absent): {test}");
        return None;
    }
    let got = corpus::sha256_file(&path).expect("hash private corpus");
    assert_eq!(got, corpus::PRIVATE_V1_SHA256, "private corpus hash differs");
    let rows = corpus::load(&path).expect("private corpus loads");
    Some(rows.into_iter().filter(|r| r.version == "ruwiki_2025").collect())
}

pub fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!("tests/fixtures/{name}.wiki"))
        .unwrap_or_else(|e| panic!("fixture {name}: {e}"))
}
