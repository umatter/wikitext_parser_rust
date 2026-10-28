#![allow(dead_code)]

pub mod check;

use std::path::{Path, PathBuf};
use wikitext_components::corpus::{self, CorpusRow};
use wikitext_components::settings::Settings;

pub const DEFAULT_PRIVATE: &str = "../ruwiki_replication_package_sources/data/results/sources/test_corpus/parser_test_corpus_v1.parquet";

pub fn public_corpus() -> Vec<CorpusRow> {
    corpus::load(Path::new("tests/corpus/wikipedia_v1.parquet")).expect("public corpus loads")
}

/// The private corpus file: `WIKITEXT_PRIVATE_CORPUS`, else `DEFAULT_PRIVATE`.
pub fn private_path() -> PathBuf {
    PathBuf::from(std::env::var("WIKITEXT_PRIVATE_CORPUS").unwrap_or_else(|_| DEFAULT_PRIVATE.to_string()))
}

/// The hand-counted facts of Ruwiki versions, which live next to the private corpus and are never committed.
pub fn private_facts_path() -> PathBuf {
    private_path().with_file_name("parser_facts_ruwiki.toml")
}

/// Ruwiki rows of the private corpus, or `None` when the file is absent.
pub fn private_corpus(test: &str) -> Option<Vec<CorpusRow>> {
    let path = private_path();
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

/// Settings of Russian Wikipedia.
pub fn ru() -> Settings {
    Settings::load(Path::new("config/sites/ru.wikipedia.toml")).expect("ru settings load")
}

use wikitext_components::model::{Key, PageOutput};
use wikitext_components::page::{self, Limits, PageInput};
use wikitext_components::registry::Registry;

/// The four defaults, with the fallback on.
pub fn defaults() -> Limits {
    Limits {
        parse_budget: page::DEFAULT_PARSE_BUDGET,
        walk_budget: page::DEFAULT_WALK_BUDGET,
        depth_cap: page::DEFAULT_DEPTH_CAP,
        fallback: true,
    }
}

/// The registry of `config/registry/`.
pub fn reg() -> Registry {
    Registry::load(Path::new("config/registry")).expect("registry loads")
}

fn test_key(site: &str) -> Key {
    Key {
        source_file: "test.parquet".into(),
        row: 0,
        page_id: "1".into(),
        site: site.into(),
        version: "test".into(),
        column: "text".into(),
    }
}

fn run_settings(text: Option<&str>, settings: &Settings, site: &str, limits: &Limits) -> PageOutput {
    let input = PageInput {
        key: test_key(site),
        title: None,
        timestamp: None,
        text: text.map(str::to_string),
    };
    page::process_page(&input, settings, &reg(), limits)
}

/// Process `text` on site `wikipedia` (Russian Wikipedia settings) or `ruwiki`.
pub fn run_page(text: &str, site: &str) -> PageOutput {
    run_settings(Some(text), &settings_for(site), site, &defaults())
}

/// Settings of a site: `ruwiki`, or Russian Wikipedia for anything else.
pub fn settings_for(site: &str) -> Settings {
    if site == "ruwiki" {
        Settings::load(Path::new("config/sites/ruwiki.toml")).expect("ruwiki settings load")
    } else {
        ru()
    }
}

/// Process every corpus row on `site`, with the defaults, in row order.
pub fn run_rows(rows: &[CorpusRow], site: &str) -> Vec<PageOutput> {
    let (settings, registry) = (settings_for(site), reg());
    rows.iter()
        .enumerate()
        .map(|(i, r)| {
            let input = PageInput {
                key: Key { row: i as i32, page_id: r.page_id.clone(), version: r.version.clone(), ..test_key(site) },
                title: Some(r.page_title.clone()),
                timestamp: None,
                text: Some(r.wikitext.clone()),
            };
            page::process_page(&input, &settings, &registry, &defaults())
        })
        .collect()
}

/// Process `text` on site `wikipedia` with the given limits.
pub fn run_with(text: &str, limits: Limits) -> PageOutput {
    run_settings(Some(text), &ru(), "wikipedia", &limits)
}

/// Process an optional text on site `wikipedia` with the defaults.
pub fn run_opt(text: Option<&str>) -> PageOutput {
    run_settings(text, &ru(), "wikipedia", &defaults())
}
