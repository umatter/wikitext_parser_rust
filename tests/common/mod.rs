#![allow(dead_code)]

pub mod check;

use std::path::{Path, PathBuf};
use wikitext_components::corpus::{self, CorpusRow};
use wikitext_components::settings::Settings;

/// Free URLs in and around refs (R73), for the walk's tests and the fallback's parity test: each text with the
/// (family, url, host, title) of its citation rows.
pub const FREE_URL_CASES: &[(&str, &[(&str, Option<&str>, Option<&str>, Option<&str>)])] = &[
    // Trailing punctuation is not part of the URL.
    ("<ref>http://a.ru/x.</ref>", &[("bare_link", Some("http://a.ru/x"), Some("a.ru"), None)]),
    ("<ref>См. http://a.ru/x, http://b.ru/y!</ref>", &[
        ("bare_link", Some("http://a.ru/x"), Some("a.ru"), None),
        ("bare_link", Some("http://b.ru/y"), Some("b.ru"), None),
    ]),
    // A `)` stays when the URL has a `(`, as MediaWiki keeps it (both of them here), and goes when it has none.
    ("<ref>(see http://a.ru/(x))</ref>", &[("bare_link", Some("http://a.ru/(x))"), Some("a.ru"), None)]),
    ("<ref>(http://a.ru/x)</ref>", &[("bare_link", Some("http://a.ru/x"), Some("a.ru"), None)]),
    ("<ref>http://a.ru/x)</ref>", &[("bare_link", Some("http://a.ru/x"), Some("a.ru"), None)]),
    // Inside a citation template, an archive wrapper or an external link: no row of its own.
    ("<ref>{{cite web|url=http://a.ru/x|title=T}}</ref>", &[("web", Some("http://a.ru/x"), Some("a.ru"), Some("T"))]),
    ("<ref>{{webarchive|url=https://web.archive.org/web/2020/http://a.ru/x|date=2020}}</ref>", &[("web", Some("http://a.ru/x"), Some("a.ru"), None)]),
    ("<ref>[http://b.ru/ Обзор, см. http://a.ru/x]</ref>", &[("bare_link", Some("http://b.ru/"), Some("b.ru"), Some("Обзор, см. http://a.ru/x"))]),
    // No protocol-relative free URL, and no URL after a letter.
    ("<ref>см. //a.ru/x</ref>", &[("plain_text", None, None, None)]),
    ("<ref>xhttp://a.ru/x</ref>", &[("plain_text", None, None, None)]),
    ("<ref>http://</ref>", &[("plain_text", None, None, None)]),
    // Other protocols: mailto has no host.
    ("<ref>mailto:x@a.ru</ref>", &[("bare_link", Some("mailto:x@a.ru"), None, None)]),
    ("<ref>FTP://a.ru/f</ref>", &[("bare_link", Some("FTP://a.ru/f"), Some("a.ru"), None)]),
    // A comment inside is skipped; `&lt;` cuts; an entity's `;` stays; quotes, templates and tags end the URL.
    ("<ref>http://a.ru/<!-- c -->x y</ref>", &[("bare_link", Some("http://a.ru/x"), Some("a.ru"), None)]),
    ("<ref>http://a.ru/x&lt;y</ref>", &[("bare_link", Some("http://a.ru/x"), Some("a.ru"), None)]),
    ("<ref>http://a.ru/?a=1&amp;</ref>", &[("bare_link", Some("http://a.ru/?a=1&"), Some("a.ru"), None)]),
    ("<ref>http://a.ru/x''курсив''</ref>", &[("bare_link", Some("http://a.ru/x"), Some("a.ru"), None)]),
    ("<ref>http://a.ru/x{{недоступная ссылка}}</ref>", &[("bare_link", Some("http://a.ru/x"), Some("a.ru"), None)]),
    ("<ref>http://a.ru/x<br>http://b.ru</ref>", &[
        ("bare_link", Some("http://a.ru/x"), Some("a.ru"), None),
        ("bare_link", Some("http://b.ru"), Some("b.ru"), None),
    ]),
    // In another template's parameter value, `|` ends it; a name is no text.
    ("<ref>{{lang-en|http://a.ru/x|y}}</ref>", &[("bare_link", Some("http://a.ru/x"), Some("a.ru"), None)]),
    // Raw text, a link, a file: no URL.
    ("<ref><nowiki>http://a.ru/x</nowiki></ref>", &[("plain_text", None, None, None)]),
    ("<ref>[[http://a.ru]] и [[Статья|http://a.ru/x]]</ref>", &[("plain_text", None, None, None)]),
    // Outside refs, a free URL is no row.
    ("Сайт: http://a.ru/x.", &[]),
    // Short footnotes have no body.
    ("{{sfn|http://a.ru|2000}}", &[]),
];

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
