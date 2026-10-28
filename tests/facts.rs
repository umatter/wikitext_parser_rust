//! Facts read from the markup (Ruling R59): for a few page versions, counts and lists written down by reading
//! the wikitext (`tests/facts.toml`; the Ruwiki ones next to the private corpus), checked against a parse.
//! The keys are defined in the header of `tests/facts.toml`.

mod common;

use std::collections::BTreeMap;

use serde::Deserialize;
use wikitext_components::corpus::CorpusRow;
use wikitext_components::model::PageOutput;
use wikitext_components::registry::Registry;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Facts {
    fact: Vec<Fact>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fact {
    page_id: String,
    version: String,
    refs_inline: Option<usize>,
    refs_named: Option<usize>,
    refs_reuse: Option<usize>,
    refs_list_defined: Option<usize>,
    refs_template: Option<usize>,
    ref_lists: Option<usize>,
    citations_by_family: Option<BTreeMap<String, usize>>,
    hosts: Option<Vec<String>>,
    reuse_targets: Option<Vec<String>>,
    reuses_unresolved: Option<usize>,
    sfn_keys: Option<Vec<String>>,
    sfn_unresolved: Option<usize>,
    sections: Option<Vec<String>>,
    archived_originals: Option<Vec<String>>,
}

/// The same keys, computed from a parse.
#[derive(Debug)]
struct Got {
    refs_inline: usize,
    refs_named: usize,
    refs_reuse: usize,
    refs_list_defined: usize,
    refs_template: usize,
    ref_lists: usize,
    citations_by_family: BTreeMap<String, usize>,
    hosts: Vec<String>,
    reuse_targets: Vec<String>,
    reuses_unresolved: usize,
    sfn_keys: Vec<String>,
    sfn_unresolved: usize,
    sections: Vec<String>,
    archived_originals: Vec<String>,
}

fn got(out: &PageOutput, site: &str, reg: &Registry) -> Got {
    let c = &out.components;
    let kind = |k: &str| c.refs.iter().filter(|r| r.kind == k).count();
    let mut citations_by_family = BTreeMap::new();
    for x in &c.citations {
        *citations_by_family.entry(x.family.clone()).or_insert(0) += 1;
    }
    let sorted = |mut v: Vec<String>| {
        v.sort();
        v
    };
    let mut hosts = sorted(c.citations.iter().filter_map(|x| x.host.clone()).collect());
    hosts.dedup();
    let reuses = || c.refs.iter().filter(|r| r.kind == "reuse");
    // A short footnote ({{sfn}} and aliases): a template ref whose template is of registry family `short`.
    let short: Vec<_> = c
        .refs
        .iter()
        .filter(|r| r.kind == "template")
        .filter(|r| {
            c.templates
                .iter()
                .find(|t| (t.start, t.end) == (r.start, r.end))
                .and_then(|t| reg.template(&t.name, site))
                .is_some_and(|e| e.family.as_deref() == Some("short"))
        })
        .collect();
    Got {
        refs_inline: kind("inline"),
        refs_named: kind("named"),
        refs_reuse: kind("reuse"),
        refs_list_defined: kind("list_defined"),
        refs_template: kind("template"),
        ref_lists: c.ref_lists.len(),
        citations_by_family,
        hosts,
        reuse_targets: sorted(reuses().filter(|r| r.resolved).filter_map(|r| r.name.clone()).collect()),
        reuses_unresolved: reuses().filter(|r| !r.resolved).count(),
        sfn_keys: sorted(short.iter().filter(|r| r.cites_citation_id.is_some()).filter_map(|r| r.harv_key.clone()).collect()),
        sfn_unresolved: short.iter().filter(|r| r.cites_citation_id.is_none()).count(),
        sections: c.sections.iter().skip(1).map(|s| s.title_text.clone()).collect(),
        archived_originals: sorted(c.citations.iter().filter(|x| x.archive_url.is_some()).filter_map(|x| x.url.clone()).collect()),
    }
}

/// Each present key of `f`: (key, whether it holds, the values for a public message).
fn compare(f: &Fact, g: &Got) -> Vec<(&'static str, bool, String)> {
    let mut out = Vec::new();
    macro_rules! key {
        ($($k:ident),*) => {$(
            if let Some(want) = &f.$k {
                out.push((stringify!($k), *want == g.$k, format!("want {:?}, got {:?}", want, g.$k)));
            }
        )*};
    }
    key!(
        refs_inline, refs_named, refs_reuse, refs_list_defined, refs_template, ref_lists, citations_by_family, hosts,
        reuse_targets, reuses_unresolved, sfn_keys, sfn_unresolved, sections, archived_originals
    );
    out
}

/// Every mismatch of `facts` against the rows, as messages; a private message names the fact's index, the page
/// id, the version and the key, never a value.
fn check(facts: &[Fact], rows: &[CorpusRow], site: &str, private: bool) -> Vec<String> {
    let reg = common::reg();
    let mut bad = Vec::new();
    for (i, f) in facts.iter().enumerate() {
        let where_ = format!("{} fact {i} (page_id {}, {})", if private { "private" } else { "public" }, f.page_id, f.version);
        let Some(row) = rows.iter().find(|r| r.page_id == f.page_id && r.version == f.version) else {
            bad.push(format!("{where_}: no such version in the corpus"));
            continue;
        };
        let out = &common::run_rows(std::slice::from_ref(row), site)[0];
        if out.page.status != "ok" {
            bad.push(format!("{where_}: status {}", out.page.status));
        }
        for (key, ok, values) in compare(f, &got(out, site, &reg)) {
            match (ok, private) {
                (true, _) => {}
                (false, true) => bad.push(format!("{where_}: {key} differs")),
                (false, false) => bad.push(format!("{where_}: {key}: {values}")),
            }
        }
    }
    bad
}

/// The facts of `path`. A parse error of a private file names only the byte offset, since toml's message would
/// quote the line.
fn load(path: &std::path::Path, private: bool) -> Vec<Fact> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let facts: Facts = toml::from_str(&text).unwrap_or_else(|e| match private {
        true => panic!("{}: not a facts file (at byte {:?})", path.display(), e.span().map(|s| s.start)),
        false => panic!("{}: {e}", path.display()),
    });
    facts.fact
}

#[test]
fn public_facts_hold() {
    let facts = load(std::path::Path::new("tests/facts.toml"), false);
    let labels: std::collections::BTreeSet<&str> = facts.iter().map(|f| f.version.as_str()).collect();
    assert_eq!(labels.len(), 3, "every Wikipedia label has facts: {labels:?}");
    let bad = check(&facts, &common::public_corpus(), "wikipedia", false);
    assert!(bad.is_empty(), "{} fact mismatches:\n{}", bad.len(), bad.join("\n"));
}

#[test]
fn private_facts_hold() {
    let Some(rows) = common::private_corpus("private_facts_hold") else { return };
    let path = common::private_facts_path();
    assert!(path.exists(), "the private corpus is present, but its facts file is not (next to it)");
    let facts = load(&path, true);
    assert!(facts.iter().all(|f| f.version == "ruwiki_2025") && !facts.is_empty(), "private facts are Ruwiki versions");
    let bad = check(&facts, &rows, "ruwiki", true);
    assert!(bad.is_empty(), "{} private fact mismatches:\n{}", bad.len(), bad.join("\n"));
}
