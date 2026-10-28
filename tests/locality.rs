//! Locality: between two versions of a page, every component that differs (no component of its table in the
//! other version has its `fp`) overlaps a region the wikitext line diff marks as changed (`compare::locality`).
//! A violation is a parser artefact, unless it is a real long-range effect of an edit, which an allow file
//! lists with its reason: `tests/expected/locality_allow.toml` publicly, the gitignored
//! `tests/private/locality_allow.toml` privately.

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;
use wikitext_components::compare::{changed_ranges, locality, Violation};
use wikitext_components::corpus::CorpusRow;
use wikitext_components::model::PageOutput;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct AllowFile {
    #[serde(default)]
    allow: Vec<Allow>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Allow {
    page_id: String,
    versions: [String; 2],
    table: String,
    fp: String,
    reason: String,
}

fn allow_file(path: &Path, private: bool) -> AllowFile {
    let Ok(text) = std::fs::read_to_string(path) else { return AllowFile::default() };
    toml::from_str(&text).unwrap_or_else(|e| match private {
        true => panic!("{}: not an allow file (at byte {:?})", path.display(), e.span().map(|s| s.start)),
        false => panic!("{}: {e}", path.display()),
    })
}

/// The pages that have both versions, by page id, each with its two versions.
fn pairs<'a>(old: &'a [CorpusRow], new: &'a [CorpusRow], old_v: &str, new_v: &str) -> Vec<(&'a CorpusRow, &'a CorpusRow)> {
    let mut out: Vec<_> = old
        .iter()
        .filter(|o| o.version == old_v)
        .filter_map(|o| Some((o, new.iter().find(|n| n.version == new_v && n.page_id == o.page_id)?)))
        .collect();
    out.sort_by(|a, b| a.0.page_id.cmp(&b.0.page_id));
    out
}

/// Which version a violation is in: the one that has a row of its table, id, span and `fp`.
fn side(v: &Violation, old: &PageOutput) -> usize {
    let c = &old.components;
    let rows: Vec<(i32, i32, i32, &str)> = match v.table {
        "sections" => c.sections.iter().map(|r| (r.id, r.start, r.end, r.fp.as_str())).collect(),
        "paragraphs" => c.paragraphs.iter().map(|r| (r.id, r.start, r.end, r.fp.as_str())).collect(),
        "templates" => c.templates.iter().map(|r| (r.id, r.start, r.end, r.fp.as_str())).collect(),
        "refs" => c.refs.iter().map(|r| (r.id, r.start, r.end, r.fp.as_str())).collect(),
        _ => c.citations.iter().map(|r| (r.id, r.start, r.end, r.fp.as_str())).collect(),
    };
    usize::from(!rows.contains(&(v.id, v.start, v.end, v.fp.as_str())))
}

struct Run {
    pairs: usize,
    /// Components of either version without an equal `fp` in the other (with no changed ranges, every one).
    differing: usize,
    violations: usize,
    by_reason: BTreeMap<String, usize>,
    bad: Vec<String>,
}

/// Every violation of every pair, checked against the allow file. A private message names the pair's index,
/// the page id, the versions, the table and the row id; the full list goes to `dump`, under `target/`.
fn run(pairs: &[(&CorpusRow, &CorpusRow)], sites: [&str; 2], allow: &AllowFile, private: bool, dump: &Path) -> Run {
    let mut used = vec![false; allow.allow.len()];
    let mut r = Run { pairs: pairs.len(), differing: 0, violations: 0, by_reason: BTreeMap::new(), bad: Vec::new() };
    let mut listing = String::new();
    for (i, (old, new)) in pairs.iter().enumerate() {
        let a = &common::run_rows(std::slice::from_ref(*old), sites[0])[0];
        let b = &common::run_rows(std::slice::from_ref(*new), sites[1])[0];
        let versions = [old.version.clone(), new.version.clone()];
        r.differing += locality("", "", a, b).len();
        for v in locality(&old.wikitext, &new.wikitext, a, b) {
            r.violations += 1;
            let s = side(&v, a);
            let (text, version) = if s == 0 { (&old.wikitext, &old.version) } else { (&new.wikitext, &new.version) };
            listing.push_str(&format!(
                "[[allow]]\npage_id = {:?}\nversions = {versions:?}\ntable = {:?}\nfp = {:?}\nreason = \"\"\n# {version} id {} span {}..{}\n\n",
                old.page_id, v.table, v.fp, v.id, v.start, v.end
            ));
            let hit = allow.allow.iter().position(|e| {
                (e.page_id.as_str(), &e.versions, e.table.as_str(), e.fp.as_str()) == (old.page_id.as_str(), &versions, v.table, v.fp.as_str())
            });
            match hit {
                Some(k) => {
                    used[k] = true;
                    *r.by_reason.entry(allow.allow[k].reason.clone()).or_insert(0) += 1;
                }
                None if private => r.bad.push(format!("private pair {i} (page_id {}, {version}): {} id {}", old.page_id, v.table, v.id)),
                None => {
                    let slice = text.get(v.start as usize..v.end as usize).unwrap_or("");
                    let slice: String = slice.chars().take(120).collect();
                    r.bad.push(format!("page_id {} {version}: {} id {} fp {} {}..{}: {slice:?}", old.page_id, v.table, v.id, v.fp, v.start, v.end));
                }
            }
        }
    }
    for (e, used) in allow.allow.iter().zip(used) {
        if !used {
            r.bad.push(match private {
                true => format!("private allow entry for page_id {} ({}) matches no violation", e.page_id, e.table),
                false => format!("allow entry page_id {} {} fp {} matches no violation", e.page_id, e.table, e.fp),
            });
        }
    }
    std::fs::create_dir_all(dump.parent().expect("dump has a directory")).expect("create the dump directory");
    std::fs::write(dump, listing).expect("write the violation listing");
    r
}

fn report(name: &str, r: &Run) {
    eprintln!(
        "locality, {name}: {} pairs, {} differing components, {} violations, {} unallowed; allowed by reason: {:?}",
        r.pairs, r.differing, r.violations, r.bad.len(), r.by_reason
    );
}

#[test]
#[allow(clippy::single_range_in_vec_init)]
fn changed_ranges_mark_lines_and_widen_insertions() {
    let abc = "a\nb\nc\n";
    // An inserted line marks its own bytes, and the lines on either side of where it goes in the other text.
    assert_eq!(changed_ranges(abc, "a\nX\nb\nc\n"), (vec![0..4], vec![2..4]));
    assert_eq!(changed_ranges(abc, "a\nc\n"), (vec![2..4], vec![0..4]));
    assert_eq!(changed_ranges(abc, "a\nB\nc\n"), (vec![2..4], vec![2..4]));
    assert_eq!(changed_ranges("a\n", "a\nb\n"), (vec![0..2], vec![2..4]));
    assert_eq!(changed_ranges("", "a\n"), (vec![0..0], vec![0..2]));
    assert_eq!(changed_ranges(abc, abc), (vec![], vec![]));
    // Lines keep their newline: a last line without one is a line too.
    assert_eq!(changed_ranges("a\nb", "a\nc"), (vec![2..3], vec![2..3]));
}

#[test]
fn locality_flags_what_differs_away_from_the_edit() {
    let run = |t: &str| common::run_page(t, "wikipedia");
    // A paragraph edit: what differs is where the text changed.
    let (old, new) = ("== A ==\nx\n\n== B ==\ny\n", "== A ==\nx\n\n== B ==\nz\n");
    assert_eq!(locality(old, new, &run(old), &run(new)), vec![]);
    assert!(!locality("", "", &run(old), &run(new)).is_empty(), "the edit makes components differ");
}

#[test]
fn a_section_is_touched_through_its_ancestors_headings() {
    let run = |t: &str| common::run_page(t, "wikipedia");
    // Renaming a parent changes the path, so the fp, of its child and grandchild, whose own lines did not change.
    let old = "== A ==\nx\n\n=== B ===\ny\n\n==== D ====\nw\n\n== E ==\nv\n";
    let new = "== C ==\nx\n\n=== B ===\ny\n\n==== D ====\nw\n\n== E ==\nv\n";
    let (a, mut b) = (run(old), run(new));
    let differ: Vec<_> = locality("", "", &a, &b).iter().map(|v| (v.table, v.id)).collect();
    assert_eq!(differ, [("sections", 1), ("sections", 2), ("sections", 3)].repeat(2));
    // The renamed heading touches its descendants (R63), so nothing is a violation.
    assert_eq!(locality(old, new, &a, &b), vec![]);
    // A section elsewhere whose fp changes, its own lines and its ancestors' headings unchanged, still is one.
    b.components.sections[4].fp = "0000000000000000".into();
    let v: Vec<_> = locality(old, new, &a, &b).iter().map(|v| (v.table, v.id, v.start, v.end)).collect();
    assert_eq!(v, vec![("sections", 4, 39, 49), ("sections", 4, 39, 49)]);
}

#[test]
fn public_locality_fork_day_to_2025() {
    let rows = common::public_corpus();
    let pairs = pairs(&rows, &rows, "wiki_forkday", "wiki_2025");
    assert!(pairs.len() > 50, "{} pairs", pairs.len());
    let allow = allow_file(Path::new("tests/expected/locality_allow.toml"), false);
    let r = run(&pairs, ["wikipedia", "wikipedia"], &allow, false, Path::new("target/tmp/locality_public_violations.toml"));
    report("public", &r);
    assert!(r.bad.is_empty(), "{} locality failures:\n{}", r.bad.len(), r.bad.join("\n"));
}

#[test]
fn private_locality_2025_to_ruwiki() {
    let Some(ruwiki) = common::private_corpus("private_locality_2025_to_ruwiki") else { return };
    let public = common::public_corpus();
    let pairs = pairs(&public, &ruwiki, "wiki_2025", "ruwiki_2025");
    assert!(!pairs.is_empty());
    let allow = allow_file(Path::new("tests/private/locality_allow.toml"), true);
    let r = run(&pairs, ["wikipedia", "ruwiki"], &allow, true, Path::new("target/tmp/locality_private_violations.toml"));
    report("private", &r);
    assert!(r.bad.is_empty(), "{} private locality failures:\n{}", r.bad.len(), r.bad.join("\n"));
}
