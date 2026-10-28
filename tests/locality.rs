//! Locality: between two versions of a page, every component that differs (no component of its table in the
//! other version has its `fp`) overlaps a region the wikitext line diff marks as changed (`compare::locality`).
//! A violation is a parser artefact, unless it is a real long-range effect of an edit, which an allow file
//! lists with its reason: `tests/expected/locality_allow.toml` publicly, the gitignored
//! `tests/private/locality_allow.toml` privately.
//!
//! The `#[ignore]` test `locality_report_sample` runs the same check on the performance sample (D10) and
//! prints counts only; it asserts nothing about them.

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use rayon::prelude::*;
use serde::Deserialize;
use similar::{DiffTag, TextDiff};
use wikitext_components::compare::{changed_ranges, locality, Side, Violation};
use wikitext_components::corpus::CorpusRow;
use wikitext_components::input::{self, Layout};
use wikitext_components::model::{Components, PageOutput};
use wikitext_components::page;

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

struct Run {
    pairs: usize,
    /// Rows of either version in surplus of their `fp` in the other (with no changed ranges, all are violations).
    differing: usize,
    violations: usize,
    allowed: usize,
    /// Allowed violations by reason; filled for the public run only, since private reasons are not printed.
    by_reason: BTreeMap<String, usize>,
    bad: Vec<String>,
}

/// Every violation of every pair, checked against the allow file. A private message names the pair's index,
/// the page id, the version, the table and the row id; the full list goes to `dump`, under `target/`.
fn run(pairs: &[(&CorpusRow, &CorpusRow)], sites: [&str; 2], allow: &AllowFile, private: bool, dump: &Path) -> Run {
    let mut used = vec![false; allow.allow.len()];
    let mut r = Run { pairs: pairs.len(), differing: 0, violations: 0, allowed: 0, by_reason: BTreeMap::new(), bad: Vec::new() };
    let mut listing = String::new();
    for (i, (old, new)) in pairs.iter().enumerate() {
        let a = &common::run_rows(std::slice::from_ref(*old), sites[0])[0];
        let b = &common::run_rows(std::slice::from_ref(*new), sites[1])[0];
        let versions = [old.version.clone(), new.version.clone()];
        r.differing += locality("", "", a, b).len();
        for v in locality(&old.wikitext, &new.wikitext, a, b) {
            r.violations += 1;
            let (text, version) = match v.side {
                Side::Old => (&old.wikitext, &old.version),
                Side::New => (&new.wikitext, &new.version),
            };
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
                    r.allowed += 1;
                    if !private {
                        *r.by_reason.entry(allow.allow[k].reason.clone()).or_insert(0) += 1;
                    }
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

/// The counts of a run; the reasons only when they are public.
fn report(name: &str, r: &Run) {
    eprintln!(
        "locality, {name}: {} pairs, {} surplus rows, {} violations, {} allowed, {} unallowed",
        r.pairs, r.differing, r.violations, r.allowed, r.bad.len()
    );
    if !r.by_reason.is_empty() {
        eprintln!("locality, {name}: allowed by reason: {:?}", r.by_reason);
    }
}

fn run_page(t: &str) -> PageOutput {
    common::run_page(t, "wikipedia")
}

/// (side, table, id) of the violations between two texts.
fn violations(old: &str, new: &str) -> Vec<(Side, &'static str, i32)> {
    locality(old, new, &run_page(old), &run_page(new)).iter().map(|v| (v.side, v.table, v.id)).collect()
}

#[test]
#[allow(clippy::single_range_in_vec_init)]
fn changed_ranges_mark_lines_and_their_neighbours() {
    let abc = "a\nb\nc\n";
    // An inserted line marks itself and its neighbours, and the lines on either side of where it goes in the
    // other text; a deleted or replaced line likewise (R64).
    assert_eq!(changed_ranges(abc, "a\nX\nb\nc\n"), (vec![0..4], vec![0..6]));
    assert_eq!(changed_ranges(abc, "a\nc\n"), (vec![0..6], vec![0..4]));
    assert_eq!(changed_ranges(abc, "a\nB\nc\n"), (vec![0..6], vec![0..6]));
    assert_eq!(changed_ranges("a\n", "a\nb\n"), (vec![0..2], vec![0..4]));
    assert_eq!(changed_ranges("", "a\n"), (vec![0..0], vec![0..2]));
    assert_eq!(changed_ranges(abc, abc), (vec![], vec![]));
    // Lines keep their newline: a last line without one is a line too.
    assert_eq!(changed_ranges("a\nb", "a\nc"), (vec![0..3], vec![0..3]));
    // A blank line splitting a paragraph marks both halves (the review's probe).
    let (old, new) = ("== A ==\naaa\nbbb\n\n== B ==\nccc\n", "== A ==\naaa\n\nbbb\n\n== B ==\nccc\n");
    let (a, b) = changed_ranges(old, new);
    assert!(a.iter().any(|r| r.start <= 8 && 16 <= r.end), "{a:?}");
    assert!(b.iter().any(|r| r.start <= 8 && 17 <= r.end), "{b:?}");
}

#[test]
fn blank_line_edits_at_paragraph_edges_are_local() {
    let base = "== A ==\naaa\nbbb\n\n== B ==\nccc\n";
    let split = "== A ==\naaa\n\nbbb\n\n== B ==\nccc\n";
    // Split one paragraph by a blank line, merge two by deleting it, replace a middle line by a blank one.
    assert_eq!(violations(base, split), vec![]);
    assert_eq!(violations(split, base), vec![]);
    assert_eq!(violations("== A ==\naaa\nxxx\nbbb\n\n== B ==\nccc\n", split), vec![]);
    // Cases that held before R64: a blank line inside a list, an inserted heading, a one-line paragraph split.
    assert_eq!(violations("x\n\n* a\n* b\n* c\n\ny\n", "x\n\n* a\n* b\n\n* c\n\ny\n"), vec![]);
    assert_eq!(violations("== A ==\nx\n\ny\n\n== B ==\nz\n", "== A ==\nx\n\n== H ==\ny\n\n== B ==\nz\n"), vec![]);
    assert_eq!(violations("== A ==\naaa bbb\n\n== B ==\nccc\n", split), vec![]);
    // Every one of them does change components.
    for (old, new) in [(base, split), (split, base)] {
        assert!(!locality("", "", &run_page(old), &run_page(new)).is_empty());
    }
}

#[test]
fn locality_flags_what_differs_away_from_the_edit() {
    // A paragraph edit: what differs is where the text changed.
    let (old, new) = ("== A ==\nx\n\n== B ==\ny\n", "== A ==\nx\n\n== B ==\nz\n");
    assert_eq!(violations(old, new), vec![]);
    assert!(!locality("", "", &run_page(old), &run_page(new)).is_empty(), "the edit makes components differ");
}

const RENAMED_OLD: &str = "== A ==\nx\n\n=== B ===\ny\n\n==== D ====\nw\n\n== E ==\nv\n";
const RENAMED_NEW: &str = "== C ==\nx\n\n=== B ===\ny\n\n==== D ====\nw\n\n== E ==\nv\n";

#[test]
fn a_section_is_touched_through_its_ancestors_headings() {
    // Renaming a parent changes the path, so the fp, of its child and grandchild, whose own lines did not change.
    let (old, new) = (RENAMED_OLD, RENAMED_NEW);
    let (a, mut b) = (run_page(old), run_page(new));
    let differ: Vec<_> = locality("", "", &a, &b).iter().map(|v| (v.side, v.table, v.id)).collect();
    let ids = |side| [(side, "sections", 1), (side, "sections", 2), (side, "sections", 3)];
    assert_eq!(differ, [ids(Side::Old), ids(Side::New)].concat());
    // The renamed heading touches its descendants (R63), so nothing is a violation.
    assert_eq!(locality(old, new, &a, &b), vec![]);
    // A section elsewhere whose fp changes, its own lines and its ancestors' headings unchanged, still is one.
    b.components.sections[4].fp = "0000000000000000".into();
    let v: Vec<_> = locality(old, new, &a, &b).iter().map(|v| (v.side, v.table, v.id, v.start, v.end)).collect();
    assert_eq!(v, vec![(Side::Old, "sections", 4, 39, 49), (Side::New, "sections", 4, 39, 49)]);
}

#[test]
fn an_ancestors_heading_does_not_cover_a_changed_title() {
    // R65: a renamed ancestor explains a new fp only when the section's own title_norm and level are in the
    // other version too. Here the child's title_norm differs although its heading bytes do not (the review's
    // probe, made by changing the output): it is a violation on both sides, its unchanged grandchild is not.
    let (old, new) = (RENAMED_OLD, RENAMED_NEW);
    let (a, mut b) = (run_page(old), run_page(new));
    assert_eq!((b.components.sections[2].level, b.components.sections[3].level), (3, 4));
    b.components.sections[2].title_norm = "другое".into();
    let v: Vec<_> = locality(old, new, &a, &b).iter().map(|v| (v.side, v.table, v.id)).collect();
    assert_eq!(v, vec![(Side::Old, "sections", 2), (Side::New, "sections", 2)]);
    // The same title at another level is not the same section either.
    let (a, mut b) = (run_page(old), run_page(new));
    b.components.sections[2].level = 2;
    let v: Vec<_> = locality(old, new, &a, &b).iter().map(|v| (v.side, v.table, v.id)).collect();
    assert_eq!(v, vec![(Side::Old, "sections", 2), (Side::New, "sections", 2)]);
}

#[test]
fn repeated_fps_count_as_a_multiset() {
    // R66: two {{-}} with the same fp, one dropped from the new output with the text unchanged: one violation.
    let t = "x\n{{-}}\ny\n{{-}}\nz\n";
    let (a, mut b) = (run_page(t), run_page(t));
    let dashes: Vec<_> = a.components.templates.iter().filter(|r| r.name == "-").map(|r| r.fp.clone()).collect();
    assert_eq!(dashes.len(), 2);
    assert_eq!(dashes[0], dashes[1]);
    let k = b.components.templates.iter().rposition(|r| r.name == "-").expect("a {{-}}");
    b.components.templates.remove(k);
    let v: Vec<_> = locality(t, t, &a, &b).iter().map(|v| (v.side, v.table)).collect();
    assert_eq!(v, vec![(Side::Old, "templates")]);
    // Dropped where the text changed, the surplus row is touched: no violation.
    let new = "x\n{{-}}\ny\nz\n";
    assert_eq!(violations(t, new), vec![]);
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

/// Where a component sits in the other version: its span mapped through the equal lines of the line diff.
/// `None` when the span does not lie inside one run of equal lines (a violation always does, since it overlaps
/// no changed range).
fn mapped(starts: [&[usize]; 2], equal: &[(usize, usize, usize, usize)], side: Side, (s, e): (i32, i32)) -> Option<(i32, i32)> {
    let (mine, theirs) = match side {
        Side::Old => (starts[0], starts[1]),
        Side::New => (starts[1], starts[0]),
    };
    let (s, e) = (s.max(0) as usize, e.max(0) as usize);
    equal.iter().find_map(|&(os, oe, ns, ne)| {
        let ((a, b), c) = match side {
            Side::Old => ((os, oe), ns),
            Side::New => ((ns, ne), os),
        };
        (mine[a] <= s && e <= mine[b]).then(|| {
            let delta = theirs[c] as i64 - mine[a] as i64;
            ((s as i64 + delta) as i32, (e as i64 + delta) as i32)
        })
    })
}

/// The fields behind each table's `fp` (SCHEMA.md), as keys of a row's JSON value.
fn fp_inputs(table: &str) -> &'static [&'static str] {
    match table {
        "sections" => &["path"],
        "paragraphs" => &["text"],
        "templates" => &["name", "params"],
        "refs" => &["kind", "group", "name", "body_text"],
        "citations" => &["family", "url", "title", "authors", "text"],
        _ => &[],
    }
}

/// A row's field as its `fp` reads it: a template's parameters by name and `value_text` only, without their
/// spans, which move with any edit before them.
fn fp_input(row: &serde_json::Value, field: &str) -> Option<serde_json::Value> {
    let v = row.get(field)?;
    match (field, v) {
        ("params", serde_json::Value::Array(params)) => Some(serde_json::Value::Array(
            params.iter().map(|p| serde_json::json!({ "name": p.get("name"), "value_text": p.get("value_text") })).collect(),
        )),
        _ => Some(v.clone()),
    }
}

/// A table's rows as (span, JSON value).
fn table_rows(c: &Components, table: &str) -> Vec<((i32, i32), serde_json::Value)> {
    fn of<T: serde::Serialize>(rows: &[T], span: impl Fn(&T) -> (i32, i32)) -> Vec<((i32, i32), serde_json::Value)> {
        rows.iter().map(|r| (span(r), serde_json::to_value(r).expect("a row serialises"))).collect()
    }
    match table {
        "sections" => of(&c.sections, |r| (r.start, r.end)),
        "paragraphs" => of(&c.paragraphs, |r| (r.start, r.end)),
        "templates" => of(&c.templates, |r| (r.start, r.end)),
        "refs" => of(&c.refs, |r| (r.start, r.end)),
        "citations" => of(&c.citations, |r| (r.start, r.end)),
        other => panic!("locality checks no table {other}"),
    }
}

/// The class of a violation, from field names and fixed vocabularies only (never a value):
/// - `same span, <fields> differ`: the other version has a row of the table at the mapped span, and these of
///   the fields behind `fp` differ (a context effect: the component's own markup is unchanged);
/// - `same span and fp`: that row has the same `fp` (see the comment below);
/// - `other span`: rows of the table overlap the mapped span, none exactly (the markup is cut differently);
/// - `absent` (refs and citations: `absent, template there (role R)` when a template sits at the mapped span):
///   nothing of the table is there in the other version.
fn class(v: &Violation, mine: &Components, theirs: &Components, at: Option<(i32, i32)>) -> String {
    let Some(at) = at else { return "outside an equal run".to_string() };
    let rows = table_rows(theirs, v.table);
    let me = &table_rows(mine, v.table)[v.id as usize].1;
    if let Some((_, other)) = rows.iter().find(|(span, _)| *span == at) {
        if me.get("fp") == other.get("fp") {
            // The multiset rule (R66) reports the untouched rows of a surplus fp in id order, so the row named may
            // have its counterpart while the missing one is elsewhere.
            return "same span and fp (surplus of a repeated fp)".to_string();
        }
        let fields: Vec<&str> =
            fp_inputs(v.table).iter().copied().filter(|f| fp_input(me, f) != fp_input(other, f)).collect();
        return match fields.is_empty() {
            true => "same span, no fp input differs".to_string(),
            false => format!("same span, {} differ", fields.join("+")),
        };
    }
    let overlap = |(s, e): (i32, i32)| s < at.1 && at.0 < e || (s == e && at.0 <= s && s <= at.1);
    if rows.iter().any(|(span, _)| overlap(*span)) {
        return "other span".to_string();
    }
    if matches!(v.table, "refs" | "citations") {
        if let Some(t) = theirs.templates.iter().find(|t| (t.start, t.end) == at) {
            return format!("absent, template there (role {})", t.role);
        }
    }
    "absent".to_string()
}

#[derive(Default)]
struct SampleReport {
    pairs: usize,
    identical: usize,
    statuses: BTreeMap<(String, String), usize>,
    surplus: usize,
    violations: usize,
    in_identical: usize,
    pairs_with: BTreeMap<&'static str, usize>,
    /// (table, side, class) -> violations.
    classes: BTreeMap<(&'static str, &'static str, String), usize>,
    /// (input row, side, table, id, start, end, class) for the optional local dump.
    listing: Vec<(i32, &'static str, &'static str, i32, i32, i32, String)>,
}

impl SampleReport {
    fn merge(mut self, o: SampleReport) -> SampleReport {
        self.pairs += o.pairs;
        self.identical += o.identical;
        self.surplus += o.surplus;
        self.violations += o.violations;
        self.in_identical += o.in_identical;
        for (k, n) in o.statuses {
            *self.statuses.entry(k).or_default() += n;
        }
        for (k, n) in o.pairs_with {
            *self.pairs_with.entry(k).or_default() += n;
        }
        for (k, n) in o.classes {
            *self.classes.entry(k).or_default() += n;
        }
        self.listing.extend(o.listing);
        self
    }
}

/// One pair of the sample: both versions parsed, their violations counted and classed.
fn sample_pair(row: i32, old: (&str, &PageOutput), new: (&str, &PageOutput)) -> SampleReport {
    let mut r = SampleReport { pairs: 1, ..SampleReport::default() };
    *r.statuses.entry((old.1.page.status.clone(), new.1.page.status.clone())).or_default() += 1;
    let identical = old.0 == new.0;
    r.identical = usize::from(identical);
    r.surplus = locality("", "", old.1, new.1).len();
    let found = locality(old.0, new.0, old.1, new.1);
    r.violations = found.len();
    r.in_identical = if identical { found.len() } else { 0 };
    let bucket = match found.len() {
        0 => "0",
        1 => "1",
        2..=9 => "2-9",
        _ => "10+",
    };
    *r.pairs_with.entry(bucket).or_default() += 1;
    if found.is_empty() {
        return r;
    }
    let diff = TextDiff::from_lines(old.0, new.0);
    let starts = |slices: Vec<&str>| {
        let mut out = vec![0];
        for l in slices {
            out.push(out.last().copied().unwrap_or(0) + l.len());
        }
        out
    };
    let (os, ns) = (starts(diff.iter_old_slices().collect()), starts(diff.iter_new_slices().collect()));
    let equal: Vec<(usize, usize, usize, usize)> = diff
        .ops()
        .iter()
        .map(|op| op.as_tag_tuple())
        .filter(|(tag, ..)| *tag == DiffTag::Equal)
        .map(|(_, o, n)| (o.start, o.end, n.start, n.end))
        .collect();
    for v in &found {
        let (mine, theirs, side) = match v.side {
            Side::Old => (old.1, new.1, "wikipedia"),
            Side::New => (new.1, old.1, "ruwiki"),
        };
        let at = mapped([&os, &ns], &equal, v.side, (v.start, v.end));
        let c = class(v, &mine.components, &theirs.components, at);
        *r.classes.entry((v.table, side, c.clone())).or_default() += 1;
        r.listing.push((row, side, v.table, v.id, v.start, v.end, c));
    }
    r
}

/// The locality report on the performance sample (D10, R71): a `crossection_diff` file in the pair layout, its
/// path in `WIKITEXT_SAMPLE`, `official_text` parsed as `wikipedia:2025-01-01` and `clone_text` as
/// `ruwiki:2025-01-01`. The sample holds Ruwiki markup, so the report prints counts per table, side and class
/// (see `class`) only: never a value, a title, an fp or a page id. With `WIKITEXT_SAMPLE_DUMP` set, the
/// violations (input row, side, table, row id, span, class) go to that file, for reading the markup locally.
#[test]
#[ignore]
fn locality_report_sample() {
    let Ok(path) = std::env::var("WIKITEXT_SAMPLE") else {
        eprintln!("SKIPPED (WIKITEXT_SAMPLE unset): locality_report_sample");
        return;
    };
    let labels = ["official_text=wikipedia:2025-01-01", "clone_text=ruwiki:2025-01-01"].map(|l| input::parse_label(l).unwrap());
    let mut file = input::open(Path::new(&path), Some(Layout::Pair), None, &labels).expect("the sample opens as a pair file");
    let (wiki, ruwiki, registry, limits) = (common::settings_for("wikipedia"), common::settings_for("ruwiki"), common::reg(), common::defaults());
    let mut report = SampleReport::default();
    for batch in file.batches(256) {
        let pages = batch.expect("the sample reads");
        let part = pages
            .par_chunks(2)
            .map(|pair| {
                let [old, new] = pair else { panic!("a pair row yields two texts") };
                assert_eq!((old.key.site.as_str(), new.key.site.as_str()), ("wikipedia", "ruwiki"));
                let (a, b) = (page::process_page(old, &wiki, &registry, &limits), page::process_page(new, &ruwiki, &registry, &limits));
                let text = |p: &page::PageInput| p.text.clone().unwrap_or_default();
                sample_pair(old.key.row, (&text(old), &a), (&text(new), &b))
            })
            .reduce(SampleReport::default, SampleReport::merge);
        report = report.merge(part);
    }
    let r = &report;
    eprintln!("locality, sample: {} pairs, {} byte-identical", r.pairs, r.identical);
    eprintln!("locality, sample: statuses (wikipedia, ruwiki): {:?}", r.statuses);
    eprintln!(
        "locality, sample: {} surplus rows, {} violations ({} in byte-identical pairs)",
        r.surplus, r.violations, r.in_identical
    );
    eprintln!("locality, sample: pairs by violations: {:?}", r.pairs_with);
    let mut by_table: BTreeMap<&str, usize> = BTreeMap::new();
    for ((table, ..), n) in &r.classes {
        *by_table.entry(table).or_default() += n;
    }
    eprintln!("locality, sample: violations by table: {by_table:?}");
    for ((table, side, class), n) in &r.classes {
        eprintln!("locality, sample: {table}\t{side}\t{class}\t{n}");
    }
    if let Ok(dump) = std::env::var("WIKITEXT_SAMPLE_DUMP") {
        let mut lines = String::from("row\tside\ttable\tid\tstart\tend\tclass\n");
        let mut listing = report.listing.clone();
        listing.sort();
        for (row, side, table, id, s, e, c) in listing {
            lines.push_str(&format!("{row}\t{side}\t{table}\t{id}\t{s}\t{e}\t{c}\n"));
        }
        std::fs::write(&dump, lines).expect("write the dump");
        eprintln!("locality, sample: violations listed in WIKITEXT_SAMPLE_DUMP");
    }
}
