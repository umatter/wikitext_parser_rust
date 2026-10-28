//! Comparing the outputs of two parses: symmetry (identical markup, identical tables) and locality (what
//! differs between two versions lies where the markup changed).
//!
//! # Symmetry
//!
//! `identical_except_labels` compares the six component tables row by row, and the pages row with its labels
//! (the key columns, `title` and `timestamp`) blanked; the timings are not compared. `PageOutput` and
//! `Components` are destructured with every field named, so a new table or field does not compile until it
//! is placed here (R67). Its error names a table, a row index and a field, never a value, so it is safe on
//! private pages; a row that does not serialise is an error, never "no difference".
//!
//! # Locality
//!
//! - `changed_ranges` diffs the two texts by lines (`similar::TextDiff::from_lines`, which keeps each line's
//!   newline). Every operation that is not `Equal` marks, in each text, the lines it covers there (none, for
//!   the text an insertion or a deletion has no lines in), widened to the line before and the line after, as
//!   far as they exist (R64). So an inserted line marks itself and its two neighbours in the new text, and the
//!   neighbouring lines of where it goes in the old one; a blank line that splits or merges two paragraphs
//!   marks both of them.
//! - `locality` checks the tables `sections`, `paragraphs`, `templates`, `refs` and `citations`, each by its
//!   `fp` taken as a multiset (R66). Where one version has `n` more rows of an `fp` than the other, at least
//!   `n` of its rows of that `fp` must be *touched*; each one short is a violation, reported as the untouched
//!   rows of that `fp` in id order. A row is touched when its span overlaps a changed range of its own text
//!   (`start < b && a < end` for a range `a..b`; for an empty span or range, the position lying in the other,
//!   ends included).
//! - A section is also touched when the heading (`heading_start..heading_end`) of one of its ancestors, found
//!   through `parent_section_id`, overlaps a changed range, provided the other version has a section with
//!   the same `title_norm` and `level` (R63, narrowed by R65): its `fp` hashes its whole path, and an edited
//!   ancestor explains a different `fp` only when the section's own title is the same.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use serde::Serialize;
use similar::{DiffTag, TextDiff};

use crate::model::{Components, PageOutput, PageRow, SectionRow};

/// The pages row with its labels blanked.
fn unlabelled(p: &PageRow) -> PageRow {
    PageRow {
        source_file: String::new(),
        row: 0,
        page_id: String::new(),
        site: String::new(),
        version: String::new(),
        column: String::new(),
        title: None,
        timestamp: None,
        ..p.clone()
    }
}

/// The name of the first field in which two unequal rows differ, in field order.
fn differing_field<T: Serialize>(table: &str, a: &T, b: &T) -> Result<String, String> {
    let value = |x: &T| serde_json::to_value(x).map_err(|_| format!("{table}: a row does not serialise"));
    let (a, b) = (value(a)?, value(b)?);
    let (Some(a), Some(b)) = (a.as_object(), b.as_object()) else {
        return Err(format!("{table}: a row is not a record"));
    };
    Ok(a.iter().find(|(k, v)| b.get(*k) != Some(*v)).map_or_else(|| "a field".to_string(), |(k, _)| k.clone()))
}

fn same_rows<T: Serialize + PartialEq>(table: &str, a: &[T], b: &[T]) -> Result<(), String> {
    if a.len() != b.len() {
        return Err(format!("{table}: {} rows against {}", a.len(), b.len()));
    }
    match a.iter().zip(b).position(|(x, y)| x != y) {
        None => Ok(()),
        Some(i) => Err(format!("{table} row {i}: {} differs", differing_field(table, &a[i], &b[i])?)),
    }
}

/// `Ok` when the two outputs have the same component tables and the same pages row apart from its labels.
pub fn identical_except_labels(a: &PageOutput, b: &PageOutput) -> Result<(), String> {
    let PageOutput { page: page_a, timing: _, components: components_a } = a;
    let PageOutput { page: page_b, timing: _, components: components_b } = b;
    let (page_a, page_b) = (unlabelled(page_a), unlabelled(page_b));
    if page_a != page_b {
        return Err(format!("pages: {} differs", differing_field("pages", &page_a, &page_b)?));
    }
    let Components { sections, paragraphs, templates, refs, ref_lists, citations } = components_a;
    let Components {
        sections: sections_b,
        paragraphs: paragraphs_b,
        templates: templates_b,
        refs: refs_b,
        ref_lists: ref_lists_b,
        citations: citations_b,
    } = components_b;
    same_rows("sections", sections, sections_b)?;
    same_rows("paragraphs", paragraphs, paragraphs_b)?;
    same_rows("templates", templates, templates_b)?;
    same_rows("refs", refs, refs_b)?;
    same_rows("ref_lists", ref_lists, ref_lists_b)?;
    same_rows("citations", citations, citations_b)
}

/// The byte offset where each line starts, then the text's length.
fn line_starts<'a>(lines: impl Iterator<Item = &'a str>) -> Vec<usize> {
    let mut out = vec![0];
    for l in lines {
        out.push(out.last().copied().unwrap_or(0) + l.len());
    }
    out
}

/// The bytes of `lines` (line indices; empty for the position before a line) widened to the line before and
/// the line after, as far as they exist.
fn widened(starts: &[usize], lines: Range<usize>) -> Range<usize> {
    let n = starts.len() - 1;
    starts[lines.start.saturating_sub(1).min(n)]..starts[(lines.end + 1).min(n)]
}

/// The byte ranges a line diff marks as changed in `old` and in `new`.
pub fn changed_ranges(old: &str, new: &str) -> (Vec<Range<usize>>, Vec<Range<usize>>) {
    let diff = TextDiff::from_lines(old, new);
    let (os, ns) = (line_starts(diff.iter_old_slices()), line_starts(diff.iter_new_slices()));
    let (mut a, mut b) = (Vec::new(), Vec::new());
    for op in diff.ops() {
        let (tag, o, n) = op.as_tag_tuple();
        if tag != DiffTag::Equal {
            a.push(widened(&os, o));
            b.push(widened(&ns, n));
        }
    }
    (a, b)
}

/// The version a violating component is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Old,
    New,
}

/// A component that differs between two versions where its own text did not change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub side: Side,
    pub table: &'static str,
    pub id: i32,
    pub fp: String,
    pub start: i32,
    pub end: i32,
}

/// What a section adds to a row: its own title and level, and its ancestors' heading spans.
struct SectionPart<'a> {
    title_norm: &'a str,
    level: i32,
    ancestor_headings: Vec<(i32, i32)>,
}

/// A row of one of the five tables.
struct Row<'a> {
    id: i32,
    fp: &'a str,
    span: (i32, i32),
    section: Option<SectionPart<'a>>,
}

fn row(id: i32, fp: &str, start: i32, end: i32) -> Row<'_> {
    Row { id, fp, span: (start, end), section: None }
}

/// The heading spans of the ancestors of section `s`, nearest first, up to the lead (its own parent).
fn ancestor_headings(by_id: &HashMap<i32, &SectionRow>, s: &SectionRow) -> Vec<(i32, i32)> {
    let mut out = Vec::new();
    let (mut id, mut steps) = (s.parent_section_id, 0);
    while id != s.id && steps < by_id.len() {
        let Some(a) = by_id.get(&id) else { break };
        if let (Some(hs), Some(he)) = (a.heading_start, a.heading_end) {
            out.push((hs, he));
        }
        if a.parent_section_id == a.id {
            break;
        }
        (id, steps) = (a.parent_section_id, steps + 1);
    }
    out
}

/// The rows of the five tables, by table.
fn rows(p: &PageOutput) -> [(&'static str, Vec<Row<'_>>); 5] {
    let Components { sections, paragraphs, templates, refs, ref_lists: _, citations } = &p.components;
    let by_id: HashMap<i32, &SectionRow> = sections.iter().map(|r| (r.id, r)).collect();
    let sections = sections
        .iter()
        .map(|r| Row {
            section: Some(SectionPart {
                title_norm: &r.title_norm,
                level: r.level,
                ancestor_headings: ancestor_headings(&by_id, r),
            }),
            ..row(r.id, &r.fp, r.start, r.end)
        })
        .collect();
    [
        ("sections", sections),
        ("paragraphs", paragraphs.iter().map(|r| row(r.id, &r.fp, r.start, r.end)).collect()),
        ("templates", templates.iter().map(|r| row(r.id, &r.fp, r.start, r.end)).collect()),
        ("refs", refs.iter().map(|r| row(r.id, &r.fp, r.start, r.end)).collect()),
        ("citations", citations.iter().map(|r| row(r.id, &r.fp, r.start, r.end)).collect()),
    ]
}

fn overlaps((start, end): (i32, i32), r: &Range<usize>) -> bool {
    let (start, end) = (start.max(0) as usize, end.max(0) as usize);
    if start == end || r.start == r.end {
        (r.start <= start && start <= r.end) || (start <= r.start && r.start <= end)
    } else {
        start < r.end && r.start < end
    }
}

/// The violations of one table on one side: `mine` against the other version's rows `theirs`.
fn table_violations(side: Side, table: &'static str, mine: &[Row], theirs: &[Row], ranges: &[Range<usize>]) -> Vec<Violation> {
    let mut count: HashMap<&str, usize> = HashMap::new();
    for r in theirs {
        *count.entry(r.fp).or_default() += 1;
    }
    let titles: HashSet<(&str, i32)> = theirs.iter().filter_map(|r| r.section.as_ref()).map(|s| (s.title_norm, s.level)).collect();
    let hit = |s: (i32, i32)| ranges.iter().any(|c| overlaps(s, c));
    let touched = |r: &Row| {
        hit(r.span)
            || r.section.as_ref().is_some_and(|s| {
                titles.contains(&(s.title_norm, s.level)) && s.ancestor_headings.iter().any(|&h| hit(h))
            })
    };
    let mut by_fp: HashMap<&str, Vec<&Row>> = HashMap::new();
    for r in mine {
        by_fp.entry(r.fp).or_default().push(r);
    }
    let mut found: Vec<&Row> = Vec::new();
    for (fp, rows) in &by_fp {
        let surplus = rows.len().saturating_sub(count.get(fp).copied().unwrap_or(0));
        let untouched: Vec<&Row> = rows.iter().copied().filter(|r| !touched(r)).collect();
        let short = surplus.saturating_sub(rows.len() - untouched.len());
        found.extend(untouched.into_iter().take(short));
    }
    found.sort_by_key(|r| r.id);
    found
        .into_iter()
        .map(|r| Violation { side, table, id: r.id, fp: r.fp.to_string(), start: r.span.0, end: r.span.1 })
        .collect()
}

/// The violations of either version: the old version's first, then the new one's, each table in the order
/// above and rows in id order.
pub fn locality(old_text: &str, new_text: &str, old: &PageOutput, new: &PageOutput) -> Vec<Violation> {
    let (old_ranges, new_ranges) = changed_ranges(old_text, new_text);
    let (old_rows, new_rows) = (rows(old), rows(new));
    let mut out = Vec::new();
    let sides = [(Side::Old, &old_rows, &new_rows, &old_ranges), (Side::New, &new_rows, &old_rows, &new_ranges)];
    for (side, mine, theirs, ranges) in sides {
        for ((table, mine), (_, theirs)) in mine.iter().zip(theirs.iter()) {
            out.extend(table_violations(side, table, mine, theirs, ranges));
        }
    }
    out
}
