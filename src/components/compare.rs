//! Comparing the outputs of two parses: symmetry (identical markup, identical tables) and locality (what
//! differs between two versions lies where the markup changed).
//!
//! # Symmetry
//!
//! `identical_except_labels` compares the six component tables row by row and field by field, and the pages
//! row without its labels (the key columns, `title` and `timestamp`); the timings are not compared. Its error
//! names a table, a row index and a field, never a value, so it is safe on private pages.
//!
//! # Locality
//!
//! - `changed_ranges` diffs the two texts by lines (`similar::TextDiff::from_lines`, which keeps each line's
//!   newline). Lines a deletion or a replacement removes are changed in the old text, lines an insertion or a
//!   replacement adds are changed in the new one. Where one text has lines the other lacks, the other text
//!   gets the zero-width position between its lines, widened to the neighbouring line on each side: from the
//!   start of the line before to the end of the line after, as far as they exist.
//! - `locality` checks the tables `sections`, `paragraphs`, `templates`, `refs` and `citations`. A component
//!   *differs* when no component of the same table in the other version has its `fp`. It is a violation when
//!   it differs and is not *touched*: its span overlaps no changed range of its own text (`start < b && a <
//!   end` for a range `a..b`, and for an empty span or range, the position lying in the other, ends included).
//!   A section is also touched when the heading (`heading_start..heading_end`) of one of its ancestors, found
//!   through `parent_section_id`, overlaps a changed range (Ruling R63): its `fp` hashes its path, so renaming
//!   an ancestor changes its identity although its own lines did not change.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use serde::Serialize;
use similar::{DiffTag, TextDiff};

use crate::model::{PageOutput, SectionRow};

/// The pages-row fields that label a version rather than describe its parse.
const LABELS: [&str; 8] = ["source_file", "row", "page_id", "site", "version", "column", "title", "timestamp"];

/// The first field of two serialised rows that differs, in field order.
fn first_field<T: Serialize>(a: &T, b: &T, skip: &[&str]) -> Option<String> {
    let (a, b) = (serde_json::to_value(a).ok()?, serde_json::to_value(b).ok()?);
    let (a, b) = (a.as_object()?, b.as_object()?);
    a.iter().filter(|(k, _)| !skip.contains(&k.as_str())).find(|(k, v)| b.get(*k) != Some(*v)).map(|(k, _)| k.clone())
}

fn same_rows<T: Serialize + PartialEq>(table: &str, a: &[T], b: &[T]) -> Result<(), String> {
    if a.len() != b.len() {
        return Err(format!("{table}: {} rows against {}", a.len(), b.len()));
    }
    match a.iter().zip(b).position(|(x, y)| x != y) {
        None => Ok(()),
        Some(i) => {
            let field = first_field(&a[i], &b[i], &[]).unwrap_or_else(|| "a field".into());
            Err(format!("{table} row {i}: {field} differs"))
        }
    }
}

/// `Ok` when the two outputs have the same component tables and the same pages row apart from its labels.
pub fn identical_except_labels(a: &PageOutput, b: &PageOutput) -> Result<(), String> {
    if let Some(field) = first_field(&a.page, &b.page, &LABELS) {
        return Err(format!("pages: {field} differs"));
    }
    let (a, b) = (&a.components, &b.components);
    same_rows("sections", &a.sections, &b.sections)?;
    same_rows("paragraphs", &a.paragraphs, &b.paragraphs)?;
    same_rows("templates", &a.templates, &b.templates)?;
    same_rows("refs", &a.refs, &b.refs)?;
    same_rows("ref_lists", &a.ref_lists, &b.ref_lists)?;
    same_rows("citations", &a.citations, &b.citations)
}

/// The byte offset where each line starts, then the text's length.
fn line_starts<'a>(lines: impl Iterator<Item = &'a str>) -> Vec<usize> {
    let mut out = vec![0];
    for l in lines {
        out.push(out.last().copied().unwrap_or(0) + l.len());
    }
    out
}

/// The zero-width position before line `i`, widened to the line before it and the line after it.
fn widened(starts: &[usize], i: usize) -> Range<usize> {
    let n = starts.len() - 1;
    starts[i.saturating_sub(1)]..starts[(i + 1).min(n)]
}

/// The byte ranges a line diff marks as changed in `old` and in `new`.
pub fn changed_ranges(old: &str, new: &str) -> (Vec<Range<usize>>, Vec<Range<usize>>) {
    let diff = TextDiff::from_lines(old, new);
    let (os, ns) = (line_starts(diff.iter_old_slices()), line_starts(diff.iter_new_slices()));
    let (mut a, mut b) = (Vec::new(), Vec::new());
    for op in diff.ops() {
        let (tag, o, n) = op.as_tag_tuple();
        match tag {
            DiffTag::Equal => {}
            DiffTag::Delete => {
                a.push(os[o.start]..os[o.end]);
                b.push(widened(&ns, n.start));
            }
            DiffTag::Insert => {
                a.push(widened(&os, o.start));
                b.push(ns[n.start]..ns[n.end]);
            }
            DiffTag::Replace => {
                a.push(os[o.start]..os[o.end]);
                b.push(ns[n.start]..ns[n.end]);
            }
        }
    }
    (a, b)
}

/// A component that differs between two versions where its own text did not change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub table: &'static str,
    pub id: i32,
    pub fp: String,
    pub start: i32,
    pub end: i32,
}

/// A row: its id, its `fp`, its span, and the further spans that touch it (a section's ancestors' headings).
struct Row<'a> {
    id: i32,
    fp: &'a str,
    span: (i32, i32),
    via: Vec<(i32, i32)>,
}

fn row(id: i32, fp: &str, start: i32, end: i32) -> Row<'_> {
    Row { id, fp, span: (start, end), via: Vec::new() }
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
    let c = &p.components;
    let by_id: HashMap<i32, &SectionRow> = c.sections.iter().map(|r| (r.id, r)).collect();
    let sections = c.sections.iter().map(|r| Row { via: ancestor_headings(&by_id, r), ..row(r.id, &r.fp, r.start, r.end) }).collect();
    [
        ("sections", sections),
        ("paragraphs", c.paragraphs.iter().map(|r| row(r.id, &r.fp, r.start, r.end)).collect()),
        ("templates", c.templates.iter().map(|r| row(r.id, &r.fp, r.start, r.end)).collect()),
        ("refs", c.refs.iter().map(|r| row(r.id, &r.fp, r.start, r.end)).collect()),
        ("citations", c.citations.iter().map(|r| row(r.id, &r.fp, r.start, r.end)).collect()),
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

/// The components of either version that differ and are not touched by a changed range of their own text:
/// the old version's first, then the new one's, each table in the order above and rows in id order.
pub fn locality(old_text: &str, new_text: &str, old: &PageOutput, new: &PageOutput) -> Vec<Violation> {
    let (old_ranges, new_ranges) = changed_ranges(old_text, new_text);
    let (old_rows, new_rows) = (rows(old), rows(new));
    let mut out = Vec::new();
    let sides = [(&old_rows, &new_rows, &old_ranges), (&new_rows, &old_rows, &new_ranges)];
    for (mine, theirs, ranges) in sides {
        for ((table, mine), (_, theirs)) in mine.iter().zip(theirs.iter()) {
            let other: HashSet<&str> = theirs.iter().map(|r| r.fp).collect();
            for r in mine {
                let touched = std::iter::once(&r.span).chain(&r.via).any(|&s| ranges.iter().any(|c| overlaps(s, c)));
                if !other.contains(r.fp) && !touched {
                    out.push(Violation { table, id: r.id, fp: r.fp.to_string(), start: r.span.0, end: r.span.1 });
                }
            }
        }
    }
    out
}
