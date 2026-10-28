//! The section tree: the lead plus one section per heading, wherever the heading is.

use crate::fingerprint::{fp, norm};
use crate::model::SectionRow;
use crate::registry::Registry;
use crate::spans::CharIndex;

struct Head {
    level: i32,
    start: usize,
    end: usize,
    title_raw: String,
    title_text: String,
}

/// Collects headings in document order; `finish` derives spans, parents, paths and roles.
pub struct Sections {
    heads: Vec<Head>,
}

impl Sections {
    pub fn new() -> Sections {
        Sections { heads: Vec::new() }
    }

    /// Opens a section at the heading `start..end` and returns its id (the lead is 0).
    pub fn open(&mut self, level: u8, start: usize, end: usize, title_raw: &str) -> i32 {
        self.heads.push(Head {
            level: level as i32,
            start,
            end,
            title_raw: title_raw.to_string(),
            title_text: String::new(),
        });
        self.heads.len() as i32
    }

    pub fn set_title(&mut self, id: i32, title_text: String) {
        self.heads[id as usize - 1].title_text = title_text;
    }

    /// The rows, lead first. A section ends where the next one starts; its subtree ends at the next heading of
    /// the same or a higher level; its parent is the nearest earlier section with a lower level.
    pub fn finish(self, text: &str, chars: &CharIndex, registry: &Registry) -> Vec<SectionRow> {
        let text_len = text.len();
        let n = self.heads.len() + 1;
        let starts: Vec<usize> = std::iter::once(0).chain(self.heads.iter().map(|h| h.start)).collect();
        let levels: Vec<i32> = std::iter::once(0).chain(self.heads.iter().map(|h| h.level)).collect();
        let level = |i: usize| levels[i];
        let mut parent = vec![0usize; n];
        let mut subtree_end = vec![text_len; n];
        // Open ancestors, levels strictly increasing from the lead.
        let mut open: Vec<usize> = vec![0];
        for i in 1..n {
            while let Some(&top) = open.last() {
                if top != 0 && level(top) >= level(i) {
                    subtree_end[top] = starts[i];
                    open.pop();
                } else {
                    break;
                }
            }
            parent[i] = *open.last().expect("the lead is never closed");
            open.push(i);
        }
        let mut rows: Vec<SectionRow> = Vec::with_capacity(n);
        let mut heads = self.heads.into_iter();
        for i in 0..n {
            let (title_raw, title_text, heading) = match i {
                0 => (String::new(), String::new(), None),
                _ => {
                    let h = heads.next().expect("one head per section");
                    (h.title_raw, h.title_text, Some((h.start as i32, h.end as i32)))
                }
            };
            let title_norm = norm(&title_text);
            let path = match i {
                0 => Vec::new(),
                _ => {
                    let mut p = rows[parent[i]].path.clone();
                    p.push(title_norm.clone());
                    p
                }
            };
            let (start, end) = (starts[i], starts.get(i + 1).copied().unwrap_or(text_len));
            debug_assert!(
                start <= end && end <= text_len && text.is_char_boundary(start) && text.is_char_boundary(end),
                "section span {start}..{end} outside a text of {text_len} bytes"
            );
            rows.push(SectionRow {
                id: i as i32,
                start: start as i32,
                end: end as i32,
                start_char: chars.char_of(start),
                end_char: chars.char_of(end),
                section_id: i as i32,
                para_id: None,
                para_offset: None,
                parent_type: None,
                parent_id: None,
                fallback: false,
                fp: fp(&path.join("\u{1f}")),
                level: level(i),
                title_text,
                title_raw,
                role: registry.section_role(&title_norm).to_string(),
                title_norm,
                path,
                parent_section_id: parent[i] as i32,
                heading_start: heading.map(|h| h.0),
                heading_end: heading.map(|h| h.1),
                subtree_end: subtree_end[i] as i32,
            });
        }
        rows
    }
}
