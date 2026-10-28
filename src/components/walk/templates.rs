//! The templates table: one row per template, in document order, with its parameters and its nesting.
//!
//! The walk (`walk/mod.rs`) tells this collector when it enters and leaves a template and a parameter and
//! hands it the text of each parameter value (a `Sink::Flat`, under the text rules of `text.rs`).
//!
//! # Rules
//!
//! - `name_raw` is the source slice of the whole name part, trimmed. `name` is `Settings::template_name`
//!   of it. The kind is `module` for `#invoke:`, `parser_function` for any other name starting with `#` or
//!   having a known parser-function word before its first colon, `variable` for a magic variable, and
//!   `template` otherwise.
//! - For a module or a parser function the name is the word before the first colon, and the text after the
//!   colon is positional parameter `1`, so the `|` parameters count from `2`. Its `value_raw` and span are
//!   that text trimmed; its `value_text` follows the text rules over the nodes after the colon.
//! - Positional parameters count from 1 in order. `N=` names a parameter `N` and does not advance the
//!   count (MediaWiki). Duplicates are all kept, in source order.
//! - `ParamRow.start`/`end` is the span the parser reports for the parameter, clamped to the text.
//! - A template inside another template (in a name or a parameter value, however deep in links or tags)
//!   has that template as its parent and a depth one greater.
//! - `fp` is over the name and the `name=norm(value_text)` pairs, sorted by name (stable).

use parse_wiki_text_2::{Node, Parameter, Positioned};

use crate::fingerprint::{fp, norm};
use crate::model::{ParamRow, TemplateRow};
use crate::registry::Registry;
use crate::settings::Settings;
use crate::spans::CharIndex;

use super::Place;

struct Open {
    row: usize,
    /// The number the next unnamed parameter takes.
    next_pos: u32,
    /// Byte after the colon of a module or parser function, whose text is parameter `1`.
    split_from: Option<usize>,
    /// The text of the parameter value that just closed, until its `Param` closes.
    pending: String,
}

pub(super) struct Templates<'t, 's> {
    text: &'t str,
    chars: &'t CharIndex,
    settings: &'s Settings,
    registry: &'s Registry,
    rows: Vec<TemplateRow>,
    places: Vec<Place>,
    open: Vec<Open>,
}

/// The source span of a group of nodes, trimmed of whitespace; empty at the group's start when it has none.
fn trimmed_span(text: &str, nodes: &[Node]) -> (usize, usize) {
    let (Some(first), Some(last)) = (nodes.first(), nodes.last()) else { return (0, 0) };
    trim_range(text, first.start(), last.end())
}

fn trim_range(text: &str, start: usize, end: usize) -> (usize, usize) {
    let end = end.min(text.len());
    let start = start.min(end);
    match text.get(start..end) {
        Some(s) => {
            let lead = s.len() - s.trim_start().len();
            (start + lead, start + lead + s.trim().len())
        }
        None => (start, start),
    }
}

impl<'t, 's> Templates<'t, 's> {
    pub(super) fn new(
        text: &'t str,
        chars: &'t CharIndex,
        settings: &'s Settings,
        registry: &'s Registry,
    ) -> Self {
        Templates { text, chars, settings, registry, rows: Vec::new(), places: Vec::new(), open: Vec::new() }
    }

    /// The byte after the colon of the template just opened, when its name part holds parameter `1`.
    pub(super) fn split_from(&self) -> Option<usize> {
        self.open.last().and_then(|o| o.split_from)
    }

    /// A template node is entered at `place`.
    pub(super) fn enter(&mut self, node: &Node, place: Place) {
        let Node::Template { name: name_nodes, start, end, .. } = node else { return };
        let len = self.text.len();
        let (start, end) = (*start.min(&len), (*end).min(len).max(*start.min(&len)));
        let (name_start, name_end) = trimmed_span(self.text, name_nodes);
        let name_raw = self.text.get(name_start..name_end).unwrap_or("");
        // The first colon in the name's own text.
        let colon = name_nodes.iter().find_map(|n| match n {
            Node::Text { value, start, .. } => value.find(':').map(|i| start + i),
            _ => None,
        });
        let head = colon.and_then(|c| self.text.get(name_start..c)).map(str::trim);
        let whole = self.settings.template_name(name_raw);
        let kind = if head.is_some_and(|h| norm(h) == "#invoke") {
            "module"
        } else if name_raw.starts_with('#') || head.is_some_and(|h| self.settings.is_parser_function(h)) {
            "parser_function"
        } else if self.settings.is_variable(&whole) {
            "variable"
        } else {
            "template"
        };
        let split = matches!(kind, "module" | "parser_function").then_some(colon).flatten();
        let name = match (split, head) {
            (Some(_), Some(h)) => self.settings.template_name(h),
            _ => whole,
        };
        let role = self
            .registry
            .template(&name, self.settings.site())
            .map_or_else(|| "other".to_string(), |e| e.role.clone());
        let (parent_type, parent_id, depth) = match self.open.last() {
            Some(o) => (Some("template".to_string()), Some(o.row as i32), self.rows[o.row].depth + 1),
            None => (None, None, 0),
        };
        let mut params = Vec::new();
        if let Some(c) = split {
            let (a, b) = trim_range(self.text, c + 1, name_end);
            params.push(ParamRow {
                name: "1".into(),
                positional: true,
                value_raw: self.text.get(a..b).unwrap_or("").to_string(),
                value_text: String::new(),
                start: a as i32,
                end: b as i32,
            });
        }
        let id = self.rows.len();
        self.rows.push(TemplateRow {
            id: id as i32,
            start: start as i32,
            end: end as i32,
            start_char: self.chars.char_of(start),
            end_char: self.chars.char_of(end),
            section_id: place.section_id,
            para_id: None,
            para_offset: None,
            parent_type,
            parent_id,
            fallback: false,
            fp: String::new(),
            name_raw: name_raw.to_string(),
            name,
            kind: kind.to_string(),
            role,
            looks_like_infobox: false,
            depth,
            rendered_len: 0,
            params,
        });
        self.places.push(place);
        self.open.push(Open {
            row: id,
            next_pos: if split.is_some() { 2 } else { 1 },
            split_from: split.map(|c| c + 1),
            pending: String::new(),
        });
    }

    /// The name part of the open template closes with the text after its colon.
    pub(super) fn set_split_text(&mut self, text: String) {
        if let Some(o) = self.open.last() {
            if let Some(p) = self.rows[o.row].params.first_mut() {
                p.value_text = text;
            }
        }
    }

    /// The value of a parameter closes with its text.
    pub(super) fn set_pending(&mut self, text: String) {
        if let Some(o) = self.open.last_mut() {
            o.pending = text;
        }
    }

    /// A parameter of the open template closes.
    pub(super) fn close_param(&mut self, p: &Parameter) {
        let Some(o) = self.open.last_mut() else { return };
        let (name, positional) = match &p.name {
            Some(nodes) => {
                let (a, b) = trimmed_span(self.text, nodes);
                (self.text.get(a..b).unwrap_or("").to_string(), false)
            }
            None => {
                let n = o.next_pos;
                o.next_pos += 1;
                (n.to_string(), true)
            }
        };
        let (a, b) = trimmed_span(self.text, &p.value);
        let len = self.text.len();
        let start = p.start.min(len);
        let row = ParamRow {
            name,
            positional,
            value_raw: self.text.get(a..b).unwrap_or("").to_string(),
            value_text: std::mem::take(&mut o.pending),
            start: start as i32,
            end: p.end.min(len).max(start) as i32,
        };
        self.rows[o.row].params.push(row);
    }

    /// The open template closes.
    pub(super) fn close(&mut self) {
        let Some(o) = self.open.pop() else { return };
        let row = &mut self.rows[o.row];
        let mut pairs: Vec<(&str, String)> =
            row.params.iter().map(|p| (p.name.as_str(), format!("{}={}", p.name, norm(&p.value_text)))).collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let joined = pairs.into_iter().map(|(_, s)| s).collect::<Vec<_>>().join("\u{1f}");
        row.fp = fp(&format!("{}\u{1f}{}", row.name, joined));
    }

    /// The rows, each with its `para_id` and `para_offset` resolved by `resolve`.
    pub(super) fn finish(
        mut self,
        resolve: impl Fn(Place) -> (Option<i32>, Option<i32>),
    ) -> Vec<TemplateRow> {
        for (row, place) in self.rows.iter_mut().zip(&self.places) {
            (row.para_id, row.para_offset) = resolve(*place);
        }
        self.rows
    }
}
