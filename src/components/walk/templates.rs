//! The templates table: one row per template, in document order, with its parameters and its nesting.
//!
//! The walk (`walk/mod.rs`) and the fallback scanner tell this collector, through `collect.rs`, when they enter
//! and leave a template and a parameter, and hand it the spans of the name and of each parameter and the text
//! of each value (on the walk a `Sink::Flat`, under the text rules of `text.rs`).
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
//! - The parent (`parent_type`, `parent_id`) is the nearest enclosing component, which the walk gives: a
//!   template (in a name or a parameter value, however deep in links or tags), a ref or a `<references>`
//!   tag. A template inside a ref inside a template has the ref as its parent. `depth` counts the templates
//!   around it.
//! - `fp` is over the name and the `name=norm(value_text)` pairs, sorted by name (stable).

use crate::fingerprint::{fp, norm};
use crate::model::{ParamRow, TemplateRow};
use crate::registry::Registry;
use crate::settings::Settings;
use crate::spans::CharIndex;

use super::collect::ParamEvent;
use super::{clamp_span, parent_cols, trim_range, Parent, Place};

struct Open {
    row: usize,
    /// The number the next unnamed parameter takes.
    next_pos: u32,
    /// Byte after the colon of a module or parser function, whose text is parameter `1`.
    split_from: Option<usize>,
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

    /// A template with this trimmed name span and span is entered at `place`, inside `parent`; `colon` is the
    /// first colon in the name's own text (not inside a nested element). Returns the row's id.
    pub(super) fn enter(
        &mut self,
        (name_start, name_end): (usize, usize),
        colon: Option<usize>,
        (start, end): (usize, usize),
        place: Place,
        parent: Parent,
    ) -> usize {
        let (start, end) = clamp_span(self.text, start, end);
        let name_raw = self.text.get(name_start..name_end).unwrap_or("");
        let head = colon.and_then(|c| self.text.get(name_start..c)).map(str::trim);
        let whole = self.settings.template_name(name_raw);
        // An explicit template-namespace prefix transcludes the page: never a function or a variable.
        let prefixed = head.is_some_and(|h| self.settings.namespace(h) == Some(10));
        let kind = if prefixed {
            "template"
        } else if head.is_some_and(|h| norm(h) == "#invoke") {
            "module"
        } else if name_raw.starts_with('#') || head.is_some_and(|h| self.settings.is_parser_function(h)) {
            "parser_function"
        } else if self.settings.is_variable(name_raw) {
            "variable"
        } else {
            "template"
        };
        let split = matches!(kind, "module" | "parser_function").then_some(colon).flatten();
        // Magic words are not page names: fully lowercased. The first-letter rule is for templates only.
        let name = match (kind, split, head) {
            ("template", ..) => whole,
            (_, Some(_), Some(h)) => norm(h),
            _ => norm(name_raw),
        };
        let role = self
            .registry
            .template(&name, self.settings.site())
            .map_or_else(|| "other".to_string(), |e| e.role.clone());
        let depth = self.open.last().map_or(0, |o| self.rows[o.row].depth + 1);
        let (parent_type, parent_id) = parent_cols(parent);
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
        });
        id
    }

    /// The row of template `id`.
    pub(super) fn row(&self, id: usize) -> &TemplateRow {
        &self.rows[id]
    }

    /// The name part of the open template closes with the text after its colon. Order: the colon
    /// parameter is pushed (empty text) when the template opens, so it is `params[0]` here; the name part
    /// closes before any `Param`, so the later parameters are pushed after it.
    pub(super) fn set_split_text(&mut self, text: String) {
        if let Some(o) = self.open.last() {
            if let Some(p) = self.rows[o.row].params.first_mut() {
                p.value_text = text;
            }
        }
    }

    /// A parameter of the open template closes.
    pub(super) fn close_param(&mut self, p: ParamEvent) {
        let Some(o) = self.open.last_mut() else { return };
        let (name, positional) = match p.name {
            Some((a, b)) => (self.text.get(a..b).unwrap_or("").to_string(), false),
            None => {
                let n = o.next_pos;
                o.next_pos += 1;
                (n.to_string(), true)
            }
        };
        let (a, b) = p.value;
        let (start, end) = clamp_span(self.text, p.span.0, p.span.1);
        let row = ParamRow {
            name,
            positional,
            value_raw: self.text.get(a..b).unwrap_or("").to_string(),
            value_text: p.value_text,
            start: start as i32,
            end: end as i32,
        };
        self.rows[o.row].params.push(row);
    }

    /// The open template closes. Returns its id.
    pub(super) fn close(&mut self) -> Option<usize> {
        let o = self.open.pop()?;
        let row = &mut self.rows[o.row];
        let mut pairs: Vec<(&str, String)> =
            row.params.iter().map(|p| (p.name.as_str(), format!("{}={}", p.name, norm(&p.value_text)))).collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let joined = pairs.into_iter().map(|(_, s)| s).collect::<Vec<_>>().join("\u{1f}");
        row.fp = fp(&format!("{}\u{1f}{}", row.name, joined));
        Some(o.row)
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
