//! The refs and ref_lists tables: one row per reference and per reference list, in document order.
//!
//! The walk (`walk/mod.rs`) tells this collector when it enters and leaves a `<ref>` or `<references>` tag and
//! a footnote or ref-list template (by registry role), and hands it the text of each tag ref's body (a
//! `Sink::Flat`, under the text rules of `text.rs`). Kinds, groups and resolution are settled in `finish`,
//! once the whole page is known.
//!
//! # Rows
//!
//! - **Tag refs.** Attributes come from `attrs::tag_attrs(attrs::open_tag(..))` over the tag's span; when a
//!   name or group is given twice, the last one counts (MediaWiki). An empty `name` is no name, an empty
//!   `group` the default group `""`. `body_raw` is the source between the open tag (which the parser ends at
//!   its first `>`) and the closing tag, trimmed; `""` for a self-closing tag.
//! - **Kinds.** A ref whose nearest enclosing ref or ref list is a ref list (a `<references>` tag, or a
//!   `ref_list` template, in any parameter and however deep in other templates) is `list_defined`. Otherwise a
//!   named ref with a body is `named`, a named ref without one `reuse` (also `<ref name=a></ref>`, which
//!   MediaWiki reads as `<ref name=a/>`), and an unnamed one `inline`.
//! - **Footnote templates** (registry role `footnote`) are refs of kind `template`. The body is parameter `1`
//!   (the last one so named, as `{{{1}}}` reads it, so `{{efn|1=a = b}}` counts): `body_raw` is its
//!   `value_raw` and `body_text` its `value_text`. `name` and `group` come from the `name=` and `group=`
//!   parameters (the last of each). The node is also a row of the templates table: two views of one node.
//! - **Ref lists.** A `<references>` tag is kind `tag`, with `group` from its attributes; a `ref_list`
//!   template is kind `template`, with `template_id` and `group` from its `group=` parameter.
//! - A list-defined ref takes the list's `section_id`, and the list's group unless it names its own.
//! - Parents (`parent_type`, `parent_id`) are given by the walk: the nearest enclosing template or ref. A ref
//!   list is no parent, so a ref in a `<references>` tag has none unless a template or a ref is around.
//!
//! # Resolution, per (group, name) over the whole page
//!
//! - A ref *has a body* when it is not a `reuse` and its `body_raw` is not empty. The first named ref with a
//!   body, in document order, is the definition of its (group, name). A later one with a body keeps its kind
//!   (`named`, `list_defined` or `template`) and is not a second definition.
//! - Every named ref gets `def_ref_id` = that definition, the definition itself included; `None` without one.
//!   An unnamed ref has `None`.
//! - `n_uses` (on `named` and named `list_defined` refs, else `None`) counts the refs of its (group, name): the
//!   definition and everything that points to it.
//! - `resolved` is true for a ref with a body and for any ref whose (group, name) has a definition (a reuse,
//!   a footnote template given only a name); false otherwise. Task 17 settles it for `{{sfn}}`.
//! - A list's `n_entries` is the number of definitions in its group on the page: refs with a body that are
//!   not a later copy of an earlier definition.
//! - `fp` = `fp(kind + "\u{1f}" + group + "\u{1f}" + name + "\u{1f}" + norm(body_text))`; a list's `fp` =
//!   `fp(kind + "\u{1f}" + group)`.

use std::collections::HashMap;

use crate::attrs;
use crate::fingerprint::{fp, norm};
use crate::model::{RefListRow, RefRow, TemplateRow};
use crate::spans::CharIndex;

use super::{Parent, Place};

/// What a ref or a ref list is inside, innermost last.
#[derive(Clone, Copy, Debug)]
enum Container {
    Ref(usize),
    List(usize),
}

pub(super) struct Refs<'t> {
    text: &'t str,
    chars: &'t CharIndex,
    rows: Vec<RefRow>,
    places: Vec<Place>,
    /// Per ref: the list it is defined in, and whether it names its own group.
    list_of: Vec<Option<usize>>,
    own_group: Vec<bool>,
    lists: Vec<RefListRow>,
    list_places: Vec<Place>,
    open: Vec<Container>,
}

/// The value of the last attribute or parameter named `key`, unless it is empty.
fn last_value<'x>(pairs: impl DoubleEndedIterator<Item = (&'x str, &'x str)>, key: &str) -> Option<&'x str> {
    pairs.rev().find(|(k, _)| *k == key).map(|(_, v)| v).filter(|v| !v.is_empty())
}

/// The body of an extension tag: from the first `>` (where the parser ends the open tag) to the closing tag
/// `</name>` at the end of the slice. `""` when the slice does not end with that closing tag (self-closing).
fn tag_body<'x>(slice: &'x str, name: &str) -> &'x str {
    let (Some(open_end), Some(close)) = (slice.find('>'), slice.rfind("</")) else { return "" };
    let closed = slice[close + 2..]
        .strip_suffix('>')
        .is_some_and(|n| n.trim_end_matches([' ', '\t', '\n']).eq_ignore_ascii_case(name));
    match closed && close > open_end {
        true => &slice[open_end + 1..close],
        false => "",
    }
}

fn parent_cols(parent: Parent) -> (Option<String>, Option<i32>) {
    match parent {
        Some((t, id)) => (Some(t.to_string()), Some(id)),
        None => (None, None),
    }
}

impl<'t> Refs<'t> {
    pub(super) fn new(text: &'t str, chars: &'t CharIndex) -> Self {
        Refs {
            text,
            chars,
            rows: Vec::new(),
            places: Vec::new(),
            list_of: Vec::new(),
            own_group: Vec::new(),
            lists: Vec::new(),
            list_places: Vec::new(),
            open: Vec::new(),
        }
    }

    /// The span clamped into the text.
    fn span(&self, start: usize, end: usize) -> (usize, usize) {
        let len = self.text.len();
        let start = start.min(len);
        (start, end.clamp(start, len))
    }

    /// The list a ref entered now is defined in: the nearest open ref or list, if it is a list.
    fn current_list(&self) -> Option<usize> {
        match self.open.last() {
            Some(Container::List(l)) => Some(*l),
            _ => None,
        }
    }

    fn push_ref(&mut self, span: (usize, usize), place: Place, parent: Parent, kind: &str) -> usize {
        let id = self.rows.len();
        let (parent_type, parent_id) = parent_cols(parent);
        self.rows.push(RefRow {
            id: id as i32,
            start: span.0 as i32,
            end: span.1 as i32,
            start_char: self.chars.char_of(span.0),
            end_char: self.chars.char_of(span.1),
            section_id: place.section_id,
            para_id: None,
            para_offset: None,
            parent_type,
            parent_id,
            fallback: false,
            fp: String::new(),
            kind: kind.to_string(),
            name: None,
            group: String::new(),
            body_raw: String::new(),
            body_text: String::new(),
            def_ref_id: None,
            n_uses: None,
            resolved: false,
            harv_key: None,
            cites_citation_id: None,
        });
        self.places.push(place);
        self.list_of.push(None);
        self.own_group.push(false);
        self.open.push(Container::Ref(id));
        id
    }

    fn push_list(&mut self, span: (usize, usize), place: Place, parent: Parent, kind: &str, template_id: Option<i32>) {
        let id = self.lists.len();
        let (parent_type, parent_id) = parent_cols(parent);
        self.lists.push(RefListRow {
            id: id as i32,
            start: span.0 as i32,
            end: span.1 as i32,
            start_char: self.chars.char_of(span.0),
            end_char: self.chars.char_of(span.1),
            section_id: place.section_id,
            para_id: None,
            para_offset: None,
            parent_type,
            parent_id,
            fallback: false,
            fp: String::new(),
            group: String::new(),
            kind: kind.to_string(),
            template_id,
            n_entries: 0,
        });
        self.list_places.push(place);
        self.open.push(Container::List(id));
    }

    /// A `<ref>` tag spanning `start..end` is entered at `place`. Returns the ref's id.
    pub(super) fn enter_tag(&mut self, start: usize, end: usize, place: Place, parent: Parent) -> i32 {
        let span = self.span(start, end);
        let slice = self.text.get(span.0..span.1).unwrap_or("");
        let attrs = attrs::tag_attrs(attrs::open_tag(slice));
        let pairs = || attrs.iter().map(|(k, v)| (k.as_str(), v.as_str()));
        let name = last_value(pairs(), "name").map(str::to_string);
        let group = last_value(pairs(), "group").map(str::to_string);
        let body_raw = tag_body(slice, "ref").trim().to_string();
        let list = self.current_list();
        let kind = match (list, &name) {
            (Some(_), _) => "list_defined",
            (None, Some(_)) if body_raw.is_empty() => "reuse",
            (None, Some(_)) => "named",
            (None, None) => "inline",
        };
        let id = self.push_ref(span, place, parent, kind);
        let row = &mut self.rows[id];
        row.name = name;
        row.body_raw = body_raw;
        self.own_group[id] = group.is_some();
        row.group = group.unwrap_or_default();
        self.list_of[id] = list;
        id as i32
    }

    /// The `<ref>` tag entered last closes, with the text of its body.
    pub(super) fn close_tag(&mut self, body_text: String) {
        if let Some(Container::Ref(id)) = self.open.pop() {
            self.rows[id].body_text = body_text;
        }
    }

    /// A footnote template is entered at `place`; its row in the templates table is `t`. Returns the ref's id.
    pub(super) fn enter_template(&mut self, t: &TemplateRow, place: Place, parent: Parent) -> i32 {
        self.push_ref(self.span(t.start as usize, t.end as usize), place, parent, "template") as i32
    }

    /// The footnote template entered last closes; `t` is its row, with its parameters.
    pub(super) fn close_template(&mut self, t: &TemplateRow) {
        let Some(Container::Ref(id)) = self.open.pop() else { return };
        let params = || t.params.iter();
        let row = &mut self.rows[id];
        if let Some(p) = params().rev().find(|p| p.name == "1") {
            row.body_raw = p.value_raw.clone();
            row.body_text = p.value_text.clone();
        }
        let values = || params().map(|p| (p.name.as_str(), p.value_text.as_str()));
        row.name = last_value(values(), "name").map(str::to_string);
        row.group = last_value(values(), "group").unwrap_or_default().to_string();
    }

    /// A `<references>` tag spanning `start..end` is entered at `place`.
    pub(super) fn enter_list_tag(&mut self, start: usize, end: usize, place: Place, parent: Parent) {
        let span = self.span(start, end);
        let slice = self.text.get(span.0..span.1).unwrap_or("");
        let attrs = attrs::tag_attrs(attrs::open_tag(slice));
        let group = last_value(attrs.iter().map(|(k, v)| (k.as_str(), v.as_str())), "group").unwrap_or_default();
        let group = group.to_string();
        self.push_list(span, place, parent, "tag", None);
        self.lists.last_mut().expect("just pushed").group = group;
    }

    /// A ref-list template is entered at `place`; its row in the templates table is `t`.
    pub(super) fn enter_list_template(&mut self, t: &TemplateRow, place: Place, parent: Parent) {
        self.push_list(self.span(t.start as usize, t.end as usize), place, parent, "template", Some(t.id));
    }

    /// The ref list entered last closes; `t` is its template row for a ref-list template.
    pub(super) fn close_list(&mut self, t: Option<&TemplateRow>) {
        let Some(Container::List(id)) = self.open.pop() else { return };
        if let Some(t) = t {
            let values = t.params.iter().map(|p| (p.name.as_str(), p.value_text.as_str()));
            self.lists[id].group = last_value(values, "group").unwrap_or_default().to_string();
        }
    }

    /// The rows, with list groups and sections, resolution and fingerprints settled, and each place
    /// resolved by `resolve` into `para_id` and `para_offset`.
    pub(super) fn finish(
        mut self,
        resolve: impl Fn(Place) -> (Option<i32>, Option<i32>),
    ) -> (Vec<RefRow>, Vec<RefListRow>) {
        for (row, place) in self.rows.iter_mut().zip(&self.places) {
            (row.para_id, row.para_offset) = resolve(*place);
        }
        for (list, place) in self.lists.iter_mut().zip(&self.list_places) {
            (list.para_id, list.para_offset) = resolve(*place);
        }
        for (i, row) in self.rows.iter_mut().enumerate() {
            if let Some(l) = self.list_of[i] {
                row.section_id = self.lists[l].section_id;
                if !self.own_group[i] {
                    row.group = self.lists[l].group.clone();
                }
            }
        }

        let has_body = |r: &RefRow| r.kind != "reuse" && !r.body_raw.is_empty();
        let key = |r: &RefRow| r.name.clone().map(|n| (r.group.clone(), n));
        let mut defs: HashMap<(String, String), i32> = HashMap::new();
        let mut uses: HashMap<(String, String), i32> = HashMap::new();
        for r in &self.rows {
            if let Some(k) = key(r) {
                if has_body(r) {
                    defs.entry(k.clone()).or_insert(r.id);
                }
                *uses.entry(k).or_default() += 1;
            }
        }
        let mut entries: HashMap<String, i32> = HashMap::new();
        for r in &mut self.rows {
            let k = key(r);
            r.def_ref_id = k.as_ref().and_then(|k| defs.get(k).copied());
            r.n_uses = match (&k, r.kind.as_str()) {
                (Some(k), "named" | "list_defined") => uses.get(k).copied(),
                _ => None,
            };
            r.resolved = has_body(r) || r.def_ref_id.is_some();
            if has_body(r) && r.def_ref_id.is_none_or(|d| d == r.id) {
                *entries.entry(r.group.clone()).or_default() += 1;
            }
            let name = r.name.as_deref().unwrap_or("");
            r.fp = fp(&format!("{}\u{1f}{}\u{1f}{}\u{1f}{}", r.kind, r.group, name, norm(&r.body_text)));
        }
        for l in &mut self.lists {
            l.n_entries = entries.get(&l.group).copied().unwrap_or(0);
            l.fp = fp(&format!("{}\u{1f}{}", l.kind, l.group));
        }
        (self.rows, self.lists)
    }
}

#[cfg(test)]
mod tests {
    use super::tag_body;

    #[test]
    fn tag_bodies() {
        assert_eq!(tag_body("<ref>x</ref>", "ref"), "x");
        assert_eq!(tag_body("<ref name=\"a\">x y</REF >", "ref"), "x y");
        assert_eq!(tag_body("<ref name=a />", "ref"), "");
        assert_eq!(tag_body("<ref/>", "ref"), "");
        assert_eq!(tag_body("<ref></ref>", "ref"), "");
        assert_eq!(tag_body("<ref>a</b>", "ref"), "");
        assert_eq!(tag_body("<ref>ж</ref>", "ref"), "ж");
        assert_eq!(tag_body("<ref>Ж</рф>", "ref"), "");
        assert_eq!(tag_body("", "ref"), "");
    }
}
