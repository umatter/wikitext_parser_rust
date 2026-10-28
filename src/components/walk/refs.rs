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
//! - **Empty bodies.** A body that is only whitespace and HTML comments is empty (`body_raw` and `body_text`
//!   `""`): MediaWiki strips comments before it looks at the body.
//! - **Kinds.** A ref whose nearest enclosing ref or ref list is a ref list (a `<references>` tag, or a
//!   `ref_list` template, in any parameter and however deep in other templates) is `list_defined`. Otherwise a
//!   named ref with a body is `named`, a named ref without one `reuse` (also `<ref name=a></ref>`, which
//!   MediaWiki reads as `<ref name=a/>`), and an unnamed one `inline`.
//! - **Footnote templates** (registry role `footnote`) are refs of kind `template`; the node is also a row of
//!   the templates table (two views of one node).
//!   - The body of a short footnote (registry `family = "short"`, `{{sfn}}`) is all its parameters:
//!     `body_raw` is their source, trimmed (after the name's `|`, before `}}`), and `body_text` their
//!     `value_text`s joined by one space in source order, a named one as `name=value_text`. So
//!     `{{sfn|Иванов|1990|с=5}}` and `{{sfn|Иванов|2001|с=7}}` differ.
//!   - The body of any other footnote template is parameter `1` (the last one so named, as `{{{1}}}` reads
//!     it, so `{{efn|1=a = b}}` counts): `body_raw` is its `value_raw` and `body_text` its `value_text`.
//!   - `name` and `group` come from the `name=` and `group=` parameters (the last of each), with one pair of
//!     surrounding quotes removed, as `#tag` removes them (`{{efn|group="К"}}` is group `К`). Without a
//!     `group=` parameter the group is the registry entry's `default_group`, else `""`.
//! - **Ref lists.** A `<references>` tag is kind `tag`, with `group` from its attributes; a `ref_list`
//!   template is kind `template`, with `template_id`, and `group` from its `group=` parameter (unquoted as
//!   above), else the entry's `default_group`, else `""`.
//! - A list-defined ref takes the list's `section_id`, and the list's group unless it names its own.
//! - Parents (`parent_type`, `parent_id`) are given by the walk: the nearest enclosing template, ref or
//!   `<references>` tag.
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
//!   not a later copy of an earlier definition. Identical short footnotes (same group and `norm(body_text)`)
//!   count once, as MediaWiki merges them; each call keeps its own row.
//! - `fp` = `fp(kind + "\u{1f}" + group + "\u{1f}" + name + "\u{1f}" + norm(body_text))`; a list's `fp` =
//!   `fp(kind + "\u{1f}" + group + "\u{1f}" + template name)`, the name `""` for a tag.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use crate::attrs;
use crate::fingerprint::{fp, norm};
use crate::model::{RefListRow, RefRow, TemplateRow};
use crate::registry::{Registry, TemplateEntry};
use crate::spans::CharIndex;

use super::{clamp_span, parent_cols, Parent, Place};

/// What a ref or a ref list is inside, innermost last.
#[derive(Clone, Copy, Debug)]
enum Container {
    Ref(usize),
    List(usize),
}

pub(super) struct Refs<'t, 's> {
    text: &'t str,
    chars: &'t CharIndex,
    registry: &'s Registry,
    site: &'s str,
    rows: Vec<RefRow>,
    places: Vec<Place>,
    /// Per ref: the list it is defined in, whether it names its own group, and whether it is a short footnote.
    list_of: Vec<Option<usize>>,
    own_group: Vec<bool>,
    short: Vec<bool>,
    lists: Vec<RefListRow>,
    list_places: Vec<Place>,
    /// Per list: the template's name, `""` for a tag.
    list_names: Vec<String>,
    open: Vec<Container>,
}

/// The value of the last attribute named `key`, unless it is empty.
fn last_attr<'x>(attrs: &'x [(String, String)], key: &str) -> Option<&'x str> {
    attrs.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v.as_str()).filter(|v| !v.is_empty())
}

/// The `value_text` of the last parameter named `key`, with one pair of surrounding quotes removed, as `#tag`
/// passes a parameter on as an attribute (`CoreParserFunctions::tagObj`: `"К"` and `'К'` are `К`, `""` and
/// `''` are empty).
fn last_param<'x>(t: &'x TemplateRow, key: &str) -> Option<&'x str> {
    let v = t.params.iter().rev().find(|p| p.name == key)?.value_text.as_str();
    let quote = |c: char| c == '"' || c == '\'';
    let mut ends = v.chars();
    let (first, last) = (ends.next(), ends.next_back());
    let quoted = first.is_some_and(quote) && last.is_some_and(quote);
    Some(match (quoted, ends.as_str()) {
        (true, "") if v == "\"\"" || v == "''" => "",
        (true, inner) if !inner.is_empty() => inner,
        _ => v,
    })
}

/// `s` without its HTML comments; an unclosed `<!--` runs to the end.
fn without_comments(s: &str) -> Cow<'_, str> {
    if !s.contains("<!--") {
        return Cow::Borrowed(s);
    }
    let (mut out, mut rest) = (String::new(), s);
    while let Some(i) = rest.find("<!--") {
        out.push_str(&rest[..i]);
        rest = rest[i + 4..].find("-->").map_or("", |j| &rest[i + 4 + j + 3..]);
    }
    out.push_str(rest);
    Cow::Owned(out)
}

/// A body that is only whitespace and comments.
fn blank(s: &str) -> bool {
    without_comments(s).trim().is_empty()
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

/// The body of a short footnote: the source of all its parameters, and their texts joined.
fn short_body(text: &str, t: &TemplateRow) -> (String, String) {
    let Some(first) = t.params.first() else { return (String::new(), String::new()) };
    let start = first.start as usize;
    let start = start + usize::from(text.get(start..).is_some_and(|s| s.starts_with('|')));
    let end = t.end as usize;
    let end = end.checked_sub(2).filter(|&e| text.get(e..end) == Some("}}")).unwrap_or(end);
    let raw = text.get(start..end.max(start)).unwrap_or("").trim().to_string();
    let parts = t.params.iter().map(|p| match p.positional {
        true => p.value_text.clone(),
        false => format!("{}={}", p.name, p.value_text),
    });
    let joined = parts.filter(|s| !s.is_empty()).collect::<Vec<_>>().join(" ");
    (raw, joined)
}

impl<'t, 's> Refs<'t, 's> {
    pub(super) fn new(text: &'t str, chars: &'t CharIndex, registry: &'s Registry, site: &'s str) -> Self {
        Refs {
            text,
            chars,
            registry,
            site,
            rows: Vec::new(),
            places: Vec::new(),
            list_of: Vec::new(),
            own_group: Vec::new(),
            short: Vec::new(),
            lists: Vec::new(),
            list_places: Vec::new(),
            list_names: Vec::new(),
            open: Vec::new(),
        }
    }

    fn entry(&self, t: &TemplateRow) -> Option<&'s TemplateEntry> {
        self.registry.template(&t.name, self.site)
    }

    /// The group of a footnote or ref-list template: its `group=` parameter, else its entry's default.
    fn template_group(&self, t: &TemplateRow) -> String {
        match last_param(t, "group") {
            Some(g) => g.to_string(),
            None => self.entry(t).and_then(|e| e.default_group.clone()).unwrap_or_default(),
        }
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
        self.short.push(false);
        self.open.push(Container::Ref(id));
        id
    }

    fn push_list(&mut self, span: (usize, usize), place: Place, parent: Parent, kind: &str, template: Option<&TemplateRow>) -> i32 {
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
            template_id: template.map(|t| t.id),
            n_entries: 0,
        });
        self.list_places.push(place);
        self.list_names.push(template.map(|t| t.name.clone()).unwrap_or_default());
        self.open.push(Container::List(id));
        id as i32
    }

    /// A `<ref>` tag spanning `start..end` is entered at `place`. Returns the ref's id.
    pub(super) fn enter_tag(&mut self, start: usize, end: usize, place: Place, parent: Parent) -> i32 {
        let span = clamp_span(self.text, start, end);
        let slice = self.text.get(span.0..span.1).unwrap_or("");
        let attrs = attrs::tag_attrs(attrs::open_tag(slice));
        let name = last_attr(&attrs, "name").map(str::to_string);
        let group = last_attr(&attrs, "group").map(str::to_string);
        let body = tag_body(slice, "ref");
        let body_raw = if blank(body) { String::new() } else { body.trim().to_string() };
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
            let row = &mut self.rows[id];
            row.body_text = if row.body_raw.is_empty() { String::new() } else { body_text };
        }
    }

    /// A footnote template is entered at `place`; its row in the templates table is `t`. Returns the ref's id.
    pub(super) fn enter_template(&mut self, t: &TemplateRow, place: Place, parent: Parent) -> i32 {
        self.push_ref(clamp_span(self.text, t.start as usize, t.end as usize), place, parent, "template") as i32
    }

    /// The footnote template entered last closes; `t` is its row, with its parameters.
    pub(super) fn close_template(&mut self, t: &TemplateRow) {
        let Some(Container::Ref(id)) = self.open.pop() else { return };
        let short = self.entry(t).is_some_and(|e| e.family.as_deref() == Some("short"));
        let (raw, body_text) = match short {
            true => short_body(self.text, t),
            false => t
                .params
                .iter()
                .rev()
                .find(|p| p.name == "1")
                .map(|p| (p.value_raw.clone(), p.value_text.clone()))
                .unwrap_or_default(),
        };
        let group = self.template_group(t);
        let row = &mut self.rows[id];
        (row.body_raw, row.body_text) = if blank(&raw) { Default::default() } else { (raw, body_text) };
        row.name = last_param(t, "name").filter(|n| !n.is_empty()).map(str::to_string);
        row.group = group;
        self.short[id] = short;
    }

    /// A `<references>` tag spanning `start..end` is entered at `place`. Returns the list's id.
    pub(super) fn enter_list_tag(&mut self, start: usize, end: usize, place: Place, parent: Parent) -> i32 {
        let span = clamp_span(self.text, start, end);
        let slice = self.text.get(span.0..span.1).unwrap_or("");
        let attrs = attrs::tag_attrs(attrs::open_tag(slice));
        let group = last_attr(&attrs, "group").unwrap_or_default().to_string();
        let id = self.push_list(span, place, parent, "tag", None);
        self.lists[id as usize].group = group;
        id
    }

    /// A ref-list template is entered at `place`; its row in the templates table is `t`.
    pub(super) fn enter_list_template(&mut self, t: &TemplateRow, place: Place, parent: Parent) {
        self.push_list(clamp_span(self.text, t.start as usize, t.end as usize), place, parent, "template", Some(t));
    }

    /// The ref list entered last closes; `t` is its template row for a ref-list template.
    pub(super) fn close_list(&mut self, t: Option<&TemplateRow>) {
        let Some(Container::List(id)) = self.open.pop() else { return };
        if let Some(t) = t {
            self.lists[id].group = self.template_group(t);
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
        let mut short_seen: HashSet<(String, String)> = HashSet::new();
        for (i, r) in self.rows.iter_mut().enumerate() {
            let k = key(r);
            r.def_ref_id = k.as_ref().and_then(|k| defs.get(k).copied());
            r.n_uses = match (&k, r.kind.as_str()) {
                (Some(k), "named" | "list_defined") => uses.get(k).copied(),
                _ => None,
            };
            r.resolved = has_body(r) || r.def_ref_id.is_some();
            let body_norm = norm(&r.body_text);
            let definition = has_body(r) && r.def_ref_id.is_none_or(|d| d == r.id);
            if definition && (!self.short[i] || short_seen.insert((r.group.clone(), body_norm.clone()))) {
                *entries.entry(r.group.clone()).or_default() += 1;
            }
            let name = r.name.as_deref().unwrap_or("");
            r.fp = fp(&format!("{}\u{1f}{}\u{1f}{}\u{1f}{}", r.kind, r.group, name, body_norm));
        }
        for (l, name) in self.lists.iter_mut().zip(&self.list_names) {
            l.n_entries = entries.get(&l.group).copied().unwrap_or(0);
            l.fp = fp(&format!("{}\u{1f}{}\u{1f}{}", l.kind, l.group, name));
        }
        (self.rows, self.lists)
    }
}

#[cfg(test)]
mod tests {
    use super::{blank, tag_body, without_comments};

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

    #[test]
    fn comments_are_not_a_body() {
        assert_eq!(without_comments("a<!-- x -->b<!-- y"), "ab");
        assert_eq!(without_comments("a<!---->b"), "ab");
        assert!(blank(" <!-- x -->\n<!-- y --> "));
        assert!(blank("<!-- unclosed"));
        assert!(!blank("<!-- x --> Y"));
        assert!(blank(""));
    }
}
