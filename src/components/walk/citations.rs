//! The citations table: one row per source a page cites, in document order.
//!
//! The walk (`walk/mod.rs`) tells this collector when it enters and leaves a ref, a template and an external
//! link. A template's parameters are known when it closes, so a row is reserved (a slot) when its source is
//! entered and filled when it is known; ids are the filled slots in order, dense from 0 in `finish`.
//!
//! # Bodies
//!
//! A *body* is what rows are grouped by: the inside of a ref, or, outside refs, the stretch of the page
//! that sits in one paragraph place (one section and paragraph: a bibliography entry, a list item).
//!
//! - A ref of a short footnote template (registry `family = "short"`, `{{sfn}}`) has no body: its arguments
//!   point at a source, they are none. A ref without a body (a reuse, an empty tag) has no rows.
//! - Every citation-role template that is not an archive wrapper is one row, unless it sits inside another
//!   such template's parameter. Its fields come from the registry entry's field map (see below).
//! - Inside a ref, every external link that is not inside a citation template is one `bare_link` row, with
//!   the link's label as `title`. Outside refs, bare links are no rows.
//! - An archive wrapper merges into the nearest preceding row of its body: it sets `archive_date_raw` and
//!   `archive_url` (the wrapper's `url` when that is an archive URL, else `wayback_url(url, date)`; no date,
//!   no archive URL), and gives the row its original as `url` when the row has none. A wrapper inside a
//!   citation template's parameter merges into that template's row. With nothing before it, it is a
//!   standalone row of the wrapper's family with the original as `url`.
//! - A `link_status` template sets `dead_link` on every row of its body.
//! - A ref body with no row yields one `plain_text` row, at the ref's position, with `text = body_text`.
//!
//! # Fields
//!
//! - `url` and `archive_url` come from the parameter's `value_raw` through `urls::analyse` (so `url` is the
//!   original of an archive URL, and the archive is `archive_url`); every other field from `value_text`.
//! - A scalar field takes the first parameter name in the entry's list that has a non-empty value; names match
//!   exactly. Of several parameters of one name, the last non-empty one counts (as MediaWiki reads it).
//! - `authors` collects, in source order, every parameter whose name (without trailing digits) is a base of
//!   the entry's `authors` list: `last`+`first` of one number as `"last, first"`; a `first` alone is none.
//! - `year` is the `year` field, else the first 4-digit run between 1000 and 2100 in `date_raw`.
//! - `template_id`, `template_name` and `params` are the template's; `harv_key` is left to Task 17.
//!
//! # Rows
//!
//! - A template row has the template's span and parent; a `bare_link` the link's span and its parent; a
//!   `plain_text` row the ref's span and the ref as parent. All are placed where the walk met them.
//! - `context` is `in_ref` with a `ref_id`; outside refs `bibliography` in a section of role `bibliography` or
//!   `external_links`, else `other`.
//! - `fp` = `fp(family + "\u{1f}" + (url, else norm(title + " " + authors) if any, else norm(text)))`.

use std::collections::HashSet;

use parse_wiki_text_2::Node;

use crate::fingerprint::{fp, norm};
use crate::model::{CitationRow, ParamRow, RefRow, SectionRow, TemplateRow};
use crate::registry::{Registry, TemplateEntry};
use crate::spans::CharIndex;
use crate::urls;

use super::{clamp_span, parent_cols, Parent, Place};

/// The ref a body belongs to.
struct RefInfo {
    id: i32,
    span: (usize, usize),
    /// The slot reserved for the ref's `plain_text` row.
    plain: usize,
}

#[derive(Default)]
struct Body {
    /// A short footnote: nothing in it is a source.
    skip: bool,
    refinfo: Option<RefInfo>,
    /// Out of refs: the place the body covers.
    key: Option<Place>,
    /// The slots of the body, in order.
    slots: Vec<usize>,
    dead: bool,
    /// Citation templates (not wrappers) open in this body.
    cite_depth: usize,
}

/// What an archive wrapper says.
struct Wrapper {
    archive: Option<String>,
    date: Option<String>,
    original: Option<String>,
}

struct OpenCite {
    id: i32,
    slot: Option<usize>,
    wrapper: bool,
    parent: Parent,
    ref_id: Option<i32>,
    wrapped: Vec<Wrapper>,
}

pub(super) struct Citations<'t, 's> {
    text: &'t str,
    chars: &'t CharIndex,
    registry: &'s Registry,
    site: &'s str,
    slots: Vec<Option<CitationRow>>,
    places: Vec<Place>,
    /// Open ref bodies, innermost last.
    bodies: Vec<Body>,
    /// The body out of refs.
    page: Body,
    open: Vec<OpenCite>,
    /// Slots of the bodies that had a dead-link template.
    dead: HashSet<usize>,
}

/// The first 4-digit run between 1000 and 2100.
fn year_in(s: &str) -> Option<i32> {
    let mut run = String::new();
    for c in s.chars().chain(std::iter::once(' ')) {
        if c.is_ascii_digit() {
            run.push(c);
            continue;
        }
        if run.len() == 4 {
            if let Some(y) = run.parse::<i32>().ok().filter(|y| (1000..=2100).contains(y)) {
                return Some(y);
            }
        }
        run.clear();
    }
    None
}

/// The last parameter named `name` with a non-empty value.
fn param<'x>(t: &'x TemplateRow, name: &str, raw: bool) -> Option<&'x ParamRow> {
    t.params.iter().rev().find(|p| p.name == name && !(if raw { &p.value_raw } else { &p.value_text }).is_empty())
}

/// The first listed parameter present and non-empty, for `key`.
fn pick<'x>(t: &'x TemplateRow, e: &TemplateEntry, key: &str, raw: bool) -> Option<&'x ParamRow> {
    e.fields.get(key)?.iter().find_map(|n| param(t, n, raw))
}

fn text_of(t: &TemplateRow, e: &TemplateEntry, key: &str) -> Option<String> {
    pick(t, e, key, false).map(|p| p.value_text.clone())
}

fn authors(t: &TemplateRow, e: &TemplateEntry) -> Vec<String> {
    let base = |n: &str| n.trim_end_matches(|c: char| c.is_ascii_digit()).to_string();
    let Some(names) = e.fields.get("authors") else { return Vec::new() };
    let bases: HashSet<String> = names.iter().map(|n| base(n)).filter(|b| !b.is_empty()).collect();
    let mut out = Vec::new();
    for p in &t.params {
        let b = base(&p.name);
        if p.value_text.is_empty() || !bases.contains(&b) {
            continue;
        }
        match b.as_str() {
            "first" => {}
            "last" => {
                let first = param(t, &format!("first{}", &p.name[b.len()..]), false);
                out.push(match first {
                    Some(f) => format!("{}, {}", p.value_text, f.value_text),
                    None => p.value_text.clone(),
                });
            }
            _ => out.push(p.value_text.clone()),
        }
    }
    out
}

/// The text of an external link's label: the nodes after the URL, as plain text.
fn label_of(nodes: &[Node]) -> Option<String> {
    fn add(n: &Node, out: &mut String) {
        match n {
            Node::Text { value, .. } => out.push_str(value),
            Node::CharacterEntity { character, .. } => out.push(*character),
            Node::Link { target, text, .. } if text.is_empty() => out.push_str(target),
            Node::Link { text, .. } => text.iter().for_each(|n| add(n, out)),
            _ => {}
        }
    }
    let mut out = String::new();
    let mut nodes = nodes.iter();
    // The URL is the start of the first text node, up to its first whitespace.
    if let Some(Node::Text { value, .. }) = nodes.next() {
        out.push_str(value.find(char::is_whitespace).map_or("", |i| &value[i..]));
    }
    nodes.for_each(|n| add(n, &mut out));
    let label = out.split_whitespace().collect::<Vec<_>>().join(" ");
    (!label.is_empty()).then_some(label)
}

impl<'t, 's> Citations<'t, 's> {
    pub(super) fn new(text: &'t str, chars: &'t CharIndex, registry: &'s Registry, site: &'s str) -> Self {
        Citations {
            text,
            chars,
            registry,
            site,
            slots: Vec::new(),
            places: Vec::new(),
            bodies: Vec::new(),
            page: Body::default(),
            open: Vec::new(),
            dead: HashSet::new(),
        }
    }

    /// The body that holds what is met at `place`: the innermost ref's, or the page's for that place.
    fn body_mut(&mut self, place: Place) -> &mut Body {
        if self.bodies.is_empty() && self.page.key != Some(place) && self.page.cite_depth == 0 {
            let old = std::mem::take(&mut self.page);
            self.close_body(&old);
            self.page.key = Some(place);
        }
        self.bodies.last_mut().unwrap_or(&mut self.page)
    }

    fn close_body(&mut self, b: &Body) {
        if b.dead && !b.skip {
            self.dead.extend(b.slots.iter().copied());
        }
    }

    fn blank(&self, span: (usize, usize), parent: Parent, family: &str, ref_id: Option<i32>) -> CitationRow {
        let (start, end) = clamp_span(self.text, span.0, span.1);
        let (parent_type, parent_id) = parent_cols(parent);
        CitationRow {
            id: 0,
            start: start as i32,
            end: end as i32,
            start_char: self.chars.char_of(start),
            end_char: self.chars.char_of(end),
            section_id: 0,
            para_id: None,
            para_offset: None,
            parent_type,
            parent_id,
            fallback: false,
            fp: String::new(),
            context: if ref_id.is_some() { "in_ref" } else { "other" }.to_string(),
            ref_id,
            family: family.to_string(),
            template_id: None,
            template_name: None,
            params: Vec::new(),
            url: None,
            archive_url: None,
            archive_date_raw: None,
            host: None,
            registrable_domain: None,
            dead_link: false,
            title: None,
            authors: Vec::new(),
            date_raw: None,
            year: None,
            publisher: None,
            work: None,
            language: None,
            isbn: None,
            doi: None,
            access_date_raw: None,
            harv_key: None,
            text: None,
        }
    }

    /// A slot in the body met at `place`; `None` in a body that has no sources.
    fn reserve(&mut self, place: Place) -> Option<usize> {
        let slot = self.slots.len();
        let body = self.body_mut(place);
        if body.skip {
            return None;
        }
        body.slots.push(slot);
        self.slots.push(None);
        self.places.push(place);
        Some(slot)
    }

    fn open_ref(&mut self, id: i32, span: (usize, usize), place: Place, skip: bool) {
        let plain = self.slots.len();
        let mut body = Body { skip, ..Body::default() };
        if !skip {
            body.slots.push(plain);
            self.slots.push(None);
            self.places.push(place);
        }
        body.refinfo = Some(RefInfo { id, span, plain });
        self.bodies.push(body);
    }

    /// A `<ref>` tag is entered.
    pub(super) fn enter_tag_ref(&mut self, id: i32, span: (usize, usize), place: Place) {
        self.open_ref(id, span, place, false);
    }

    /// A footnote template is entered; `t` is its row and `ref_id` its ref's id.
    pub(super) fn enter_footnote(&mut self, t: &TemplateRow, ref_id: i32, place: Place) {
        let short = self.registry.template(&t.name, self.site).is_some_and(|e| e.family.as_deref() == Some("short"));
        self.open_ref(ref_id, (t.start as usize, t.end as usize), place, short);
    }

    /// The ref entered last closes; `r` is its finished row.
    pub(super) fn close_ref(&mut self, r: &RefRow) {
        let Some(body) = self.bodies.pop() else { return };
        if body.skip {
            return;
        }
        if let Some(info) = &body.refinfo {
            let has_rows = body.slots.iter().any(|&s| s != info.plain && self.slots[s].is_some());
            if !has_rows && !r.body_raw.is_empty() {
                let mut row = self.blank(info.span, Some(("ref", info.id)), "plain_text", Some(info.id));
                row.text = Some(r.body_text.clone());
                self.slots[info.plain] = Some(row);
            }
        }
        self.close_body(&body);
    }

    /// An external link is entered.
    pub(super) fn enter_link(&mut self, nodes: &[Node], span: (usize, usize), place: Place, parent: Parent) {
        let Some(body) = self.bodies.last() else { return };
        let (Some(info), false, 0) = (&body.refinfo, body.skip, body.cite_depth) else { return };
        let ref_id = info.id;
        let (start, end) = clamp_span(self.text, span.0, span.1);
        let raw = self.text.get(start..end).unwrap_or("");
        let inner = raw.strip_prefix('[').unwrap_or(raw);
        let url_raw = inner.split(|c: char| c.is_whitespace() || c == ']').next().unwrap_or("");
        let Some(slot) = self.reserve(place) else { return };
        let mut row = self.blank((start, end), parent, "bare_link", Some(ref_id));
        set_url(&mut row, url_raw);
        row.title = label_of(nodes);
        self.slots[slot] = Some(row);
    }

    /// A template is entered; `parent` is the template's own parent.
    pub(super) fn enter_template(&mut self, t: &TemplateRow, place: Place, parent: Parent) {
        let Some(e) = self.registry.template(&t.name, self.site) else { return };
        match e.role.as_str() {
            "link_status" => self.body_mut(place).dead = true,
            "citation" => {
                let nested = self.body_mut(place).cite_depth > 0;
                let slot = if nested { None } else { self.reserve(place) };
                let ref_id = self.bodies.last().and_then(|b| b.refinfo.as_ref()).map(|i| i.id);
                if !e.archive_wrapper {
                    self.body_mut(place).cite_depth += 1;
                }
                self.open.push(OpenCite { id: t.id, slot, wrapper: e.archive_wrapper, parent, ref_id, wrapped: Vec::new() });
            }
            _ => {}
        }
    }

    /// A template closes with its parameters.
    pub(super) fn close_template(&mut self, t: &TemplateRow) {
        if self.open.last().map(|o| o.id) != Some(t.id) {
            return;
        }
        let Some(o) = self.open.pop() else { return };
        let Some(e) = self.registry.template(&t.name, self.site) else { return };
        let body = self.bodies.last_mut().unwrap_or(&mut self.page);
        let nested = body.cite_depth > 0;
        if !o.wrapper {
            body.cite_depth = body.cite_depth.saturating_sub(1);
        }
        if o.wrapper {
            let w = wrapper_of(t, e);
            if nested {
                if let Some(outer) = self.open.iter_mut().rev().find(|c| !c.wrapper) {
                    outer.wrapped.push(w);
                }
            } else if let Some(slot) = o.slot {
                self.merge_or_stand(slot, w, t, e, o.parent, o.ref_id);
            }
            return;
        }
        let Some(slot) = o.slot else { return };
        let family = e.family.as_deref().unwrap_or("other_template");
        let mut row = self.blank((t.start as usize, t.end as usize), o.parent, family, o.ref_id);
        fill_from_template(&mut row, t, e);
        for w in &o.wrapped {
            apply_wrapper(&mut row, w);
        }
        self.slots[slot] = Some(row);
    }

    /// An archive wrapper merges into the nearest preceding row of its body, else stands alone in `slot`.
    fn merge_or_stand(&mut self, slot: usize, w: Wrapper, t: &TemplateRow, e: &TemplateEntry, parent: Parent, ref_id: Option<i32>) {
        let body = self.bodies.last().unwrap_or(&self.page);
        let prev = body.slots.iter().rev().copied().find(|&s| s < slot && self.slots[s].is_some());
        match prev {
            Some(p) => {
                if let Some(row) = self.slots[p].as_mut() {
                    apply_wrapper(row, &w);
                }
            }
            None => {
                let mut row = self.blank((t.start as usize, t.end as usize), parent, e.family.as_deref().unwrap_or("web"), ref_id);
                row.template_id = Some(t.id);
                row.template_name = Some(t.name.clone());
                row.params = t.params.clone();
                if let Some(o) = &w.original {
                    set_url(&mut row, o);
                }
                apply_wrapper(&mut row, &w);
                self.slots[slot] = Some(row);
            }
        }
    }

    /// The rows with ids, contexts, places resolved by `resolve`, and fingerprints.
    pub(super) fn finish(
        mut self,
        resolve: impl Fn(Place) -> (Option<i32>, Option<i32>),
        sections: &[SectionRow],
    ) -> Vec<CitationRow> {
        let page = std::mem::take(&mut self.page);
        self.close_body(&page);
        let mut rows = Vec::new();
        for (i, (slot, place)) in self.slots.into_iter().zip(&self.places).enumerate() {
            let Some(mut row) = slot else { continue };
            row.id = rows.len() as i32;
            row.section_id = place.section_id;
            (row.para_id, row.para_offset) = resolve(*place);
            row.dead_link = self.dead.contains(&i);
            if row.ref_id.is_none() {
                let role = sections.get(place.section_id as usize).map_or("", |s| s.role.as_str());
                row.context = if matches!(role, "bibliography" | "external_links") { "bibliography" } else { "other" }.to_string();
            }
            let identity = match (&row.url, row.title.is_some() || !row.authors.is_empty()) {
                (Some(u), _) => u.clone(),
                (None, true) => norm(&format!("{} {}", row.title.as_deref().unwrap_or(""), row.authors.join(" "))),
                (None, false) => norm(row.text.as_deref().unwrap_or("")),
            };
            row.fp = fp(&format!("{}\u{1f}{}", row.family, identity));
            rows.push(row);
        }
        rows
    }
}

/// Sets `url` (the original), `archive_url`, `host` and `registrable_domain` from a URL as written.
fn set_url(row: &mut CitationRow, raw: &str) {
    let a = urls::analyse(raw);
    row.url = Some(a.url);
    if a.archive_url.is_some() {
        row.archive_url = a.archive_url;
    }
    row.host = a.host;
    row.registrable_domain = a.registrable_domain;
}

fn fill_from_template(row: &mut CitationRow, t: &TemplateRow, e: &TemplateEntry) {
    row.template_id = Some(t.id);
    row.template_name = Some(t.name.clone());
    row.params = t.params.clone();
    if let Some(p) = pick(t, e, "url", true) {
        set_url(row, &p.value_raw);
    }
    if let Some(p) = pick(t, e, "archive_url", true) {
        let a = urls::analyse(&p.value_raw);
        let archived = a.archive_url.is_some();
        row.archive_url = Some(a.archive_url.unwrap_or_else(|| a.url.clone()));
        if row.url.is_none() && archived {
            set_url(row, &a.url);
        }
    }
    row.archive_date_raw = text_of(t, e, "archive_date_raw");
    row.title = text_of(t, e, "title");
    row.authors = authors(t, e);
    row.date_raw = text_of(t, e, "date_raw");
    row.year = text_of(t, e, "year").and_then(|y| year_in(&y)).or_else(|| row.date_raw.as_deref().and_then(year_in));
    row.publisher = text_of(t, e, "publisher");
    row.work = text_of(t, e, "work");
    row.language = text_of(t, e, "language");
    row.isbn = text_of(t, e, "isbn");
    row.doi = text_of(t, e, "doi");
    row.access_date_raw = text_of(t, e, "access_date_raw");
}

fn wrapper_of(t: &TemplateRow, e: &TemplateEntry) -> Wrapper {
    let date = text_of(t, e, "archive_date_raw");
    let Some(p) = pick(t, e, "archive_url", true) else { return Wrapper { archive: None, date, original: None } };
    let a = urls::analyse(&p.value_raw);
    match a.archive_url {
        Some(archive) => Wrapper { archive: Some(archive), date, original: Some(a.url) },
        None => {
            let archive = date.as_deref().map(|d| urls::wayback_url(&a.url, d));
            Wrapper { archive, date, original: Some(a.url) }
        }
    }
}

fn apply_wrapper(row: &mut CitationRow, w: &Wrapper) {
    if w.date.is_some() {
        row.archive_date_raw = w.date.clone();
    }
    if w.archive.is_some() {
        row.archive_url = w.archive.clone();
    }
    if row.url.is_none() {
        if let Some(o) = &w.original {
            let a = urls::analyse(o);
            row.url = Some(a.url);
            row.host = a.host;
            row.registrable_domain = a.registrable_domain;
        }
    }
}
