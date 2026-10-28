//! The component builders behind both paths (R52): the walk and the fallback scanner describe a page to
//! `Collect` as the same plain events, in document order, and every row comes from the collectors here.
//!
//! # Events
//!
//! - **Heading** (`heading`, then `heading_title`): its level, span and raw title, then its title text.
//! - **Template** (`template_open`, `template_split_text`, `template_param`, `template_close`): on enter its
//!   trimmed name span, the first colon of the name's own text and its span; for a module or a parser function
//!   (`split_from`) the text after that colon; each parameter (`ParamEvent`: name and value spans, span,
//!   `value_text`) while it is the innermost open template; then its exit.
//! - **`<ref>` tag** (`ref_open`, `ref_close`): its span, from which `refs.rs` reads the attributes and the
//!   body, then the text of its body.
//! - **`<references>` tag** (`ref_list_open`, `ref_list_close`): its span.
//! - **External link** (`external_link`): its span, from which `citations.rs` reads the URL, and the text of
//!   its label, asked for only when a `bare_link` row keeps it.
//!
//! Every event carries the `Place` the caller met it at. Texts follow the text rules of `text.rs` on the walk
//! and the strip of `fallback.rs` (R53) on the fallback path.
//!
//! # Dispatch
//!
//! A template is a row of the templates table; by its registry role it is also a ref (`footnote`) or a ref
//! list (`ref_list`), and the citations collector sees every template, ref and external link. The open
//! components that are parents of what they contain are kept here (see "Parents" in `walk/mod.rs`).

use crate::model::Components;
use crate::registry::Registry;
use crate::settings::Settings;
use crate::spans::CharIndex;

use super::citations::Citations;
use super::refs::{self, Refs};
use super::sections::Sections;
use super::templates::Templates;
use super::{Parent, Place};

/// A template parameter, as it closes.
pub(crate) struct ParamEvent {
    /// The trimmed span of its name; `None` for an unnamed parameter.
    pub name: Option<(usize, usize)>,
    /// The trimmed span of its value: `value_raw`.
    pub value: (usize, usize),
    /// The parameter's span, as the parser reports it.
    pub span: (usize, usize),
    pub value_text: String,
}

pub(crate) struct Collect<'t, 's> {
    text: &'t str,
    chars: &'t CharIndex,
    registry: &'s Registry,
    templates: Templates<'t, 's>,
    refs: Refs<'t, 's>,
    citations: Citations<'t, 's>,
    sections: Sections,
    /// The open components that are parents of what they contain, innermost last.
    parents: Vec<(&'static str, i32)>,
    /// The current section.
    section: i32,
}

impl<'t, 's> Collect<'t, 's> {
    pub(crate) fn new(text: &'t str, chars: &'t CharIndex, settings: &'s Settings, registry: &'s Registry) -> Self {
        Collect {
            text,
            chars,
            registry,
            templates: Templates::new(text, chars, settings, registry),
            refs: Refs::new(text, chars, registry, settings.site()),
            citations: Citations::new(text, chars, registry, settings.site()),
            sections: Sections::new(),
            parents: Vec::new(),
            section: 0,
        }
    }

    /// The section that components met from now on belong to.
    pub(crate) fn section(&self) -> i32 {
        self.section
    }

    fn parent(&self) -> Parent {
        self.parents.last().copied()
    }

    /// A heading spanning `span` opens a section. Returns its id.
    pub(crate) fn heading(&mut self, level: u8, (start, end): (usize, usize), title_raw: &str) -> i32 {
        self.section = self.sections.open(level, start, end, title_raw);
        self.section
    }

    /// The title text of the section `id` opened.
    pub(crate) fn heading_title(&mut self, id: i32, title_text: String) {
        self.sections.set_title(id, title_text);
    }

    /// A template is entered: its trimmed name span, the first colon of its name's own text, its span.
    pub(crate) fn template_open(
        &mut self,
        name: (usize, usize),
        colon: Option<usize>,
        span: (usize, usize),
        place: Place,
    ) {
        let parent = self.parent();
        let id = self.templates.enter(name, colon, span, place, parent);
        self.parents.push(("template", id as i32));
        let row = self.templates.row(id);
        match row.role.as_str() {
            "footnote" => {
                let r = self.refs.enter_template(row, place, parent);
                self.citations.enter_footnote(row, r, place);
                self.parents.push(("ref", r));
            }
            "ref_list" => self.refs.enter_list_template(row, place, parent),
            _ => {}
        }
        self.citations.enter_template(row, place, parent);
    }

    /// The byte after the colon of the template just opened, when its name part holds parameter `1`.
    pub(crate) fn split_from(&self) -> Option<usize> {
        self.templates.split_from()
    }

    /// The text after the colon of the template just opened (a module or a parser function).
    pub(crate) fn template_split_text(&mut self, text: String) {
        self.templates.set_split_text(text);
    }

    /// A parameter of the innermost open template closes.
    pub(crate) fn template_param(&mut self, p: ParamEvent) {
        self.templates.close_param(p);
    }

    /// The innermost open template closes.
    pub(crate) fn template_close(&mut self) {
        if let Some(id) = self.templates.close() {
            let row = self.templates.row(id);
            match row.role.as_str() {
                "footnote" => {
                    self.refs.close_template(row);
                    if let Some(("ref", id)) = self.parents.last() {
                        self.citations.close_ref(self.refs.row(*id as usize));
                    }
                    self.parents.pop();
                }
                "ref_list" => self.refs.close_list(Some(row)),
                _ => {}
            }
            self.citations.close_template(self.templates.row(id));
        }
        self.parents.pop();
    }

    /// A `<ref>` tag spanning `span` is entered.
    pub(crate) fn ref_open(&mut self, (start, end): (usize, usize), place: Place) {
        let id = self.refs.enter_tag(start, end, place, self.parent());
        self.citations.enter_tag_ref(id, (start, end), place);
        self.parents.push(("ref", id));
    }

    /// The `<ref>` tag entered last closes, with the text of its body.
    pub(crate) fn ref_close(&mut self, body_text: String) {
        self.refs.close_tag(body_text);
        if let Some(("ref", id)) = self.parents.last() {
            self.citations.close_ref(self.refs.row(*id as usize));
        }
        self.parents.pop();
    }

    /// A `<references>` tag spanning `span` is entered.
    pub(crate) fn ref_list_open(&mut self, (start, end): (usize, usize), place: Place) {
        let id = self.refs.enter_list_tag(start, end, place, self.parent());
        self.parents.push(("ref_list", id));
    }

    /// The `<references>` tag entered last closes.
    pub(crate) fn ref_list_close(&mut self) {
        self.refs.close_list(None);
        self.parents.pop();
    }

    /// An external link spanning `span` is entered; `label` gives the text of its label, asked for only when a
    /// row keeps it.
    pub(crate) fn external_link(&mut self, span: (usize, usize), label: impl FnOnce() -> Option<String>, place: Place) {
        self.citations.enter_link(label, span, place, self.parent());
    }

    /// Whether a free URL met now can be a row: in a ref's body, outside citation templates (R73). The walk asks
    /// before it looks for free URLs in a text node.
    pub(crate) fn wants_free_urls(&self) -> bool {
        self.citations.wants_free_urls()
    }

    /// A free URL (`autolink`) spanning `span` is met: a `bare_link` row in a ref's body, outside citation
    /// templates; nothing elsewhere.
    pub(crate) fn free_url(&mut self, span: (usize, usize), url: &str, place: Place) {
        self.citations.free_url(url, span, place, self.parent());
    }

    /// The rows of the five component tables, each place resolved by `resolve` into `para_id` and
    /// `para_offset`. The paragraphs table is the caller's.
    pub(crate) fn finish(self, resolve: impl Fn(Place) -> (Option<i32>, Option<i32>)) -> Components {
        let templates = self.templates.finish(&resolve);
        let (mut refs, ref_lists, is_short) = self.refs.finish(&resolve);
        let sections = self.sections.finish(self.text, self.chars, self.registry);
        let citations = self.citations.finish(&resolve, &sections);
        refs::link_short(&mut refs, &citations, &is_short);
        Components { templates, refs, ref_lists, citations, sections, paragraphs: Vec::new() }
    }
}
