//! The structured walk: one pass over the parse tree that builds the section tree and the paragraph text,
//! and places every component in a section, a paragraph, and at an offset in that paragraph's text.
//!
//! # Structure
//!
//! - `tree::traverse` visits every element of the tree in document order, iteratively, and enforces the
//!   limits. `stats` runs the same traversal without limits, so the counts it reports are the walk's own.
//! - `Walker` is the visitor. Entering an element, it reads the context of the element around it (`Ctx`),
//!   acts on the element (breaks a paragraph, opens a section, adds text), and pushes the context for the
//!   element's children; leaving the element, it pops that context and closes what the element opened.
//! - `text::TextBuilder` collects the paragraphs (the text rules are documented in `text.rs`),
//!   `sections::Sections` the section tree.
//!
//! # Hook points for the component tables
//!
//! A component is placed with `Walker::place`, called when the walk enters it (before `Walker::enter`
//! pushes the component's own context), and the place becomes `section_id`, `para_id` and `para_offset`
//! with `Walker::resolve` once the walk has closed the last paragraph (see `walk`). `place` runs before
//! `enter`, so a component whose own `enter` breaks the paragraph (a table, a list, a preformatted block)
//! would be placed in the paragraph before it; no component of milestones 0-2 is of that kind.
//! - Task 13b (templates): enter `Node::Template` in `Walker::enter_node`; give each parameter's
//!   `Part::Value` a `Sink::Flat` (its `value_text`).
//! - Task 15 (refs, ref lists): enter `Node::Tag` named `ref` or `references`, and templates whose registry
//!   role is `footnote` or `ref_list` (`refs.rs`); a tag ref's body is a `Sink::Flat` (its `body_text`).
//! - Task 16 (citations, `citations.rs`): citation templates and external links inside ref bodies, found
//!   from the same enter and exit events; the collector keeps its own stack of ref bodies.
//!
//! # Parents
//!
//! `parent_type` and `parent_id` name the nearest enclosing component of any table. `Walker::parents` holds
//! the open ones: a template while the walk is inside it, a ref while it is inside the ref's body, and a
//! `<references>` tag (`"ref_list"`) while it is inside the tag. A footnote template is a template and a ref,
//! the ref innermost, so what it contains belongs to the ref; a ref-list template is a parent as a template.

mod citations;
mod refs;
mod sections;
mod templates;
mod text;
mod tree;

use parse_wiki_text_2::{DefinitionListItemType, Node, Positioned};

use crate::frontend::Parsed;
use crate::model::Components;
use crate::registry::Registry;
use crate::settings::Settings;
use crate::spans::CharIndex;

pub use citations::citeref;
use citations::Citations;
use refs::Refs;
use sections::Sections;
use templates::Templates;
use text::{Flat, ParaKind, TextBuilder};
use tree::{Element, Part, Visit};

/// The limits of one walk: nodes visited, and the nesting depth of templates, tags and links.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalkLimits {
    pub budget: u64,
    pub depth_cap: usize,
}

/// Why the walk stopped: more than `budget` nodes, or nesting deeper than `depth_cap`. Each carries the
/// count at which the limit was exceeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalkError {
    Budget { nodes: u64 },
    DepthLimit { depth: usize },
}

/// What a walk of a tree costs: the nodes it visits and the deepest nesting it meets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalkStats {
    pub nodes: u64,
    pub max_depth: usize,
}

/// Extension tags whose content adds nothing to the text (`text.rs`).
const EXCLUDED_TAGS: [&str; 21] = [
    "ref",
    "references",
    "math",
    "chem",
    "ce",
    "gallery",
    "templatestyles",
    "score",
    "timeline",
    "graph",
    "mapframe",
    "maplink",
    "syntaxhighlight",
    "source",
    "templatedata",
    "imagemap",
    "inputbox",
    "categorytree",
    "hiero",
    "indicator",
    "includeonly",
];

/// Walks a parsed page into its components.
pub fn walk(
    text: &str,
    parsed: &Parsed,
    settings: &Settings,
    registry: &Registry,
    limits: &WalkLimits,
) -> Result<Components, WalkError> {
    let chars = CharIndex::new(text);
    let mut w = Walker::new(text, settings, registry, &chars);
    tree::traverse(&parsed.output.nodes, limits, &mut w)?;
    w.paras.break_paragraph(ParaKind::Prose);
    // Component rows collected during the walk get their `para_id` and `para_offset` here, now that every
    // paragraph is closed and before `finish` consumes the paragraph builder.
    let Walker { paras, templates, refs, citations, sections, .. } = w;
    let resolve = |place| resolve_place(&paras, place);
    let templates = templates.finish(resolve);
    let (mut refs, ref_lists, is_short) = refs.finish(resolve);
    let sections = sections.finish(text, &chars, registry);
    let citations = citations.finish(resolve, &sections);
    refs::link_short(&mut refs, &citations, &is_short);
    Ok(Components { templates, refs, ref_lists, citations, sections, paragraphs: paras.finish() })
}

/// `para_id` and `para_offset` of a place, once every paragraph is closed.
fn resolve_place(paras: &TextBuilder, place: Place) -> (Option<i32>, Option<i32>) {
    match place.para.and_then(|(slot, offset)| paras.para_id(slot).map(|id| (id, offset))) {
        Some((id, offset)) => (Some(id), Some(offset)),
        None => (None, None),
    }
}

/// The nodes the walk of `parsed` visits and the deepest nesting it meets, counted by the walk's own rules.
pub fn stats(parsed: &Parsed) -> WalkStats {
    struct Count;
    impl<'n, 'a> Visit<'n, 'a> for Count {
        fn enter(&mut self, _: Element<'n, 'a>) {}
        fn exit(&mut self, _: Element<'n, 'a>) {}
    }
    let unlimited = WalkLimits { budget: u64::MAX, depth_cap: usize::MAX };
    tree::traverse(&parsed.output.nodes, &unlimited, &mut Count).expect("no limits")
}

/// Where the text met in a context goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sink {
    /// The open paragraph.
    Para,
    /// A `Flat` on `Walker::flats`, by index: a heading's title (later: a parameter value, a ref body).
    Flat(usize),
    /// Nowhere.
    Mute,
}

/// Where an excluded element sits: the slot of the paragraph that was open and the offset in its text, or
/// no slot outside paragraphs (in a heading).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Anchor {
    slot: Option<usize>,
    offset: i32,
}

/// The context an element gives its children.
#[derive(Clone, Copy, Debug)]
struct Ctx {
    sink: Sink,
    /// The kind of paragraph a break in this context closes.
    kind: ParaKind,
    /// The anchor of the outermost excluded element around, if any.
    excluded: Option<Anchor>,
    /// The innermost list around: its id (counting every list met), and its depth (0 outside lists).
    list_id: i32,
    list_depth: i32,
    /// For the children of a list: the list's start, until its first item takes it. The parser starts a
    /// list's first item after its marker and every later item at its marker.
    list_start: Option<usize>,
    /// For the children of an external link: its URL has not ended yet.
    url: bool,
    /// For the children of a link without a label: the text node at this byte is its target, already
    /// added as the title.
    title_at: Option<usize>,
    /// For the children of a template's name part that holds parameter `1`: only what starts at or after
    /// this byte adds text.
    from: usize,
    /// What the parts and parameters among the children belong to.
    owner: Owner,
}

/// The element a part (a name, a value) or a parameter is a child of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
    None,
    Template,
    Param,
}

/// The nearest enclosing component: its table (`"template"`, `"ref"`, `"ref_list"`) and id.
type Parent = Option<(&'static str, i32)>;

/// `parent_type` and `parent_id` of a row.
fn parent_cols(parent: Parent) -> (Option<String>, Option<i32>) {
    match parent {
        Some((table, id)) => (Some(table.to_string()), Some(id)),
        None => (None, None),
    }
}

/// A span the parser reports, clamped into the text: `start <= end <= text.len()`.
fn clamp_span(text: &str, start: usize, end: usize) -> (usize, usize) {
    let start = start.min(text.len());
    (start, end.clamp(start, text.len()))
}

/// Where a component sits, as the walk meets it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Place {
    section_id: i32,
    /// The paragraph slot and the offset in its text.
    para: Option<(usize, i32)>,
}

struct Walker<'t, 's> {
    text: &'t str,
    settings: &'s Settings,
    paras: TextBuilder<'t>,
    templates: Templates<'t, 's>,
    refs: Refs<'t, 's>,
    citations: Citations<'t, 's>,
    /// The open components that are parents of what they contain, innermost last (see "Parents").
    parents: Vec<(&'static str, i32)>,
    sections: Sections,
    /// The current section.
    section: i32,
    /// Open `Sink::Flat` texts, innermost last.
    flats: Vec<Flat>,
    /// The section each open heading opened, innermost last.
    headings: Vec<i32>,
    /// The context of each open element, the root's first.
    ctx: Vec<Ctx>,
    /// Lists met so far.
    lists: i32,
    /// The end of a link the walk has just left, until the next element. The parser repeats a piped link's
    /// trail as the link's next sibling: a text node that starts before the link's end.
    after_link: Option<usize>,
}

impl<'t, 's> Walker<'t, 's> {
    fn new(
        text: &'t str,
        settings: &'s Settings,
        registry: &'s Registry,
        chars: &'t CharIndex,
    ) -> Walker<'t, 's> {
        let root = Ctx {
            sink: Sink::Para,
            kind: ParaKind::Prose,
            excluded: None,
            list_id: -1,
            list_depth: 0,
            list_start: None,
            url: false,
            title_at: None,
            from: 0,
            owner: Owner::None,
        };
        Walker {
            text,
            settings,
            paras: TextBuilder::new(text, chars),
            templates: Templates::new(text, chars, settings, registry),
            refs: Refs::new(text, chars, registry, settings.site()),
            citations: Citations::new(text, chars, registry, settings.site()),
            parents: Vec::new(),
            sections: Sections::new(),
            section: 0,
            flats: Vec::new(),
            headings: Vec::new(),
            ctx: vec![root],
            lists: 0,
            after_link: None,
        }
    }

    fn top(&self) -> Ctx {
        *self.ctx.last().expect("the root context is never popped")
    }

    /// The place of a component about to be entered. Call it before `enter`: a component whose `enter`
    /// breaks the paragraph (a table, a list, a preformatted block) is then placed in the paragraph before.
    fn place(&self) -> Place {
        let around = self.top();
        let para = match around.excluded {
            Some(a) => a.slot.map(|slot| (slot, a.offset)),
            None if around.sink == Sink::Para => Some((self.paras.slot(), self.paras.anchor())),
            None => None,
        };
        Place { section_id: self.section, para }
    }

    fn parent(&self) -> Parent {
        self.parents.last().copied()
    }

    fn add_text(&mut self, sink: Sink, s: &str, start: usize) {
        match sink {
            Sink::Para => self.paras.push_text(s, start),
            Sink::Flat(i) => self.flats[i].push(s),
            Sink::Mute => {}
        }
    }

    fn add_str(&mut self, sink: Sink, s: &str, start: usize, end: usize) {
        match sink {
            Sink::Para => self.paras.push_str(s, start, end),
            Sink::Flat(i) => self.flats[i].push(s),
            Sink::Mute => {}
        }
    }

    fn touch(&mut self, around: Ctx, start: usize, end: usize) {
        if around.sink == Sink::Para {
            self.paras.touch(start, end);
        }
    }

    /// A block boundary: breaks the paragraph, or separates words in a flat text.
    fn boundary(&mut self, around: Ctx) {
        match around.sink {
            Sink::Para => self.paras.break_paragraph(around.kind),
            Sink::Flat(i) => self.flats[i].push(" "),
            Sink::Mute => {}
        }
    }

    /// Makes `c` the context of an excluded element: its content adds nothing, and the components inside
    /// take the anchor of the outermost excluded element.
    fn exclude(&self, around: Ctx, c: &mut Ctx) {
        c.sink = Sink::Mute;
        c.excluded = around.excluded.or(Some(match around.sink {
            Sink::Para => Anchor { slot: Some(self.paras.slot()), offset: self.paras.anchor() },
            Sink::Flat(_) | Sink::Mute => Anchor { slot: None, offset: 0 },
        }));
    }

    /// An interlanguage link: its target starts with a language prefix and no colon.
    fn is_interlanguage(&self, target: &str) -> bool {
        match target.split_once(':') {
            Some((prefix, _)) if !target.starts_with(':') => {
                self.settings.namespace(prefix).is_none() && self.settings.is_language_prefix(prefix)
            }
            _ => false,
        }
    }

    /// A link target as a title: without a leading colon and without a namespace prefix.
    fn link_title<'x>(&self, target: &'x str) -> &'x str {
        let t = target.trim();
        let t = t.strip_prefix(':').unwrap_or(t);
        match t.split_once(':') {
            Some((prefix, rest)) if self.settings.namespace(prefix).is_some() => rest.trim(),
            _ => t,
        }
    }

    /// Whether the link starting at `start` has a label (`[[target|label]]`). A target never contains `|`
    /// or `]`, so the first of them after `[[` decides.
    fn has_label(&self, start: usize) -> bool {
        let rest = self.text.as_bytes().get(start + 2..).unwrap_or(&[]);
        rest.iter().find(|b| matches!(b, b'|' | b']')) == Some(&b'|')
    }

    /// A list item's span: from its own marker (`list_start` for the first item of a list) to its end, or to
    /// its last node before its first nested list, without trailing whitespace.
    fn item_span(&self, item: Element, list_start: Option<usize>) -> (usize, usize) {
        let ((item_start, item_end), nodes) = match item {
            Element::ListItem(i) => ((i.start, i.end), &i.nodes),
            Element::DefItem(d) => ((d.start, d.end), &d.nodes),
            _ => unreachable!("only list items have list spans"),
        };
        let start = list_start.map_or(item_start, |s| s.min(item_start));
        let is_list = |n: &Node| {
            matches!(n, Node::OrderedList { .. } | Node::UnorderedList { .. } | Node::DefinitionList { .. })
        };
        let end = match nodes.iter().position(is_list) {
            None => item_end,
            Some(k) => nodes[..k].last().map_or(start, |n| n.end()),
        };
        let end = end.clamp(start, self.text.len().max(start));
        (start, start + self.text.get(start..end).map_or(0, |s| s.trim_end().len()))
    }

    /// `after_link`: the end of the link that is this node's previous sibling, if it is one.
    fn enter_node(&mut self, node: &Node, around: Ctx, c: &mut Ctx, after_link: Option<usize>) {
        let (start, end) = (node.start(), node.end());
        match node {
            Node::Text { value, .. } => {
                let (mut value, mut start) = (*value, start);
                if let Some(link_end) = after_link.filter(|&e| start < e) {
                    // The parser's repeat of a piped link's trail.
                    match self.text.get(link_end..end) {
                        Some(rest) if end > link_end => (value, start) = (rest, link_end),
                        _ => return,
                    }
                }
                if around.from > start {
                    // The part of a template name after its colon.
                    match value.get(around.from - start..) {
                        Some(rest) if end > around.from => (value, start) = (rest, around.from),
                        _ => return,
                    }
                }
                if around.title_at == Some(start) {
                    return;
                }
                if around.url {
                    let Some(i) = value.find(char::is_whitespace) else { return };
                    self.ctx.last_mut().expect("root").url = false;
                    (value, start) = (&value[i..], start + i);
                }
                self.add_text(around.sink, value, start);
            }
            Node::CharacterEntity { character, .. } => {
                if !around.url {
                    let mut buf = [0u8; 4];
                    self.add_str(around.sink, character.encode_utf8(&mut buf), start, end);
                }
            }
            Node::Bold { .. } | Node::Italic { .. } | Node::BoldItalic { .. } => self.touch(around, start, end),
            Node::StartTag { name, .. } | Node::EndTag { name, .. } => {
                self.touch(around, start, end);
                if name == "br" {
                    self.add_str(around.sink, " ", start, end);
                }
            }
            Node::ParagraphBreak { .. } => self.boundary(around),
            Node::HorizontalDivider { .. } => {
                self.boundary(around);
                self.touch(around, start, end);
            }
            Node::Heading { level, .. } => {
                // Every heading opens a section and breaks the paragraph, wherever it is (the parser makes
                // headings in running text and in table cells); the components inside keep the anchor of an
                // excluded ancestor.
                self.paras.break_before(around.kind, start);
                if let Sink::Flat(i) = around.sink {
                    self.flats[i].push(" ");
                }
                let level = *level as usize;
                let raw = self.text.get(start + level..end.saturating_sub(level)).unwrap_or("").trim();
                self.section = self.sections.open(level as u8, start, end, raw);
                self.paras.set_section(self.section);
                self.headings.push(self.section);
                self.flats.push(Flat::default());
                c.sink = Sink::Flat(self.flats.len() - 1);
            }
            Node::Link { target, text, .. } => {
                self.touch(around, start, end);
                if self.is_interlanguage(target) {
                    self.exclude(around, c);
                } else if !self.has_label(start) {
                    let title = self.link_title(target);
                    self.add_str(around.sink, title, start, end);
                    if let Some(Node::Text { start, .. }) = text.first() {
                        c.title_at = Some(*start);
                    }
                }
            }
            Node::ExternalLink { nodes, .. } => {
                self.touch(around, start, end);
                c.url = true;
                self.citations.enter_link(nodes, (start, end), self.place(), self.parent());
            }
            Node::Tag { name, .. } => {
                let place = self.place();
                self.touch(around, start, end);
                if EXCLUDED_TAGS.contains(&name.as_ref()) {
                    self.exclude(around, c);
                }
                match name.as_ref() {
                    "ref" => {
                        let id = self.refs.enter_tag(start, end, place, self.parent());
                        self.citations.enter_tag_ref(id, (start, end), place);
                        self.parents.push(("ref", id));
                        self.flats.push(Flat::default());
                        c.sink = Sink::Flat(self.flats.len() - 1);
                    }
                    "references" => {
                        let id = self.refs.enter_list_tag(start, end, place, self.parent());
                        self.parents.push(("ref_list", id));
                    }
                    _ => {}
                }
            }
            Node::Template { name, .. } => {
                let (place, parent) = (self.place(), self.parent());
                let id = self.templates.enter(name, (start, end), place, parent);
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
                self.touch(around, start, end);
                self.exclude(around, c);
                c.owner = Owner::Template;
            }
            Node::Parameter { .. }
            | Node::Image { .. }
            | Node::Category { .. }
            | Node::Comment { .. }
            | Node::MagicWord { .. }
            | Node::Redirect { .. } => {
                self.touch(around, start, end);
                self.exclude(around, c);
            }
            Node::Table { .. } => {
                self.boundary(around);
                self.touch(around, start, end);
                self.exclude(around, c);
            }
            Node::Preformatted { .. } => {
                // Not touched: the parser ends the block after its newline (past the text at its end), so
                // its content gives the span.
                self.boundary(around);
                c.kind = ParaKind::Preformatted;
            }
            Node::OrderedList { .. } | Node::UnorderedList { .. } | Node::DefinitionList { .. } => {
                self.boundary(around);
                c.list_id = self.lists;
                c.list_depth = around.list_depth + 1;
                c.list_start = Some(start);
                self.lists += 1;
            }
        }
    }

    fn exit_node(&mut self, node: &Node, c: Ctx, around: Ctx) {
        match node {
            Node::Heading { .. } => {
                debug_assert_eq!(c.sink, Sink::Flat(self.flats.len() - 1));
                let title = self.flats.pop().expect("pushed on enter").into_text();
                let id = self.headings.pop().expect("pushed on enter");
                self.sections.set_title(id, title);
            }
            Node::Tag { name, .. } => match name.as_ref() {
                "ref" => {
                    debug_assert_eq!(c.sink, Sink::Flat(self.flats.len() - 1));
                    let body = self.flats.pop().expect("pushed on enter").into_text();
                    self.refs.close_tag(body);
                    if let Some(("ref", id)) = self.parents.last() {
                        self.citations.close_ref(self.refs.row(*id as usize));
                    }
                    self.parents.pop();
                }
                "references" => {
                    self.refs.close_list(None);
                    self.parents.pop();
                }
                _ => {}
            },
            Node::Template { .. } => {
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
            Node::Link { .. } => self.after_link = Some(node.end()),
            Node::Preformatted { .. } => {
                if c.sink == Sink::Para {
                    self.paras.break_paragraph(c.kind);
                }
            }
            Node::Table { .. }
            | Node::HorizontalDivider { .. }
            | Node::OrderedList { .. }
            | Node::UnorderedList { .. }
            | Node::DefinitionList { .. } => self.boundary(around),
            _ => {}
        }
    }
}

impl<'n, 'a> Visit<'n, 'a> for Walker<'_, '_> {
    fn enter(&mut self, item: Element<'n, 'a>) {
        let after_link = self.after_link.take();
        let mut around = self.top();
        if let Element::Node(n) = item {
            if around.from > 0 && n.end() <= around.from {
                around.sink = Sink::Mute; // before the colon of a template name
            }
        }
        let mut c = Ctx { url: false, title_at: None, list_start: None, from: 0, owner: Owner::None, ..around };
        match item {
            Element::Node(node) => self.enter_node(node, around, &mut c, after_link),
            Element::ListItem(_) | Element::DefItem(_) => {
                let (list_id, depth) = (around.list_id, around.list_depth);
                c.kind = match item {
                    Element::DefItem(d) if d.type_ == DefinitionListItemType::Term => {
                        ParaKind::DefinitionTerm { list_id, depth }
                    }
                    Element::DefItem(_) => ParaKind::Definition { list_id, depth },
                    _ => ParaKind::ListItem { list_id, depth },
                };
                self.boundary(around);
                let span = self.item_span(item, around.list_start);
                self.ctx.last_mut().expect("root").list_start = None;
                self.touch(around, span.0, span.1);
            }
            // The name part of a module or parser function holds parameter 1, after its colon.
            Element::Part(Part::Name, _) if around.owner == Owner::Template && self.templates.split_from().is_some() => {
                self.flats.push(Flat::default());
                c.sink = Sink::Flat(self.flats.len() - 1);
                c.from = self.templates.split_from().unwrap_or(0);
            }
            // Names and attributes never add text.
            Element::Part(Part::Name | Part::Attributes, _) => c.sink = Sink::Mute,
            // A parameter value is a flat text.
            Element::Part(Part::Value, _) if around.owner == Owner::Param => {
                self.flats.push(Flat::default());
                c.sink = Sink::Flat(self.flats.len() - 1);
            }
            Element::Param(_) => c.owner = Owner::Param,
            Element::Part(Part::Value, _)
            | Element::Caption(_)
            | Element::Row(_)
            | Element::Cell(_) => {}
        }
        self.ctx.push(c);
    }

    fn exit(&mut self, item: Element<'n, 'a>) {
        self.after_link = None;
        let c = self.ctx.pop().expect("pushed on enter");
        let around = self.top();
        match item {
            Element::Node(node) => self.exit_node(node, c, around),
            Element::ListItem(_) | Element::DefItem(_) => {
                if c.sink == Sink::Para {
                    self.paras.break_paragraph(c.kind);
                }
            }
            Element::Part(Part::Name, _) if around.owner == Owner::Template && matches!(c.sink, Sink::Flat(_)) => {
                let text = self.flats.pop().expect("pushed on enter").into_text();
                self.templates.set_split_text(text);
            }
            Element::Part(Part::Value, _) if around.owner == Owner::Param => {
                let text = self.flats.pop().expect("pushed on enter").into_text();
                self.templates.set_pending(text);
            }
            Element::Param(p) => self.templates.close_param(p),
            Element::Part(..) | Element::Caption(_) | Element::Row(_) | Element::Cell(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Records the place of every template and extension tag as the walk enters it.
    struct Probe<'t, 's> {
        w: Walker<'t, 's>,
        seen: Vec<(usize, Place)>,
    }

    impl<'n, 'a> Visit<'n, 'a> for Probe<'_, '_> {
        fn enter(&mut self, item: Element<'n, 'a>) {
            if let Element::Node(n @ (Node::Template { .. } | Node::Tag { .. })) = item {
                self.seen.push((n.start(), self.w.place()));
            }
            self.w.enter(item);
        }
        fn exit(&mut self, item: Element<'n, 'a>) {
            self.w.exit(item);
        }
    }

    /// (byte offset of the component, section_id, para_id, para_offset) for each template and tag.
    fn places(text: &str) -> Vec<(usize, i32, Option<i32>, Option<i32>)> {
        let settings = Settings::load(Path::new("config/sites/ru.wikipedia.toml")).unwrap();
        let registry = Registry::default();
        let parsed = crate::frontend::parse(text, settings.configuration(), 0).unwrap();
        let chars = CharIndex::new(text);
        let mut p = Probe { w: Walker::new(text, &settings, &registry, &chars), seen: Vec::new() };
        let unlimited = WalkLimits { budget: u64::MAX, depth_cap: usize::MAX };
        tree::traverse(&parsed.output.nodes, &unlimited, &mut p).unwrap();
        p.w.paras.break_paragraph(ParaKind::Prose);
        p.seen
            .iter()
            .map(|(at, place)| {
                let (id, offset) = resolve_place(&p.w.paras, *place);
                (*at, place.section_id, id, offset)
            })
            .collect()
    }

    #[test]
    fn components_take_the_anchor_where_they_are_met() {
        let t = "Город<ref>{{cite|x}}</ref> стоит.\n\n{{Карточка}}\n\nВторой {{a|{{b}}}} абзац.";
        let at = |s: &str| t.find(s).unwrap();
        assert_eq!(
            places(t),
            vec![
                (at("<ref>"), 0, Some(0), Some(5)),
                (at("{{cite"), 0, Some(0), Some(5)), // inside an excluded ref: the ref's offset
                (at("{{Карточка"), 0, None, None),   // its paragraph has no text and is dropped
                (at("{{a|"), 0, Some(1), Some(6)),   // the space before it is not written yet
                (at("{{b}}"), 0, Some(1), Some(6)),
            ]
        );
    }

    #[test]
    fn components_in_headings_and_tables_have_no_paragraph() {
        let t = "Лид.\n== Заголовок{{a}} ==\nТекст{{b}}\n{|\n| {{c}}\n|}\nПосле";
        let at = |s: &str| t.find(s).unwrap();
        assert_eq!(
            places(t),
            vec![(at("{{a}}"), 1, None, None), (at("{{b}}"), 1, Some(1), Some(5)), (at("{{c}}"), 1, None, None)]
        );
    }

    #[test]
    fn link_label_and_list_item_anchors() {
        let t = "[[Москва|Город {{a}}]] и\n* пункт{{b}}\n* {{c}}";
        let at = |s: &str| t.find(s).unwrap();
        assert_eq!(
            places(t),
            vec![(at("{{a}}"), 0, Some(0), Some(5)), (at("{{b}}"), 0, Some(1), Some(5)), (at("{{c}}"), 0, None, None)]
        );
    }

    #[test]
    fn components_in_nested_items_take_the_nested_items_paragraph() {
        let t = "* a {{p}}\n** b {{q}}\n* c {{r}}";
        let at = |s: &str| t.find(s).unwrap();
        assert_eq!(
            places(t),
            vec![(at("{{p}}"), 0, Some(0), Some(1)), (at("{{q}}"), 0, Some(1), Some(1)), (at("{{r}}"), 0, Some(2), Some(1))]
        );
    }

    #[test]
    fn components_after_a_heading_in_a_table_keep_the_tables_anchor() {
        // The heading in the cell opens section 1; the components after it are still in the table, which
        // has no paragraph, and not in the paragraph after the table.
        let t = "Текст\n{|\n| {{x}}\n== H ==\n{{y}}\n|}\nпосле {{z}}";
        let at = |s: &str| t.find(s).unwrap();
        assert_eq!(
            places(t),
            vec![(at("{{x}}"), 0, None, None), (at("{{y}}"), 1, None, None), (at("{{z}}"), 1, Some(1), Some(5))]
        );
    }
}
