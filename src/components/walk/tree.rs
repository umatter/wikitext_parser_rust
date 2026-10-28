//! The traversal that `walk` and `stats` share, so that both count nodes and depth by one rule.
//!
//! It is iterative, with an explicit stack on the heap, so no nesting of the parse tree can overflow the
//! thread's stack.

use parse_wiki_text_2::{DefinitionListItem, ListItem, Node, Parameter, Positioned, TableCaption, TableCell, TableRow};

use super::{WalkError, WalkLimits, WalkStats};

/// Which child list of its parent a `Part` is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Part {
    /// A template's name, a template parameter's name, the name of a `{{{parameter}}}`.
    Name,
    /// A template parameter's value, the default of a `{{{parameter|default}}}`.
    Value,
    /// The attributes of a table, a table row, a cell or a caption.
    Attributes,
}

/// One element of the parse tree.
#[derive(Clone, Copy)]
pub(crate) enum Element<'n, 'a> {
    Node(&'n Node<'a>),
    ListItem(&'n ListItem<'a>),
    DefItem(&'n DefinitionListItem<'a>),
    Param(&'n Parameter<'a>),
    Caption(&'n TableCaption<'a>),
    Row(&'n TableRow<'a>),
    Cell(&'n TableCell<'a>),
    /// A group of nodes that is one part of its parent. Not itself a node: it is not counted.
    Part(Part, &'n [Node<'a>]),
}

impl Element<'_, '_> {
    /// The element's byte span; `None` for a `Part`.
    pub(crate) fn span(&self) -> Option<(usize, usize)> {
        match self {
            Element::Node(n) => Some((n.start(), n.end())),
            Element::ListItem(i) => Some((i.start, i.end)),
            Element::DefItem(i) => Some((i.start, i.end)),
            Element::Param(p) => Some((p.start, p.end)),
            Element::Caption(c) => Some((c.start, c.end)),
            Element::Row(r) => Some((r.start, r.end)),
            Element::Cell(c) => Some((c.start, c.end)),
            Element::Part(..) => None,
        }
    }

    /// Every element counts 1 against the node budget, except a `Part`.
    fn counts(&self) -> bool {
        !matches!(self, Element::Part(..))
    }

    /// The depth is the nesting of templates (with `{{{parameters}}}`), extension tags and links (with
    /// files, categories and external links).
    fn nests(&self) -> bool {
        matches!(
            self,
            Element::Node(
                Node::Template { .. }
                    | Node::Parameter { .. }
                    | Node::Tag { .. }
                    | Node::Link { .. }
                    | Node::Image { .. }
                    | Node::Category { .. }
                    | Node::ExternalLink { .. }
            )
        )
    }
}

/// What the traversal reports: `enter` before an element's children, `exit` after them.
pub(crate) trait Visit<'n, 'a> {
    fn enter(&mut self, item: Element<'n, 'a>);
    fn exit(&mut self, item: Element<'n, 'a>);
}

enum Frame<'n, 'a> {
    Enter(Element<'n, 'a>),
    Exit(Element<'n, 'a>),
}

/// Visits every element under `nodes` in document order, depth first, within `limits`.
pub(crate) fn traverse<'n, 'a>(
    nodes: &'n [Node<'a>],
    limits: &WalkLimits,
    v: &mut impl Visit<'n, 'a>,
) -> Result<WalkStats, WalkError> {
    let mut stack = Vec::new();
    push_nodes(&mut stack, nodes);
    let mut stats = WalkStats { nodes: 0, max_depth: 0 };
    let mut depth = 0usize;
    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter(item) => {
                if item.counts() {
                    stats.nodes += 1;
                    if stats.nodes > limits.budget {
                        return Err(WalkError::Budget { nodes: stats.nodes });
                    }
                }
                if item.nests() {
                    depth += 1;
                    if depth > limits.depth_cap {
                        return Err(WalkError::DepthLimit { depth });
                    }
                    stats.max_depth = stats.max_depth.max(depth);
                }
                v.enter(item);
                stack.push(Frame::Exit(item));
                push_children(&mut stack, item);
            }
            Frame::Exit(item) => {
                v.exit(item);
                if item.nests() {
                    depth -= 1;
                }
            }
        }
    }
    Ok(stats)
}

fn push_nodes<'n, 'a>(stack: &mut Vec<Frame<'n, 'a>>, nodes: &'n [Node<'a>]) {
    stack.extend(nodes.iter().rev().map(|n| Frame::Enter(Element::Node(n))));
}

fn push_part<'n, 'a>(stack: &mut Vec<Frame<'n, 'a>>, part: Part, nodes: &'n [Node<'a>]) {
    stack.push(Frame::Enter(Element::Part(part, nodes)));
}

/// Pushes an element's children last first, so that they pop in document order.
fn push_children<'n, 'a>(stack: &mut Vec<Frame<'n, 'a>>, item: Element<'n, 'a>) {
    match item {
        Element::Node(node) => match node {
            Node::Category { ordinal: nodes, .. }
            | Node::ExternalLink { nodes, .. }
            | Node::Heading { nodes, .. }
            | Node::Image { text: nodes, .. }
            | Node::Link { text: nodes, .. }
            | Node::Preformatted { nodes, .. }
            | Node::Tag { nodes, .. } => push_nodes(stack, nodes),
            Node::OrderedList { items, .. } | Node::UnorderedList { items, .. } => {
                stack.extend(items.iter().rev().map(|i| Frame::Enter(Element::ListItem(i))))
            }
            Node::DefinitionList { items, .. } => {
                stack.extend(items.iter().rev().map(|i| Frame::Enter(Element::DefItem(i))))
            }
            Node::Template { name, parameters, .. } => {
                stack.extend(parameters.iter().rev().map(|p| Frame::Enter(Element::Param(p))));
                push_part(stack, Part::Name, name);
            }
            Node::Parameter { name, default, .. } => {
                if let Some(default) = default {
                    push_part(stack, Part::Value, default);
                }
                push_part(stack, Part::Name, name);
            }
            Node::Table { attributes, captions, rows, .. } => {
                // Captions and rows in source order (a caption normally comes first, but need not).
                let mut kids: Vec<Element> =
                    captions.iter().map(Element::Caption).chain(rows.iter().map(Element::Row)).collect();
                kids.sort_by_key(|k| k.span().map(|s| s.0));
                stack.extend(kids.into_iter().rev().map(Frame::Enter));
                push_part(stack, Part::Attributes, attributes);
            }
            Node::Bold { .. }
            | Node::BoldItalic { .. }
            | Node::CharacterEntity { .. }
            | Node::Comment { .. }
            | Node::EndTag { .. }
            | Node::HorizontalDivider { .. }
            | Node::Italic { .. }
            | Node::MagicWord { .. }
            | Node::ParagraphBreak { .. }
            | Node::Redirect { .. }
            | Node::StartTag { .. }
            | Node::Text { .. } => {}
        },
        Element::ListItem(i) => push_nodes(stack, &i.nodes),
        Element::DefItem(i) => push_nodes(stack, &i.nodes),
        Element::Param(p) => {
            push_part(stack, Part::Value, &p.value);
            if let Some(name) = &p.name {
                push_part(stack, Part::Name, name);
            }
        }
        Element::Caption(c) => {
            push_nodes(stack, &c.content);
            if let Some(a) = &c.attributes {
                push_part(stack, Part::Attributes, a);
            }
        }
        Element::Row(r) => {
            stack.extend(r.cells.iter().rev().map(|c| Frame::Enter(Element::Cell(c))));
            push_part(stack, Part::Attributes, &r.attributes);
        }
        Element::Cell(c) => {
            push_nodes(stack, &c.content);
            if let Some(a) = &c.attributes {
                push_part(stack, Part::Attributes, a);
            }
        }
        Element::Part(_, nodes) => push_nodes(stack, nodes),
    }
}
