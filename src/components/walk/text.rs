//! Paragraph text: whitespace collapsing, code-point offsets (anchors) and paragraph spans.
//!
//! # What each node adds to the text
//!
//! The walk (`walk/mod.rs`) applies these rules; this module only collects what they add.
//!
//! - **Whitespace.** Runs of whitespace (`char::is_whitespace`, so a decoded `&nbsp;` too) collapse to one
//!   space, and the text is trimmed. Offsets (`para_offset`, `n_chars`) count code points of the collapsed text.
//! - **Text and entities.** Plain text is added; character entities are added decoded. A free URL in running
//!   text stays in the text, as MediaWiki displays it.
//! - **Bold and italic** markers add nothing; the text between them is added.
//! - **Links.** A link adds the text of its label, or, without a label, its target title with any leading
//!   colon and namespace prefix removed; then its link trail. The parser repeats the trail of a piped link as
//!   a sibling text node; that copy is skipped. An interlanguage link (the target starts with a language
//!   prefix and no colon, `[[en:Moscow]]`) adds nothing. Interwiki (`[[w:Москва|Москва]]`) and leading-colon
//!   links add text like any link.
//! - **External links** add their label (the text after the URL and the first whitespace), or nothing when
//!   they have none (`[https://example.org]`).
//! - **HTML tags** (`StartTag`, `EndTag`) add nothing, except `br`, which adds one space. The text between a
//!   start and an end tag is added.
//! - **Extension tags.** `nowiki` adds its raw text and `poem` its content. `ref`, `references`, `math`,
//!   `chem`, `ce`, `gallery`, `templatestyles`, `score`, `timeline`, `graph`, `mapframe`, `maplink`,
//!   `syntaxhighlight`, `source`, `templatedata`, `imagemap`, `inputbox`, `categorytree`, `hiero`,
//!   `indicator` and `includeonly` (shown only where the page is transcluded) add nothing. Any other
//!   extension tag adds its content, `noinclude` and `onlyinclude` among them.
//! - **Excluded** (add nothing): templates, template parameters (`{{{1}}}`), files, categories, comments,
//!   magic words, tables, and the extension tags listed above. Components inside an excluded node take the
//!   `para_id` and `para_offset` of the outermost excluded node around them.
//! - **Headings** add nothing to paragraphs; their text, under the same rules, is the section's `title_text`.
//!   Every heading opens a section, wherever it is: in running text, in a table cell, in a template
//!   parameter or in a tag. (The vendored parser makes headings, and lists, only in running text and in
//!   table cells.)
//!
//! # Where paragraphs break
//!
//! At a paragraph break (an empty line), at a heading, at a horizontal divider (`----`, which MediaWiki also
//! renders between paragraphs), and at the start and end of a list, a table or a preformatted block.
//! - A preformatted block is its own paragraph (`preformatted`).
//! - Each list item is its own paragraph: `list_item`, or `definition_term` and `definition` in a definition
//!   list, with `list_id` and `item_depth` (1 for an outermost list). `list_id` counts, from 0 in document
//!   order, the lists that have at least one paragraph; a list that has none (one in a table cell, or one
//!   whose items have no text) takes no id. A nested list ends the text of the item around it.
//! - A heading breaks the paragraph wherever it is. Paragraph breaks, lists and preformatted blocks inside an
//!   excluded node (a template parameter, a table cell, a ref) do not: excluded content makes no paragraphs.
//!   Components inside excluded content keep the paragraph and offset of their excluded ancestor, also
//!   after a heading inside it.
//! - A paragraph whose text is empty is dropped and takes no id.
//!
//! # Spans
//!
//! A paragraph's `start..end` runs from its first node to its last, excluded nodes included, leading and
//! trailing whitespace not. Spans lie inside the text and never overlap.
//! - A list item's span starts at its own marker (the `*`, `#`, `;` or `:` of its level) and ends with the
//!   item, or where its first nested list begins.
//! - A paragraph that a heading inside an excluded node breaks ends before that heading.

use crate::fingerprint::{fp, norm};
use crate::model::ParagraphRow;
use crate::spans::CharIndex;

/// Text with whitespace runs collapsed to one space and both ends trimmed, built incrementally.
///
/// A whitespace run is only written when more text follows, so the text is trimmed at every moment and its
/// length is the offset of whatever comes next.
#[derive(Debug, Default, Clone)]
pub struct Flat {
    buf: String,
    /// Code points in `buf`.
    n: i32,
    /// Whitespace was seen after the last character written.
    pending: bool,
}

impl Flat {
    pub fn push(&mut self, s: &str) {
        for c in s.chars() {
            if c.is_whitespace() {
                self.pending = self.n > 0;
            } else {
                if self.pending {
                    self.buf.push(' ');
                    self.n += 1;
                    self.pending = false;
                }
                self.buf.push(c);
                self.n += 1;
            }
        }
    }

    /// Code points so far.
    pub fn len(&self) -> i32 {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    pub fn into_text(self) -> String {
        self.buf
    }
}

/// The byte range of `s` without leading and trailing whitespace, or `None` when `s` is all whitespace.
fn trimmed(s: &str) -> Option<(usize, usize)> {
    let start = s.len() - s.trim_start().len();
    let end = s.trim_end().len();
    (start < end).then_some((start, end))
}

/// What kind of paragraph a break closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParaKind {
    Prose,
    Preformatted,
    ListItem { list_id: i32, depth: i32 },
    DefinitionTerm { list_id: i32, depth: i32 },
    Definition { list_id: i32, depth: i32 },
}

impl ParaKind {
    fn name(self) -> &'static str {
        match self {
            ParaKind::Prose => "prose",
            ParaKind::Preformatted => "preformatted",
            ParaKind::ListItem { .. } => "list_item",
            ParaKind::DefinitionTerm { .. } => "definition_term",
            ParaKind::Definition { .. } => "definition",
        }
    }

    fn list(self) -> (Option<i32>, Option<i32>) {
        match self {
            ParaKind::Prose | ParaKind::Preformatted => (None, None),
            ParaKind::ListItem { list_id, depth }
            | ParaKind::DefinitionTerm { list_id, depth }
            | ParaKind::Definition { list_id, depth } => (Some(list_id), Some(depth)),
        }
    }
}

/// Builds the page's paragraphs. One paragraph is open at any time; `break_paragraph` closes it.
///
/// Each paragraph, while open, has a *slot*: a number that components met inside it record together with
/// `anchor()`. A slot becomes a `para_id` when its paragraph closes with text, and `None` when it is dropped
/// (`para_id`), so ids stay dense and in document order.
pub struct TextBuilder<'t> {
    text: &'t str,
    chars: &'t CharIndex,
    section_id: i32,
    cur: Flat,
    /// Span of the open paragraph's nodes so far.
    span: Option<(usize, usize)>,
    /// `para_id` of each closed slot; the open paragraph's slot is `slots.len()`.
    slots: Vec<Option<i32>>,
    rows: Vec<ParagraphRow>,
}

impl<'t> TextBuilder<'t> {
    /// A builder for the paragraphs of `text`, whose char index is `chars`.
    pub fn new(text: &'t str, chars: &'t CharIndex) -> TextBuilder<'t> {
        TextBuilder {
            text,
            chars,
            section_id: 0,
            cur: Flat::default(),
            span: None,
            slots: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// `pos` moved into the text and back to a char boundary. (The parser ends a preformatted block that ends
    /// the text one byte past it.)
    fn fit(&self, pos: usize) -> usize {
        let mut pos = pos.min(self.text.len());
        while !self.text.is_char_boundary(pos) {
            pos -= 1;
        }
        pos
    }

    /// The section that paragraphs opened from now on belong to.
    pub fn set_section(&mut self, id: i32) {
        self.section_id = id;
    }

    /// Adds `s`, a verbatim slice of the wikitext starting at byte `source_start`.
    pub fn push_text(&mut self, s: &str, source_start: usize) {
        if let Some((a, b)) = trimmed(s) {
            self.touch(source_start + a, source_start + b);
        }
        self.cur.push(s);
    }

    /// Adds `s`, text that stands for the source span `start..end` (a decoded entity, a link's title).
    pub fn push_str(&mut self, s: &str, start: usize, end: usize) {
        if trimmed(s).is_some() {
            self.touch(start, end);
        }
        self.cur.push(s);
    }

    /// Extends the open paragraph's span over a node.
    pub fn touch(&mut self, start: usize, end: usize) {
        let (start, end) = (self.fit(start), self.fit(end));
        self.span = Some(match self.span {
            None => (start, end),
            Some((s, e)) => (s.min(start), e.max(end)),
        });
    }

    /// The current code-point offset in the open paragraph's text.
    pub fn anchor(&self) -> i32 {
        self.cur.len()
    }

    /// The open paragraph's slot.
    pub fn slot(&self) -> usize {
        self.slots.len()
    }

    /// The `para_id` of a closed slot: `None` when its paragraph was dropped (or is still open).
    pub fn para_id(&self, slot: usize) -> Option<i32> {
        self.slots.get(slot).copied().flatten()
    }

    /// Closes the open paragraph as `kind`, its span cut back to end before `pos` (without the whitespace
    /// there). For a heading inside an element that the paragraph's span already covers.
    pub fn break_before(&mut self, kind: ParaKind, pos: usize) {
        if let Some((start, end)) = self.span {
            let end = self.fit(end.min(pos)).max(start);
            self.span = Some((start, start + self.text[start..end].trim_end().len()));
        }
        self.break_paragraph(kind);
    }

    /// Closes the open paragraph as `kind`. A paragraph without text is dropped; one that has met no node
    /// at all is left open, so it keeps its slot.
    pub fn break_paragraph(&mut self, kind: ParaKind) {
        let Some((start, end)) = self.span.take() else {
            debug_assert!(self.cur.is_empty());
            return;
        };
        let cur = std::mem::take(&mut self.cur);
        if cur.is_empty() {
            self.slots.push(None);
            return;
        }
        debug_assert!(
            start <= end && self.text.is_char_boundary(start) && self.text.is_char_boundary(end),
            "paragraph span {start}..{end} outside a text of {} bytes",
            self.text.len()
        );
        let id = self.rows.len() as i32;
        self.slots.push(Some(id));
        let n_chars = cur.len();
        let text = cur.into_text();
        let (list_id, item_depth) = kind.list();
        self.rows.push(ParagraphRow {
            id,
            start: start as i32,
            end: end as i32,
            start_char: self.chars.char_of(start),
            end_char: self.chars.char_of(end),
            section_id: self.section_id,
            para_id: Some(id),
            para_offset: None,
            parent_type: None,
            parent_id: None,
            fallback: false,
            fp: fp(&norm(&text)),
            kind: kind.name().to_string(),
            list_id,
            item_depth,
            text,
            n_chars,
        });
    }

    /// The paragraphs, in document order. An open paragraph is closed as prose first.
    ///
    /// The `list_id` given to `break_paragraph` counts every list the walk met; here it becomes the rank of
    /// that list among the lists with at least one paragraph, so that ids are dense.
    pub fn finish(mut self) -> Vec<ParagraphRow> {
        self.break_paragraph(ParaKind::Prose);
        let mut lists: Vec<i32> = self.rows.iter().filter_map(|r| r.list_id).collect();
        lists.sort_unstable();
        lists.dedup();
        for r in &mut self.rows {
            if let Some(l) = r.list_id {
                r.list_id = Some(lists.binary_search(&l).expect("collected above") as i32);
            }
        }
        self.rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_collapses_and_trims() {
        let mut f = Flat::default();
        for s in ["  a", " \n\t b  ", "\u{a0}", "c", "  "] {
            f.push(s);
        }
        assert_eq!(f.len(), 5);
        assert_eq!(f.into_text(), "a b c");
    }

    #[test]
    fn anchors_slots_and_dropped_paragraphs() {
        let text = "Раз два.\n\n{{x}}\n\nТри";
        let chars = CharIndex::new(text);
        let mut b = TextBuilder::new(text, &chars);
        b.push_text("Раз ", 0);
        assert_eq!((b.anchor(), b.slot()), (3, 0)); // the pending space is not written yet
        b.push_text("два.", 7);
        b.break_paragraph(ParaKind::Prose);
        b.break_paragraph(ParaKind::Prose); // nothing met since: no-op, the slot stays open
        assert_eq!(b.slot(), 1);
        b.touch(16, 21); // an excluded node only: dropped
        b.break_paragraph(ParaKind::Prose);
        b.push_text("Три", 23);
        b.set_section(3);
        b.break_paragraph(ParaKind::ListItem { list_id: 0, depth: 1 });
        assert_eq!((b.para_id(0), b.para_id(1), b.para_id(2), b.para_id(3)), (Some(0), None, Some(1), None));
        let rows = b.finish();
        let v: Vec<_> =
            rows.iter().map(|r| (r.id, r.start, r.end, r.section_id, r.kind.as_str(), r.text.as_str())).collect();
        assert_eq!(v, vec![(0, 0, 14, 0, "prose", "Раз два."), (1, 23, 29, 3, "list_item", "Три")]);
        let r = &rows[1];
        assert_eq!((r.start_char, r.end_char, r.list_id, r.item_depth), (17, 20, Some(0), Some(1)));
    }

    #[test]
    fn whitespace_only_text_does_not_extend_the_span() {
        let text = "  ab  ";
        let chars = CharIndex::new(text);
        let mut b = TextBuilder::new(text, &chars);
        b.push_text("  ", 0);
        b.push_text("ab  ", 2);
        let rows = b.finish();
        assert_eq!((rows[0].start, rows[0].end), (2, 4));
    }
}
