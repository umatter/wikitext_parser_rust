//! The fallback scanner: when the parse or the walk of a page fails (step budget, depth limit, panic), the page
//! still yields its sections, refs, ref lists, templates and citations, so that a pathological version can be
//! compared with its counterpart.
//!
//! # Scope (R54)
//!
//! `scan` emits sections, refs, ref lists, templates and citations, every row with `fallback = true`. It makes
//! no paragraphs, so `para_id` and `para_offset` are `None` and the paragraphs table is empty. The rows come
//! from the walk's own builders (`walk::Collect`, R52): the scanner emits the events the walker emits, so the
//! rules of Tasks 13-17 (sections, templates, refs, citations, short footnotes) hold on both paths.
//!
//! # The pass
//!
//! One left-to-right pass over the bytes, with a stack of open elements (`Frame`) and no recursion, then one
//! pass over what it found to emit the events in document order.
//!
//! - **Templates** `{{…}}` by brace counting: `{{` opens one, `}}` closes the nearest open one (a `{{{…}}}`
//!   parameter is no component; one that meets `}}` without a third `}` is a template from its second brace,
//!   as the parser reads it). Separators are the `|`, `=` and `:` met while a template is the innermost open
//!   element: `|` splits parameters, the first `=` of a parameter names it, the first `:` before the first `|`
//!   is the colon of a module or parser function. A `|` inside a link, an external link or a tag inside the
//!   template therefore does not split it.
//! - **Links** `[[target|label]]` (the target, up to `|` or `]]`, on one line and without `[`, `{`, `}`, is
//!   not looked into) and **external links** `[url label]` (a `[` before one of the site's protocols, on one
//!   line). A link opening in the label of a link that is no file's makes the outer one text, as in the parser.
//! - **Tags**, case-insensitive, by a bounded lookahead for the name (`MAX_TAG_NAME` bytes), classed as the
//!   parser classes them (`Settings::tag_class`): an unknown name is text. A tag ends at its first `>`, as the
//!   parser ends it; an extension tag is none when a `<` comes first (the parser would read it, and read it
//!   again as text when its element does not close: the scanner keeps to one pass). `<ref>` and
//!   `<references>` are components; any other paired extension tag (`<poem>`, `<includeonly>`) is an element
//!   whose content is scanned, and an unclosed one is text. An HTML start or end tag is one unit. `<nowiki>`
//!   and `<math>` (the parser's plain-text tags) and `<pre>` are skipped regions, found with their closing
//!   tag, as are comments `<!-- … -->`. A closing extension tag closes the nearest open element of its name;
//!   as in the parser, it also ends a comment or a skipped region it is met in (a skipped region it cuts is
//!   none), and an unclosed comment runs to the end.
//!
//!   `<pre>` is a skipped region by R54, as MediaWiki reads it; the vendored parser reads it as an HTML tag
//!   and its content as wikitext, so a component or a heading inside `<pre>` is a row only on the parsed path.
//! - **Headings**, as the parser reads them: a line that starts with `=` opens one, whose content starts after
//!   the `=`s and the whitespace after them (newlines included) and ends at the first newline met while no
//!   element opened in it is open; it is a heading when it ends with `=` (trailing spaces and tabs aside),
//!   at the parser's level rule. It counts only outside templates, refs and every other element: one met
//!   inside an element that closes is dropped when it closes. Skipped regions are never looked into.
//! - **Malformed input.** A closer finds its opener under unclosed elements, which are dropped. An element
//!   that never closes (an unclosed `{{`, `<ref>`, `[[`) is no component: its text stays, what it contained is
//!   its parent's (components, heading lines and separators alike, as the parser's rescan would find them).
//! - **Depth.** Past `DEFAULT_DEPTH_CAP` open elements no element opens; `{{` and `}}` are still counted, so
//!   the closes of the outer templates are found, and a closing tag of an open element still closes it.
//!
//! Lookaheads are bounded or consume what they scan: a tag name, an entity or a magic word, a link target up
//! to its first `[`, `{`, `}`, `]`, `|` or newline, a heading line. The searches that are not (the `>` that
//! ends a tag, the closing tag of a skipped region) keep what they found, so an open tag with no `>` or an
//! unclosed `<nowiki>` costs one search, not one per opener.
//!
//! # Places
//!
//! Each component is in the section of the last heading before it. With no paragraphs, the citations
//! collector's bodies outside refs (R49: one per paragraph on the parsed path) are paragraph slots of the
//! running text between top-level components (`Paras`): a slot ends at an empty line, at each list item and
//! divider, and where a list or a preformatted block starts or ends, and one newline does not end it. A
//! component inside another is in the slot of the outermost one, as on the parsed path an element inside
//! excluded content takes the place of its outermost excluded ancestor.
//!
//! # Text (R53)
//!
//! `value_text`, `body_text` and `title_text` are a linear strip of the source: comments, templates,
//! `{{{parameters}}}`, refs, ref lists, `<math>`, self-closing extension tags and the extension tags whose
//! content the text rules exclude (`walk::EXCLUDED_TAGS`, `includeonly` among them, R29) are removed;
//! `<nowiki>` and `<pre>` keep their raw content; `[[a|b]]` is `b` and `[[a]]` is `a` (without a leading colon
//! or a namespace prefix; files, categories and interlanguage links are removed, as the text rules remove
//! them); `[url label]` is `label`; `''` and `'''` are removed; HTML tags are removed and keep their text
//! (`<br>` is a space); whitespace is collapsed. Beyond R53, as the parser reads them too, character entities
//! are decoded and magic words (`__TOC__`) removed; the parser's tables of both are its own, so each one met
//! is asked of the parser once. An external link's label (the `title` of a `bare_link` citation) follows the
//! walk's rules for one (`label_of` in `walk/mod.rs`): tags and their content add nothing, a link without a
//! label adds its target.

use std::cell::RefCell;
use std::collections::HashMap;

use parse_wiki_text_2::Node;

use crate::frontend;
use crate::model::Components;
use crate::page::DEFAULT_DEPTH_CAP;
use crate::registry::Registry;
use crate::settings::{Settings, TagClass};
use crate::spans::CharIndex;
use crate::walk::{self, trim_range, Collect, Flat, ParamEvent, Place, EXCLUDED_TAGS};

/// The longest tag name looked for (`syntaxhighlight` has 15 bytes).
const MAX_TAG_NAME: usize = 16;

/// The longest character entity name looked for (`thetasym` has 8 bytes).
const MAX_ENTITY_NAME: usize = 10;

/// The longest magic word looked for, in bytes (`БЕЗ_РЕДАКТИРОВАНИЯ_РАЗДЕЛА` has 49).
const MAX_MAGIC_WORD: usize = 80;

/// The sections, refs, ref lists, templates and citations of `text`, from one linear pass over it. Every row has
/// `fallback = true`; there are no paragraphs.
pub fn scan(text: &str, settings: &Settings, registry: &Registry) -> Components {
    let mut s = Scanner::new(text, settings);
    s.run();
    let chars = CharIndex::new(text);
    let mut c = s.emit(&chars, registry);
    c.sections.iter_mut().for_each(|r| r.fallback = true);
    c.templates.iter_mut().for_each(|r| r.fallback = true);
    c.refs.iter_mut().for_each(|r| r.fallback = true);
    c.ref_lists.iter_mut().for_each(|r| r.fallback = true);
    c.citations.iter_mut().for_each(|r| r.fallback = true);
    c
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// `{{…}}`.
    Template,
    /// `{{{…}}}`: no component; adds no text.
    Param,
    /// `[[…]]`.
    Link,
    /// `[url …]`.
    ExtLink,
    /// `<ref>…</ref>`, `<ref/>`.
    Ref,
    /// `<references>…</references>`, `<references/>`.
    RefList,
    /// Any other paired extension tag; `muted` when its content adds no text.
    Tag { muted: bool },
    /// A skipped region whose content is raw text: `<nowiki>`, `<pre>`.
    Raw,
    /// A skipped region that adds no text: a comment, `<math>`, a self-closing extension tag.
    Mute,
    /// An HTML start or end tag, as the parser reads one: a unit that adds nothing (`<br>` a space).
    /// `inner` is its name.
    Html,
    /// A heading line, open until the newline that ends it. Its item never closes (the heading is a `Head`),
    /// and it leaves the separators and headings met in it to its parent.
    Heading { level: usize },
}

/// An element, in the order of its opener. Only a closed one is a component (or a link, a tag, a region).
#[derive(Debug)]
struct Item {
    kind: Kind,
    start: usize,
    end: usize,
    closed: bool,
    /// Once closed: the index of the first item after its end (the items between are inside it).
    next: usize,
    /// Tags and skipped regions: the content, between the open tag and the closing tag. Links: the target.
    inner: (usize, usize),
    /// A link's `|`.
    pipe: Option<usize>,
    /// A template's separators, in order: `|`, `=`, `:` and their bytes.
    seps: Vec<(usize, u8)>,
}

/// An open element.
struct Frame {
    item: usize,
    /// The lengths of `Scanner::seps` and `Scanner::heads` when it opened.
    seps: usize,
    heads: usize,
    /// A tag's name.
    tag: Option<String>,
}

/// A heading line: its span (from the line start to after the last `=`) and its level.
#[derive(Clone, Copy, Debug)]
struct Head {
    start: usize,
    end: usize,
    level: usize,
}

/// The kind of a line of running text, for where paragraphs break.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Line {
    #[default]
    Text,
    /// `*`, `#`, `:`, `;`: each item is a paragraph.
    Item,
    /// A leading space: one preformatted block.
    Pre,
    /// `----`.
    Divider,
}

/// The paragraph slots of the running text between top-level components, a stand-in for the walk's
/// paragraphs that the citations collector groups bodies by outside refs (R49). A slot ends at an empty line,
/// at each list item and divider, and where a list or a preformatted block starts or ends; one newline does
/// not end it, and what a component spans is skipped.
#[derive(Default)]
struct Paras {
    slot: usize,
    /// Where the running text not yet read starts.
    counted: usize,
    /// Read up to a line start.
    fresh: bool,
    /// The kind of the current line, and whether an empty line came after it.
    line: Line,
    blank: bool,
}

impl Paras {
    /// Read the running text up to `to`, where a component starts.
    fn advance(&mut self, b: &[u8], to: usize) {
        let mut i = self.counted;
        if i == 0 {
            self.fresh = true;
        }
        while i <= to {
            if self.fresh {
                self.fresh = false;
                let mut j = i;
                while j < to && matches!(b[j], b' ' | b'\t') {
                    j += 1;
                }
                if j < to && b[j] == b'\n' {
                    self.blank = true;
                } else {
                    let line = match b.get(i) {
                        Some(b'*' | b'#' | b':' | b';') => Line::Item,
                        Some(b' ') => Line::Pre,
                        _ if b[i..].starts_with(b"----") => Line::Divider,
                        _ => Line::Text,
                    };
                    if self.blank || line != self.line || matches!(line, Line::Item | Line::Divider) {
                        self.slot += 1;
                    }
                    (self.line, self.blank) = (line, false);
                }
            }
            match b[i..to].iter().position(|&c| c == b'\n') {
                Some(p) => (i, self.fresh) = (i + p + 1, true),
                None => break,
            }
        }
        self.counted = to;
    }
}

/// A template's name and parameters, read from its separators.
struct Layout {
    name: (usize, usize),
    colon: Option<usize>,
    /// Name span (named only), value span and span of each parameter.
    params: Vec<(Option<(usize, usize)>, (usize, usize), (usize, usize))>,
}

struct Scanner<'t, 's> {
    text: &'t str,
    b: &'t [u8],
    settings: &'s Settings,
    items: Vec<Item>,
    frames: Vec<Frame>,
    /// Separators met inside elements; an element that closes takes (a template) or drops those met since it
    /// opened, and one dropped unclosed leaves them to its parent.
    seps: Vec<(usize, u8)>,
    /// Heading lines met, the same way: one inside an element that closes is dropped.
    heads: Vec<Head>,
    /// `{{` met past the depth cap and not closed yet.
    excess: usize,
    /// Open templates and parameters, refs, ref lists, and tags by name: a closer without an opener costs
    /// nothing.
    braces: usize,
    refs: usize,
    lists: usize,
    tags: HashMap<String, usize>,
    /// Open refs, ref lists and tags together.
    open_tags: usize,
    /// Per skipped-region tag (`nowiki`, `pre`, `math`): the last search for its closing tag, from where, and
    /// what it found.
    closers: [Option<(usize, RegionEnd)>; 3],
    /// The last search for a `>`, from where, and what it found.
    gt: Option<(usize, Option<usize>)>,
    /// Character entities by source (`&nbsp;`), as the parser decodes them.
    entities: RefCell<HashMap<String, Option<char>>>,
    /// Magic words by source (`__TOC__`), as the parser reads them.
    magic_words: RefCell<HashMap<String, bool>>,
}

/// ASCII whitespace as the parser skips it.
fn is_ws(b: u8) -> bool {
    matches!(b, b'\t' | b'\n' | b' ')
}

fn skip_ws_forwards(b: &[u8], mut pos: usize) -> usize {
    while pos < b.len() && is_ws(b[pos]) {
        pos += 1;
    }
    pos
}

fn skip_ws_backwards(b: &[u8], mut pos: usize) -> usize {
    while pos > 0 && is_ws(b[pos - 1]) {
        pos -= 1;
    }
    pos
}

/// The first `needle` at or after `from`.
fn find(b: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    let hay = b.get(from..)?;
    hay.windows(needle.len()).position(|w| w == needle).map(|i| from + i)
}

/// What the search for a skipped region's closing tag found.
#[derive(Clone, Debug)]
enum RegionEnd {
    /// Its closing tag: start and end.
    Closer(usize, usize),
    /// First, the closing tag of an open element, at this position and of this name.
    Conflict(usize, String),
    /// Neither, up to the end of the text.
    Nothing,
}

impl<'t, 's> Scanner<'t, 's> {
    fn new(text: &'t str, settings: &'s Settings) -> Self {
        Scanner {
            text,
            b: text.as_bytes(),
            settings,
            items: Vec::new(),
            frames: Vec::new(),
            seps: Vec::new(),
            heads: Vec::new(),
            excess: 0,
            braces: 0,
            refs: 0,
            lists: 0,
            tags: HashMap::new(),
            open_tags: 0,
            closers: [None, None, None],
            gt: None,
            entities: RefCell::new(HashMap::new()),
            magic_words: RefCell::new(HashMap::new()),
        }
    }

    fn at(&self, i: usize) -> u8 {
        self.b.get(i).copied().unwrap_or(0)
    }

    /// No element opens: past the depth cap.
    fn full(&self) -> bool {
        self.excess > 0 || self.frames.len() >= DEFAULT_DEPTH_CAP
    }

    fn top(&self) -> Option<Kind> {
        self.frames.last().map(|f| self.items[f.item].kind)
    }

    fn count(&mut self, kind: Kind, tag: Option<&str>, up: bool) {
        if matches!(kind, Kind::Ref | Kind::RefList | Kind::Tag { .. }) {
            match up {
                true => self.open_tags += 1,
                false => self.open_tags -= 1,
            }
        }
        let n = match kind {
            Kind::Template | Kind::Param => &mut self.braces,
            Kind::Ref => &mut self.refs,
            Kind::RefList => &mut self.lists,
            Kind::Tag { .. } => self.tags.entry(tag.unwrap_or_default().to_string()).or_default(),
            _ => return,
        };
        if up {
            *n += 1;
        } else {
            *n -= 1;
        }
    }

    fn push(&mut self, kind: Kind, start: usize, inner: (usize, usize), pipe: Option<usize>, tag: Option<String>) {
        let k = self.items.len();
        self.items.push(Item { kind, start, end: start, closed: false, next: k + 1, inner, pipe, seps: Vec::new() });
        self.count(kind, tag.as_deref(), true);
        self.frames.push(Frame { item: k, seps: self.seps.len(), heads: self.heads.len(), tag });
    }

    /// An element found whole: a self-closing tag, a link without a label, a skipped region.
    fn complete(&mut self, kind: Kind, start: usize, end: usize, inner: (usize, usize)) {
        let k = self.items.len();
        self.items.push(Item { kind, start, end, closed: true, next: k + 1, inner, pipe: None, seps: Vec::new() });
    }

    /// The innermost open element never closes: no component; what it met is its parent's.
    fn drop_top(&mut self) {
        let f = self.frames.pop().expect("an open element");
        self.count(self.items[f.item].kind, f.tag.as_deref(), false);
    }

    /// The innermost open element closes at `end`; its closing tag (or `]`, `}}`) starts at `close`.
    fn close_top(&mut self, end: usize, close: usize) {
        let f = self.frames.pop().expect("an open element");
        let next = self.items.len();
        let kind = self.items[f.item].kind;
        self.count(kind, f.tag.as_deref(), false);
        let it = &mut self.items[f.item];
        (it.closed, it.end, it.next) = (true, end, next);
        match kind {
            Kind::Template => it.seps = self.seps.split_off(f.seps),
            Kind::Ref | Kind::RefList | Kind::Tag { .. } => {
                it.inner.1 = close;
                self.seps.truncate(f.seps);
            }
            _ => self.seps.truncate(f.seps),
        }
        self.heads.truncate(f.heads);
    }

    fn run(&mut self) {
        let n = self.b.len();
        let (mut i, mut line_start) = (0, true);
        while i < n {
            if line_start {
                line_start = false;
                if self.b[i] == b'=' && !self.full() {
                    i = self.open_heading(i);
                    continue;
                }
            }
            let c = self.b[i];
            i = match c {
                b'\n' => {
                    // An external link ends on its line or is none.
                    while self.excess == 0 && self.top() == Some(Kind::ExtLink) {
                        self.drop_top();
                    }
                    if self.excess == 0 && matches!(self.top(), Some(Kind::Heading { .. })) {
                        self.close_heading(i);
                    }
                    line_start = true;
                    i + 1
                }
                b'{' if self.at(i + 1) == b'{' => self.open_braces(i),
                b'}' if self.at(i + 1) == b'}' => self.close_braces(i),
                b'[' if self.at(i + 1) == b'[' => self.open_link(i),
                b'[' => {
                    if !self.full() && self.settings.starts_with_protocol(&self.text[i + 1..]) {
                        self.push(Kind::ExtLink, i, (i, i), None, None);
                    }
                    i + 1
                }
                b']' => self.close_bracket(i),
                b'|' | b'=' | b':' => {
                    if !self.frames.is_empty() && self.excess == 0 {
                        self.seps.push((i, c));
                    }
                    i + 1
                }
                b'<' => self.tag(i),
                _ => i + 1,
            };
        }
        while let Some(top) = self.top() {
            match top {
                Kind::Heading { .. } if self.excess == 0 => self.close_heading(n),
                _ => self.drop_top(),
            }
        }
    }

    /// A line starting at `i` with `=` opens a heading, as the parser opens one: its content starts after the
    /// `=`s (at most six) and the whitespace after them, newlines included. Returns where scanning goes on.
    fn open_heading(&mut self, i: usize) -> usize {
        let b = self.b;
        let mut level = 1;
        while level < 6 && b.get(i + level) == Some(&b'=') {
            level += 1;
        }
        // Inside another element the `=`s are that element's separators, as the parser reads no heading there.
        let enclosed = !self.frames.is_empty();
        self.push(Kind::Heading { level }, i, (i, i), None, None);
        if enclosed {
            self.seps.extend((i..i + level).map(|k| (k, b'=')));
        }
        skip_ws_forwards(b, i + level)
    }

    /// The open heading on top meets the newline at `pos` (or the end of the text): it is a heading when its
    /// line ends with `=`, at the parser's level; otherwise its text is text.
    fn close_heading(&mut self, pos: usize) {
        let b = self.b;
        let f = self.frames.pop().expect("an open heading");
        let (start, Kind::Heading { level: start_level }) = (self.items[f.item].start, self.items[f.item].kind) else {
            unreachable!("the top is a heading")
        };
        let mut end = pos;
        while end > start && matches!(b[end - 1], b' ' | b'\t') {
            end -= 1;
        }
        if end < start + 3 || b[end - 1] != b'=' {
            return;
        }
        // A heading met inside it, in an element that never closed, would overlap it: the parser ends this
        // heading first, at a newline the scan has passed. Rare; neither counts then.
        if self.heads.len() > f.heads {
            self.heads.truncate(f.heads);
            return;
        }
        let mut level = 1;
        while level < start_level && end - level > start + level + 2 && b[end - level - 1] == b'=' {
            level += 1;
        }
        self.heads.push(Head { start, end, level });
    }

    fn open_braces(&mut self, i: usize) -> usize {
        if self.full() {
            self.excess += 1;
            return i + 2;
        }
        if self.at(i + 2) == b'{' {
            self.push(Kind::Param, i, (i, i), None, None);
            return i + 3;
        }
        self.push(Kind::Template, i, (i, i), None, None);
        i + 2
    }

    fn close_braces(&mut self, i: usize) -> usize {
        if self.excess > 0 {
            self.excess -= 1;
            return i + 2;
        }
        if self.braces == 0 {
            return i + 2;
        }
        while !matches!(self.top(), Some(Kind::Template | Kind::Param)) {
            self.drop_top();
        }
        let item = self.frames.last().expect("a template or parameter").item;
        if self.items[item].kind == Kind::Param {
            if self.at(i + 2) == b'}' {
                self.close_top(i + 3, i);
                return i + 3;
            }
            // `{{{x}}`: the parser rewinds the parameter and reads a template from its second brace.
            let it = &mut self.items[item];
            (it.kind, it.start) = (Kind::Template, it.start + 1);
        }
        self.close_top(i + 2, i);
        i + 2
    }

    fn open_link(&mut self, i: usize) -> usize {
        // A link opening in the label of a link that is no file's: the parser reads the outer one as text.
        if let Some(f) = self.frames.last().filter(|_| self.excess == 0) {
            let it = &self.items[f.item];
            if it.kind == Kind::Link && self.link_namespace(it) != Some(6) {
                self.drop_top();
            }
        }
        if self.full() {
            return i + 2;
        }
        let b = self.b;
        let t = skip_ws_forwards(b, i + 2);
        let mut j = t;
        loop {
            match b.get(j) {
                None | Some(b'\n' | b'[' | b'{' | b'}') => return i + 1,
                Some(b']') if self.at(j + 1) == b']' => {
                    self.complete(Kind::Link, i, j + 2, (t, j));
                    return j + 2;
                }
                Some(b']') => return i + 1,
                Some(b'|') => {
                    self.push(Kind::Link, i, (t, j), Some(j), None);
                    self.seps.push((j, b'|'));
                    return j + 1;
                }
                _ => j += 1,
            }
        }
    }

    fn close_bracket(&mut self, i: usize) -> usize {
        if self.excess == 0 {
            match self.top() {
                Some(Kind::ExtLink) => {
                    self.close_top(i + 1, i);
                    return i + 1;
                }
                Some(Kind::Link) if self.at(i + 1) == b']' => {
                    self.close_top(i + 2, i);
                    return i + 2;
                }
                _ => {}
            }
        }
        i + 1
    }

    /// The closing tag of a skipped region `name` whose open tag ends at `from`: its start and end. `None` when
    /// the text ends first, or when the closing tag of an open element comes first, as the parser lets a
    /// plain-text tag be closed by nothing else and then reads its open tag as text.
    ///
    /// A search from an earlier position answers when what it found is still ahead (and, for a closing tag of
    /// an open element, the element is still open), so that unclosed regions cost one search, not one per open
    /// tag. An element opened between the two positions whose closing tag lies in the region is not seen then.
    fn region_end(&mut self, name: &str, from: usize) -> Option<(usize, usize)> {
        let slot = match name {
            "nowiki" => 0,
            "pre" => 1,
            _ => 2,
        };
        if let Some((searched_from, found)) = &self.closers[slot] {
            if from >= *searched_from {
                match found {
                    RegionEnd::Closer(s, e) if *s >= from => return Some((*s, *e)),
                    RegionEnd::Conflict(p, n) if *p >= from && self.is_open(n) => return None,
                    RegionEnd::Nothing => return None,
                    _ => {}
                }
            }
        }
        let b = self.b;
        let mut k = from;
        let found = loop {
            match find(b, k, b"</") {
                None => break RegionEnd::Nothing,
                Some(p) => match self.plain_closer(p) {
                    Some((n, end)) if n == name => break RegionEnd::Closer(p, end),
                    Some((n, _)) if self.open_tags > 0 && self.is_open(&n) => break RegionEnd::Conflict(p, n),
                    _ => k = p + 2,
                },
            }
        };
        let answer = match found {
            RegionEnd::Closer(s, e) => Some((s, e)),
            _ => None,
        };
        self.closers[slot] = Some((from, found));
        answer
    }

    /// Where the comment opening at `i` ends: after its `-->`; at the closing tag of an open element in it,
    /// which the parser lets close the element; or at the end of the text. One scan, which consumes it.
    fn comment_end(&self, i: usize) -> usize {
        let b = self.b;
        let mut k = i + 4;
        while k < b.len() {
            match b[k] {
                b'-' if b[k..].starts_with(b"-->") => return k + 3,
                b'<' if self.open_tags > 0 && self.at(k + 1) == b'/' => match self.plain_closer(k) {
                    Some((name, _)) if self.is_open(&name) => return k,
                    _ => k += 2,
                },
                _ => k += 1,
            }
        }
        b.len()
    }

    /// The closing tag at `k` (`</name>`, whitespace allowed before `>`) as the parser reads one inside a
    /// comment or a plain-text tag: its lowercased name and its end.
    fn plain_closer(&self, k: usize) -> Option<(String, usize)> {
        let b = self.b;
        let ns = k + 2;
        let mut j = ns;
        loop {
            match b.get(j) {
                None | Some(b'/' | b'<') => return None,
                Some(b'\t' | b'\n' | b' ') => {
                    let q = skip_ws_forwards(b, j + 1);
                    return (b.get(q) == Some(&b'>')).then(|| (self.text[ns..j].to_ascii_lowercase(), q + 1));
                }
                Some(b'>') => return Some((self.text[ns..j].to_ascii_lowercase(), j + 1)),
                _ if j - ns >= MAX_TAG_NAME => return None,
                _ => j += 1,
            }
        }
    }

    /// An open ref, ref list or tag of this name.
    fn is_open(&self, name: &str) -> bool {
        match name {
            "ref" => self.refs > 0,
            "references" => self.lists > 0,
            _ => self.tags.get(name).is_some_and(|&n| n > 0),
        }
    }

    /// The first `>` at or after `from`, from the cache when it can answer.
    fn next_gt(&mut self, from: usize) -> Option<usize> {
        if let Some((searched_from, found)) = self.gt {
            if from >= searched_from && found.is_none_or(|p| p >= from) {
                return found;
            }
        }
        let found = self.b.get(from..)?.iter().position(|&c| c == b'>').map(|p| from + p);
        self.gt = Some((from, found));
        found
    }

    fn tag(&mut self, i: usize) -> usize {
        let b = self.b;
        if b[i..].starts_with(b"<!--") {
            let end = self.comment_end(i);
            self.complete(Kind::Mute, i, end, (end, end));
            return end;
        }
        let closing = self.at(i + 1) == b'/';
        let ns = i + 1 + usize::from(closing);
        let mut j = ns;
        while j < b.len() && !matches!(b[j], b'\t' | b'\n' | b' ' | b'/' | b'>') {
            if j - ns >= MAX_TAG_NAME {
                return i + 1;
            }
            j += 1;
        }
        let name = self.text[ns..j].to_ascii_lowercase();
        if closing {
            return self.end_tag(i, j, &name);
        }
        let Some(class) = self.settings.tag_class(&name) else { return i + 1 };
        // The open tag ends at its first `>`, as the parser ends it; an extension tag's is no tag when a `<`
        // comes first.
        let gt = match class {
            TagClass::Html => self.next_gt(j),
            TagClass::Extension => b[j..].iter().position(|&c| c == b'>' || c == b'<').map(|p| j + p).filter(|&p| b[p] == b'>'),
        };
        let Some(gt) = gt else { return i + 1 };
        let open_end = gt + 1;
        let self_closing = b[gt - 1] == b'/';
        // `<pre>` is a skipped region (R54), although the parser reads it as an HTML tag.
        let region = match (class, name.as_str()) {
            (_, "pre") | (TagClass::Extension, "nowiki" | "math") => !self_closing,
            _ => false,
        };
        if region {
            if let Some((close, end)) = self.region_end(&name, open_end) {
                let kind = if name == "math" { Kind::Mute } else { Kind::Raw };
                self.complete(kind, i, end, (open_end, close));
                return end;
            }
            if class == TagClass::Extension {
                return i + 1;
            }
        }
        if class == TagClass::Html {
            self.complete(Kind::Html, i, open_end, (ns, j));
            return open_end;
        }
        match name.as_str() {
            _ if self.full() => self.complete(Kind::Mute, i, open_end, (open_end, open_end)),
            "ref" | "references" => {
                let kind = if name == "ref" { Kind::Ref } else { Kind::RefList };
                match self_closing {
                    true => self.complete(kind, i, open_end, (open_end, open_end)),
                    false => self.push(kind, i, (open_end, open_end), None, None),
                }
            }
            _ if self_closing => self.complete(Kind::Mute, i, open_end, (open_end, open_end)),
            _ => {
                let muted = EXCLUDED_TAGS.contains(&name.as_str());
                self.push(Kind::Tag { muted }, i, (open_end, open_end), None, Some(name));
            }
        }
        open_end
    }

    /// `</name`, its name ending at `j`: closes the nearest open element of that name, dropping the unclosed
    /// ones inside it.
    fn end_tag(&mut self, i: usize, j: usize, name: &str) -> usize {
        match self.settings.tag_class(name) {
            None => return i + 1,
            Some(TagClass::Html) => {
                // An end tag runs to its first `>`.
                let Some(gt) = self.next_gt(j) else { return i + 1 };
                self.complete(Kind::Html, i, gt + 1, (i + 2, j));
                return gt + 1;
            }
            Some(TagClass::Extension) => {}
        }
        let k = skip_ws_forwards(self.b, j);
        if self.at(k) != b'>' {
            return i + 1;
        }
        let end = k + 1;
        let open = match name {
            "ref" => self.refs,
            "references" => self.lists,
            _ => self.tags.get(name).copied().unwrap_or(0),
        };
        if open > 0 {
            self.excess = 0;
            loop {
                let f = self.frames.last().expect("an open element of that name");
                let matched = match self.items[f.item].kind {
                    Kind::Ref => name == "ref",
                    Kind::RefList => name == "references",
                    Kind::Tag { .. } => f.tag.as_deref() == Some(name),
                    _ => false,
                };
                if matched {
                    break;
                }
                self.drop_top();
            }
            self.close_top(end, i);
            return end;
        }
        // No open element of that name: the parser reads the closing tag as text.
        i + 1
    }

    /// A template's name and parameters, from its separators, as the parser reads them.
    fn layout(&self, it: &Item) -> Layout {
        let b = self.b;
        let close = it.end - 2;
        let mut pipes: Vec<(usize, Option<usize>)> = Vec::new();
        let mut colon = None;
        for &(p, c) in &it.seps {
            match c {
                b'|' => pipes.push((p, None)),
                b'=' => {
                    if let Some(last) = pipes.last_mut() {
                        last.1 = last.1.or(Some(p));
                    }
                }
                _ => {
                    if pipes.is_empty() {
                        colon = colon.or(Some(p));
                    }
                }
            }
        }
        let name = trim_range(self.text, it.start + 2, pipes.first().map_or(close, |p| p.0));
        let params = pipes
            .iter()
            .enumerate()
            .map(|(k, &(pipe, eq))| {
                let next = pipes.get(k + 1).map_or(close, |q| q.0);
                let (start, end) = (skip_ws_forwards(b, pipe + 1), skip_ws_backwards(b, next));
                match eq {
                    Some(q) => (
                        Some(trim_range(self.text, start, skip_ws_backwards(b, q))),
                        trim_range(self.text, skip_ws_forwards(b, q + 1), end),
                        (start, end),
                    ),
                    None => (None, trim_range(self.text, start, end), (start, end)),
                }
            })
            .collect();
        Layout { name, colon, params }
    }

    /// The namespace of a link's target, when it has a prefix and no leading colon.
    fn link_namespace(&self, it: &Item) -> Option<i32> {
        let target = self.text[it.inner.0..it.inner.1].trim();
        let (prefix, _) = target.split_once(':').filter(|_| !target.starts_with(':'))?;
        self.settings.namespace(prefix)
    }

    /// A link whose text the text rules remove: a file, a category, an interlanguage link; in an external
    /// link's label (`label`), a file or a category, which the parser makes no link of.
    fn link_muted(&self, it: &Item, label: bool) -> bool {
        let target = self.text[it.inner.0..it.inner.1].trim();
        matches!(self.link_namespace(it), Some(6 | 14)) || (!label && walk::is_interlanguage(self.settings, target))
    }

    /// Where an external link's label starts: at the first whitespace after its URL, outside the elements in it.
    fn label_start(&self, idx: usize) -> Option<usize> {
        let it = &self.items[idx];
        let (mut pos, end) = (it.start + 1, it.end - 1);
        let mut k = idx + 1;
        while pos < end {
            while k < it.next && !self.items[k].closed {
                k += 1;
            }
            let stop = if k < it.next { self.items[k].start.min(end) } else { end };
            if let Some(w) = self.text[pos..stop].find(char::is_whitespace) {
                return Some(pos + w);
            }
            if k >= it.next {
                return None;
            }
            pos = self.items[k].end;
            k = self.items[k].next;
        }
        None
    }

    /// The text of `a..b` (R53).
    fn strip(&self, a: usize, b: usize) -> String {
        self.strip_as(a, b, false)
    }

    /// The text of `a..b`; as an external link's label (`label`), under the walk's rules for one: the label's
    /// own text, entities and links; tags and their content add nothing, a link without a label adds its
    /// target.
    fn strip_as(&self, a: usize, b: usize, label: bool) -> String {
        let mut out = Flat::default();
        let mut k = self.items.partition_point(|it| it.start < a);
        let mut pos = a;
        // Where the content of an element being read ends, and the length of its closer; innermost last.
        let mut ends: Vec<(usize, usize)> = Vec::new();
        while pos < b {
            while let Some(it) = self.items.get(k) {
                match (it.closed, it.start < pos) {
                    (false, _) => k += 1,
                    (true, true) => k = it.next,
                    (true, false) => break,
                }
            }
            let next_item = self.items.get(k).map_or(usize::MAX, |it| it.start);
            let next_end = ends.last().map_or(usize::MAX, |e| e.0);
            let stop = b.min(next_item).min(next_end);
            if pos < stop {
                self.plain(&mut out, pos, stop);
                pos = stop;
            }
            if pos >= b {
                break;
            }
            if pos == next_end {
                pos += ends.pop().expect("an open element").1;
                continue;
            }
            let it = &self.items[k];
            match it.kind {
                Kind::Raw if !label => {
                    out.push(&self.text[it.inner.0..it.inner.1]);
                    (pos, k) = (it.end, it.next);
                }
                Kind::Tag { muted: false } if !label => {
                    ends.push((it.inner.1, it.end - it.inner.1));
                    (pos, k) = (it.inner.0, k + 1);
                }
                Kind::Link => match (self.link_muted(it, label), it.pipe) {
                    (false, Some(p)) => {
                        ends.push((it.end - 2, 2));
                        (pos, k) = (p + 1, k + 1);
                    }
                    (false, None) => {
                        let target = &self.text[it.inner.0..it.inner.1];
                        out.push(if label { target } else { walk::link_title(self.settings, target) });
                        (pos, k) = (it.end, it.next);
                    }
                    (true, _) => (pos, k) = (it.end, it.next),
                },
                Kind::Html => {
                    if !label && self.text[it.inner.0..it.inner.1].eq_ignore_ascii_case("br") {
                        out.push(" ");
                    }
                    (pos, k) = (it.end, it.next);
                }
                Kind::ExtLink => match self.label_start(k) {
                    Some(l) => {
                        ends.push((it.end - 1, 1));
                        (pos, k) = (l, k + 1);
                    }
                    None => (pos, k) = (it.end, it.next),
                },
                _ => (pos, k) = (it.end, it.next),
            }
        }
        out.into_text()
    }

    /// A named character entity at `i` (`&nbsp;`), before `to`, as the parser decodes it: the character and
    /// the entity's end. The parser's entity table is its own, so each name is asked of the parser once.
    fn entity(&self, i: usize, to: usize) -> Option<(char, usize)> {
        let b = self.b;
        let semi = (i + 1..to.min(i + 2 + MAX_ENTITY_NAME)).find(|&k| !b[k].is_ascii_alphanumeric())?;
        if b[semi] != b';' || semi == i + 1 {
            return None;
        }
        let name = &self.text[i..=semi];
        let mut cache = self.entities.borrow_mut();
        let c = *cache.entry(name.to_string()).or_insert_with(|| {
            match frontend::parse(name, self.settings.configuration(), 0).ok()?.output.nodes.as_slice() {
                [Node::CharacterEntity { character, .. }] => Some(*character),
                _ => None,
            }
        });
        c.map(|c| (c, semi + 1))
    }

    /// A magic word at `i` (`__TOC__`), before `to`, as the parser reads one: its end.
    fn magic_word(&self, i: usize, to: usize) -> Option<usize> {
        let end = find(&self.b[..to.min(i + 4 + MAX_MAGIC_WORD)], i + 2, b"__")? + 2;
        let word = &self.text[i..end];
        let mut cache = self.magic_words.borrow_mut();
        let known = *cache.entry(word.to_string()).or_insert_with(|| {
            frontend::parse(word, self.settings.configuration(), 0)
                .is_ok_and(|p| matches!(p.output.nodes.as_slice(), [Node::MagicWord { .. }]))
        });
        known.then_some(end)
    }

    /// Text outside elements: bold and italic markers and magic words removed, character entities decoded.
    fn plain(&self, out: &mut Flat, from: usize, to: usize) {
        let b = self.b;
        let (mut run, mut i) = (from, from);
        while i < to {
            match b[i] {
                b'\'' if i + 1 < to && b[i + 1] == b'\'' => {
                    let mut j = i;
                    while j < to && b[j] == b'\'' {
                        j += 1;
                    }
                    out.push(&self.text[run..i]);
                    // `''''` is an apostrophe and bold; past five, the extra apostrophes stay.
                    let keep = match j - i {
                        2 | 3 | 5 => 0,
                        4 => 1,
                        n => n - 5,
                    };
                    out.push(&self.text[i..i + keep]);
                    (i, run) = (j, j);
                }
                b'_' if i + 1 < to && b[i + 1] == b'_' => match self.magic_word(i, to) {
                    Some(end) => {
                        out.push(&self.text[run..i]);
                        (i, run) = (end, end);
                    }
                    None => i += 1,
                },
                b'&' => match self.entity(i, to) {
                    Some((c, end)) => {
                        out.push(&self.text[run..i]);
                        out.push(c.encode_utf8(&mut [0; 4]));
                        (i, run) = (end, end);
                    }
                    None => i += 1,
                },
                _ => i += 1,
            }
        }
        out.push(&self.text[run..to]);
    }

    /// The events of what the pass found, in document order, into the walk's builders.
    fn emit(self, chars: &CharIndex, registry: &Registry) -> Components {
        struct Open {
            item: usize,
            end: usize,
            /// The paragraph slot whose body the component and what it contains are in.
            slot: usize,
            params: Vec<(Option<(usize, usize)>, (usize, usize), (usize, usize))>,
        }
        let mut collect = Collect::new(self.text, chars, self.settings, registry);
        let mut open: Vec<Open> = Vec::new();
        let (mut k, mut h) = (0, 0);
        let mut paras = Paras::default();
        loop {
            let head = self.heads.get(h).map(|x| x.start);
            let item = self.items.get(k).map(|x| x.start);
            let pos = match (head, item) {
                (None, None) => usize::MAX,
                (Some(a), Some(b)) => a.min(b),
                (a, b) => a.or(b).unwrap_or(usize::MAX),
            };
            while open.last().is_some_and(|o| o.end <= pos) {
                let o = open.pop().expect("checked");
                let it = &self.items[o.item];
                match it.kind {
                    Kind::Template => {
                        for (name, value, span) in o.params {
                            let value_text = self.strip(value.0, value.1);
                            collect.template_param(ParamEvent { name, value, span, value_text });
                        }
                        collect.template_close();
                    }
                    Kind::Ref => collect.ref_close(self.strip(it.inner.0, it.inner.1)),
                    _ => collect.ref_list_close(),
                }
            }
            if pos == usize::MAX {
                break;
            }
            if head == Some(pos) {
                let Head { start, end, level } = self.heads[h];
                let (a, b) = (start + level, end - level);
                let id = collect.heading(level as u8, (start, end), self.text[a..b].trim());
                collect.heading_title(id, self.strip(a, b));
                h += 1;
                continue;
            }
            let it = &self.items[k];
            k += 1;
            if !it.closed || !matches!(it.kind, Kind::Template | Kind::Ref | Kind::RefList | Kind::ExtLink) {
                continue;
            }
            if open.is_empty() {
                paras.advance(self.b, it.start);
            }
            let slot = open.first().map_or(paras.slot, |o| o.slot);
            let place = Place { section_id: collect.section(), para: Some((slot, 0)) };
            let span = (it.start, it.end);
            match it.kind {
                Kind::Template => {
                    let Layout { name, colon, params } = self.layout(it);
                    collect.template_open(name, colon, span, place);
                    if let Some(from) = collect.split_from() {
                        collect.template_split_text(self.strip(from, name.1));
                    }
                    open.push(Open { item: k - 1, end: it.end, slot, params });
                }
                Kind::Ref | Kind::RefList => {
                    match it.kind {
                        Kind::Ref => collect.ref_open(span, place),
                        _ => collect.ref_list_open(span, place),
                    }
                    open.push(Open { item: k - 1, end: it.end, slot, params: Vec::new() });
                }
                _ => {
                    let label =
                        self.label_start(k - 1).map(|l| self.strip_as(l, it.end - 1, true)).filter(|l| !l.is_empty());
                    collect.external_link(span, label, place);
                }
            }
            if open.len() <= 1 {
                // What a top-level component spans is no running text.
                paras.counted = paras.counted.max(it.end);
            }
        }
        collect.finish(|_| (None, None))
    }
}
