//! Free URLs: the URLs in running text that MediaWiki links without brackets (R73). The rules are those of
//! MediaWiki's `Parser::handleMagicLinks` and `Parser::makeFreeExternalLink` (read in MediaWiki 1.43.1), and
//! `scan` is the one implementation both paths use: the walk calls it on the text nodes it may link, the
//! fallback scanner on the same stretches of running text, so the two paths find the same URLs (R52).
//!
//! - **Start.** One of the site's URL protocols (`Settings::free_link_protocols`: `Settings::protocols` without
//!   the protocol-relative `//`), in any case, at a word boundary: the character before it is not a letter, a
//!   digit or `_` (MediaWiki's `\b` under `/u`).
//! - **Characters.** After the protocol, one address character (a bracketed IPv6 address `[…]` of hex digits,
//!   `:` and `.`, or a URL character), then any run of URL characters. A URL character is any character but
//!   `[`, `]`, `<`, `>`, `"`, U+0000 to U+0020, U+007F, the space separators (Unicode Zs) and U+FFFD
//!   (`Parser::EXT_LINK_URL_CLASS`).
//! - **What MediaWiki has done before.** It links free URLs after comments are removed, templates expanded
//!   and quotes made tags. So a comment (`<!-- … -->`) inside the run is skipped and adds nothing to the URL;
//!   `''` ends the run (it becomes a tag), and so does `{{` (a template or a parameter, never expanded here).
//!   In a template parameter's value, `|` and `}}` end it too, as they end the value.
//! - **Cut.** The URL ends before `&lt;`, `&gt;`, `&nbsp;` and their numeric forms (`&#60;`, `&#62;`,
//!   `&#160;`, `&#x3C;`, `&#x3E;`, `&#xA0;`, leading zeros allowed).
//! - **Trailing punctuation.** Trailing `,`, `;`, `.`, `:`, `!` and `?` are not part of the URL, nor a trailing
//!   `)` when the URL has no `(`; of that run, a first `;` that ends a character entity (`&amp;`) stays.
//! - **Nothing left.** When nothing is left after the protocol, there is no URL. Either way the run is
//!   consumed: a protocol inside it starts no URL of its own.
//!
//! The URL is the run's source without its comments, cut and trimmed as above; its span runs from the
//! protocol to the URL's last character.

use std::sync::LazyLock;

use regex::Regex;

use crate::settings::Settings;

/// A free URL: its span in the text and the URL as written (comments removed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeUrl {
    pub start: usize,
    pub end: usize,
    pub url: String,
}

/// A letter, a digit or `_`: what `\b` separates from other characters.
fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Unicode Zs (space separators).
fn is_space_separator(c: char) -> bool {
    matches!(c, '\u{20}' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}')
}

/// `Parser::EXT_LINK_URL_CLASS`.
pub fn is_url_char(c: char) -> bool {
    !matches!(c, '[' | ']' | '<' | '>' | '"' | '\u{0}'..='\u{20}' | '\u{7f}' | '\u{fffd}') && !is_space_separator(c)
}

/// The entities that end a free URL (`makeFreeExternalLink`).
static CUT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"&(lt|gt|nbsp|#x0*(3[CcEe]|[Aa]0)|#0*(60|62|160));").expect("cut regex"));
/// A character entity that a trailing `;` ends.
static ENTITY_END: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)&([a-z]+|#x[0-9a-f]+|#[0-9]+)$").expect("entity regex"));

/// The length of the protocol `text[at..]` starts with, if any.
fn protocol_at(text: &str, at: usize, settings: &Settings) -> Option<usize> {
    let b = text.as_bytes().get(at..)?;
    settings
        .free_link_protocols()
        .filter(|p| b.len() >= p.len() && b[..p.len()].eq_ignore_ascii_case(p.as_bytes()))
        .map(str::len)
        .max()
}

/// The free URLs whose protocol starts in `from..to`, at or after `*until`, appended to `out` in order.
/// `*until` is where the last run ended, and moves past each run found. `in_template`: the text is a template
/// parameter's value, which `|` and `}}` end.
pub fn scan(
    text: &str,
    settings: &Settings,
    from: usize,
    to: usize,
    in_template: bool,
    until: &mut usize,
    out: &mut Vec<FreeUrl>,
) {
    let b = text.as_bytes();
    let to = to.min(text.len());
    let mut i = from.max(*until);
    while i < to {
        if !b[i].is_ascii_alphabetic() {
            i += 1;
            continue;
        }
        let boundary = text[..i].chars().next_back().is_none_or(|c| !is_word(c));
        let Some(plen) = protocol_at(text, i, settings).filter(|_| boundary) else {
            i += 1;
            continue;
        };
        match run(text, i, plen, in_template) {
            Some((end, url)) => {
                *until = end;
                if let Some(u) = finish(text, i, plen, &url) {
                    out.push(u);
                }
                i = end.max(i + 1);
            }
            None => i += 1,
        }
    }
}

/// The run of URL characters after the protocol at `start`: where it ends in the source, and its pieces
/// (source spans without comments). `None` without an address character.
fn run(text: &str, start: usize, plen: usize, in_template: bool) -> Option<(usize, Vec<(usize, usize)>)> {
    let b = text.as_bytes();
    let mut pos = start + plen;
    if b.get(pos) == Some(&b'[') {
        let k = b[pos + 1..].iter().position(|&c| !(c.is_ascii_hexdigit() || c == b':' || c == b'.'))?;
        if k == 0 || b.get(pos + 1 + k) != Some(&b']') {
            return None;
        }
        pos += k + 2;
    }
    let mut pieces = Vec::new();
    let mut piece = start;
    loop {
        let rest = &text[pos..];
        if rest.starts_with("<!--") {
            pieces.push((piece, pos));
            match rest[4..].find("-->") {
                Some(k) => {
                    pos += 4 + k + 3;
                    piece = pos;
                    continue;
                }
                None => {
                    piece = pos;
                    break;
                }
            }
        }
        let ends_value = in_template && (rest.starts_with('|') || rest.starts_with("}}"));
        if rest.starts_with("''") || rest.starts_with("{{") || ends_value {
            break;
        }
        match rest.chars().next() {
            Some(c) if is_url_char(c) => pos += c.len_utf8(),
            _ => break,
        }
    }
    pieces.push((piece, pos));
    let len: usize = pieces.iter().map(|(a, b)| b - a).sum();
    (len > plen).then_some((pos, pieces))
}

/// The URL of a run, cut and trimmed, with its span; `None` when nothing is left after the protocol.
fn finish(text: &str, start: usize, plen: usize, pieces: &[(usize, usize)]) -> Option<FreeUrl> {
    let mut url: String = pieces.iter().map(|&(a, b)| &text[a..b]).collect();
    if let Some(m) = CUT.find(&url) {
        url.truncate(m.start());
    }
    let mut sep = ",;.:!?".to_string();
    if !url.contains('(') {
        sep.push(')');
    }
    let trail = url.chars().rev().take_while(|c| sep.contains(*c)).count();
    let mut keep = url.len() - trail; // the separators are ASCII
    if trail > 0 && url.as_bytes()[keep] == b';' && ENTITY_END.is_match(&url[..keep]) {
        keep += 1;
    }
    url.truncate(keep);
    if url.len() <= plen {
        return None;
    }
    // The URL's length, mapped back through the pieces to a source byte.
    let (mut left, mut end) = (url.len(), start);
    for &(a, b) in pieces {
        if left <= b - a {
            end = a + left;
            break;
        }
        left -= b - a;
    }
    Some(FreeUrl { start, end, url })
}
