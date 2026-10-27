//! Tag attributes, lexed the way MediaWiki's Sanitizer reads them, so that
//! `<ref name=… group=…>` matches across reused named refs.
//!
//! Every delimiter is ASCII, so byte indexing never splits a multi-byte char.

use crate::urls::decode_entities;

fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

/// The slice from `<` to the first `>` outside quotes. With no such `>` the
/// whole input; input not starting with `<` comes back unchanged.
pub fn open_tag(source: &str) -> &str {
    if !source.starts_with('<') {
        return source;
    }
    let mut quote: Option<u8> = None;
    for (i, &b) in source.as_bytes().iter().enumerate() {
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => match b {
                b'"' | b'\'' => quote = Some(b),
                b'>' => return &source[..=i],
                _ => {}
            },
        }
    }
    source
}

fn clean(raw: &str) -> String {
    decode_entities(raw).split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Attributes in order, duplicates kept. Names lowercased; values unquoted,
/// entity-decoded, whitespace-collapsed and trimmed. A bare attribute has `""`.
pub fn tag_attrs(open_tag: &str) -> Vec<(String, String)> {
    let s = open_tag.as_bytes();
    let n = s.len();
    let ends = |i: usize| i >= n || s[i] == b'>' || (s[i] == b'/' && (i + 1 >= n || s[i + 1] == b'>'));
    let mut out = Vec::new();
    let mut i = usize::from(s.first() == Some(&b'<'));
    // The tag name is not an attribute.
    while i < n && !is_ws(s[i]) && !ends(i) {
        i += 1;
    }
    loop {
        while i < n && is_ws(s[i]) {
            i += 1;
        }
        if ends(i) {
            break;
        }
        if s[i] == b'/' {
            i += 1; // stray slash inside the tag
            continue;
        }
        let start = i;
        if s[i] == b'=' {
            i += 1; // a value with no name: skip the '='
        }
        while i < n && !is_ws(s[i]) && s[i] != b'=' && !ends(i) {
            i += 1;
        }
        let name = open_tag[start..i].trim_start_matches('=').to_lowercase();
        while i < n && is_ws(s[i]) {
            i += 1;
        }
        let mut value = "";
        if i < n && s[i] == b'=' {
            i += 1;
            while i < n && is_ws(s[i]) {
                i += 1;
            }
            if i < n && (s[i] == b'"' || s[i] == b'\'') {
                let q = s[i];
                let vs = i + 1;
                let mut j = vs;
                while j < n && s[j] != q {
                    j += 1;
                }
                value = &open_tag[vs..j];
                i = if j < n { j + 1 } else { n };
            } else {
                let vs = i;
                while i < n && !is_ws(s[i]) && !ends(i) {
                    i += 1;
                }
                value = &open_tag[vs..i];
            }
        }
        if !name.is_empty() {
            out.push((name, clean(value)));
        }
    }
    out
}

/// True when the last non-whitespace char before the closing `>` is a `/`
/// outside quotes.
pub fn self_closing(open_tag: &str) -> bool {
    let mut quote: Option<u8> = None;
    let mut last = 0u8;
    for &b in open_tag.as_bytes() {
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
                last = q;
            }
            None => match b {
                b'>' => break,
                b'"' | b'\'' => {
                    quote = Some(b);
                    last = b;
                }
                _ if is_ws(b) => {}
                _ => last = b,
            },
        }
    }
    last == b'/'
}
