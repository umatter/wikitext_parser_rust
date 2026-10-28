//! Site settings: what the parser must know about a wiki to read its markup (namespaces, link trail,
//! tags, magic words, image options). One static file per site under `config/sites/`.

use anyhow::{anyhow, Context, Result};
use parse_wiki_text_2::{Configuration, ConfigurationSource};
use regex::Regex;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::OnceLock;

#[derive(Deserialize)]
struct Raw {
    site: String,
    #[allow(dead_code)]
    source: String,
    link_trail: String,
    namespaces: BTreeMap<String, Vec<String>>,
    extension_tags: Vec<String>,
    html_tags: Vec<String>,
    magic_words: Vec<String>,
    redirect_magic_words: Vec<String>,
    protocols: Vec<String>,
    image_options: Vec<String>,
    image_option_patterns: Vec<String>,
    variables: Vec<String>,
    parser_functions: Vec<String>,
    language_prefixes: Vec<String>,
    interwiki_prefixes: Vec<String>,
}

/// How the parser reads a tag name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagClass {
    /// An extension tag (`ref`, `nowiki`, `gallery`): an element with content, or self-closing.
    Extension,
    /// An HTML tag (`span`, `br`, `pre`): a start or end tag on its own.
    Html,
}

pub struct Settings {
    site: String,
    hash: String,
    configuration: Configuration,
    /// Normalised namespace name or alias -> namespace id.
    namespaces: HashMap<String, i32>,
    extension_tags: HashSet<String>,
    html_tags: HashSet<String>,
    redirect_words: Vec<String>,
    image_options: HashSet<String>,
    /// (prefix, suffix, is_width) of each `$1` pattern, normalised.
    image_patterns: Vec<(String, String, bool)>,
    variables: HashSet<String>,
    parser_functions: HashSet<String>,
    language_prefixes: HashSet<String>,
    interwiki_prefixes: HashSet<String>,
    /// URL protocols that open an external link (`[https://…]`), lowercased.
    protocols: Vec<String>,
    /// The tag names the parser recognises, by class.
    tag_classes: HashMap<String, TagClass>,
}

/// Lowercase, `_` as a space, whitespace collapsed and trimmed.
fn norm(s: &str) -> String {
    s.replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn set(items: &[String]) -> HashSet<String> {
    items.iter().map(|x| norm(x)).collect()
}

impl Settings {
    pub fn load(path: &Path) -> Result<Settings> {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let text = std::str::from_utf8(&bytes).with_context(|| format!("{} is not UTF-8", path.display()))?;
        let raw: Raw = toml::from_str(text).with_context(|| format!("parsing {}", path.display()))?;
        let hash: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();

        let mut namespaces = HashMap::new();
        for (id, names) in &raw.namespaces {
            let id: i32 = id.parse().map_err(|_| anyhow!("namespace id {id:?} is not an integer"))?;
            for name in names {
                namespaces.insert(norm(name), id);
            }
        }
        let names_of = |id: &str| -> Vec<&str> {
            raw.namespaces.get(id).map(|v| v.iter().map(String::as_str).collect()).unwrap_or_default()
        };
        let files = names_of("6");
        let categories = names_of("14");
        let ext: Vec<&str> = raw.extension_tags.iter().map(String::as_str).collect();
        let magic: Vec<&str> = raw.magic_words.iter().map(String::as_str).collect();
        let redirect: Vec<&str> = raw.redirect_magic_words.iter().map(String::as_str).collect();
        let protocols: Vec<&str> = raw.protocols.iter().map(String::as_str).collect();
        let configuration = Configuration::new(&ConfigurationSource {
            category_namespaces: &categories,
            extension_tags: &ext,
            file_namespaces: &files,
            link_trail: &raw.link_trail,
            magic_words: &magic,
            protocols: &protocols,
            redirect_magic_words: &redirect,
        });

        // The parser's own reading of each tag name the site lists: it keeps a fixed set of HTML tags of its
        // own, which wins over an extension tag of the same name (`pre`), and ignores other HTML tags.
        let tag_classes = raw
            .extension_tags
            .iter()
            .chain(&raw.html_tags)
            .filter_map(|name| {
                let name = name.to_lowercase();
                let probe = format!("<{name}/>");
                let class = match crate::frontend::parse(&probe, &configuration, 0).ok()?.output.nodes.first()? {
                    parse_wiki_text_2::Node::Tag { .. } => TagClass::Extension,
                    parse_wiki_text_2::Node::StartTag { .. } => TagClass::Html,
                    _ => return None,
                };
                Some((name, class))
            })
            .collect();

        let image_patterns = raw
            .image_option_patterns
            .iter()
            .map(|p| {
                let (pre, post) = p
                    .split_once("$1")
                    .ok_or_else(|| anyhow!("image option pattern {p:?} has no $1"))?;
                // `$1px` and `$1пкс` take a size; every other pattern takes any value.
                Ok((norm_keep_space(pre), norm_keep_space(post), pre.is_empty()))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Settings {
            site: raw.site,
            hash,
            configuration,
            namespaces,
            extension_tags: set(&raw.extension_tags),
            html_tags: set(&raw.html_tags),
            redirect_words: raw.redirect_magic_words,
            image_options: set(&raw.image_options),
            image_patterns,
            variables: set(&raw.variables),
            parser_functions: set(&raw.parser_functions),
            language_prefixes: set(&raw.language_prefixes),
            interwiki_prefixes: set(&raw.interwiki_prefixes),
            protocols: raw.protocols.iter().map(|p| p.to_lowercase()).collect(),
            tag_classes,
        })
    }

    pub fn site(&self) -> &str {
        &self.site
    }

    /// For `frontend` and `walk` only.
    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    /// Namespace id of a name or alias. Matching ignores case (as MediaWiki does) and reads `_` as a space.
    pub fn namespace(&self, prefix: &str) -> Option<i32> {
        self.namespaces.get(&norm(prefix)).copied()
    }

    pub fn is_language_prefix(&self, p: &str) -> bool {
        self.language_prefixes.contains(&norm(p))
    }

    pub fn is_interwiki_prefix(&self, p: &str) -> bool {
        self.interwiki_prefixes.contains(&norm(p))
    }

    /// True for the options that separate image options from the caption: `мини`, `200px`, `альт=...`.
    pub fn is_image_option(&self, s: &str) -> bool {
        let n = norm(s);
        if self.image_options.contains(&n) {
            return true;
        }
        self.image_patterns.iter().any(|(pre, post, width)| {
            if !n.starts_with(pre.as_str()) || !n.ends_with(post.as_str()) || n.len() < pre.len() + post.len() {
                return false;
            }
            let value = &n[pre.len()..n.len() - post.len()];
            if *width {
                width_re().is_match(value)
            } else {
                true
            }
        })
    }

    /// Template name as the registry keys it: namespace 10 prefix stripped, `_` as a space, whitespace
    /// collapsed and trimmed, first character lowercased.
    pub fn template_name(&self, raw: &str) -> String {
        normalize_template_name(raw, |prefix| self.namespace(prefix) == Some(10))
    }

    pub fn is_variable(&self, name: &str) -> bool {
        self.variables.contains(&norm(name))
    }

    /// The word before the colon of `{{lc:x}}` or `{{#if:x}}`.
    pub fn is_parser_function(&self, word: &str) -> bool {
        self.parser_functions.contains(&norm(word))
    }

    /// Whether `s` starts with a URL protocol of the site (`https://`, `//`, `mailto:`), in any case: what
    /// the parser needs after `[` to open an external link.
    pub fn starts_with_protocol(&self, s: &str) -> bool {
        let b = s.as_bytes();
        self.protocols.iter().any(|p| b.len() >= p.len() && b[..p.len()].eq_ignore_ascii_case(p.as_bytes()))
    }

    /// The protocols a free URL can start with (`autolink`): the site's URL protocols without the
    /// protocol-relative `//`, as MediaWiki's `UrlUtils::validAbsoluteProtocols` gives them; lowercased.
    pub fn free_link_protocols(&self) -> impl Iterator<Item = &str> {
        self.protocols.iter().map(String::as_str).filter(|p| !p.starts_with("//"))
    }

    /// How the parser reads a tag of this lowercased name; `None` when it reads it as text.
    pub fn tag_class(&self, name: &str) -> Option<TagClass> {
        self.tag_classes.get(name).copied()
    }

    /// Redirect words without the leading `#`.
    pub fn redirect_words(&self) -> &[String] {
        &self.redirect_words
    }

    /// sha256 of the settings file, as lowercase hex.
    pub fn hash(&self) -> String {
        self.hash.clone()
    }

    fn knows_prefix(&self, p: &str) -> bool {
        self.namespace(p).is_some() || self.is_language_prefix(p) || self.is_interwiki_prefix(p)
    }

    fn knows_tag(&self, t: &str) -> bool {
        self.extension_tags.contains(t) || self.html_tags.contains(t)
    }
}

/// The rule behind `Settings::template_name`, shared with the registry: `_` as a space, trimmed, a prefix
/// accepted by `is_template_ns` stripped, whitespace collapsed, first character lowercased.
pub fn normalize_template_name(raw: &str, is_template_ns: impl Fn(&str) -> bool) -> String {
    let spaced = raw.replace('_', " ");
    let mut name = spaced.trim();
    if let Some((prefix, rest)) = name.split_once(':') {
        if is_template_ns(prefix) {
            name = rest;
        }
    }
    let collapsed = name.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = collapsed.chars();
    match chars.next() {
        Some(c) => c.to_lowercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// Like `norm` but keeps a leading or trailing space, which separates `страница $1` from `страница=$1`.
fn norm_keep_space(s: &str) -> String {
    s.to_lowercase()
}

fn width_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^(\d+|\d*x\d+)$").expect("width regex"))
}

/// Finds `[[X:` prefixes that are neither a namespace nor a language or interwiki prefix, and `<tag` names
/// that are neither extension tags nor HTML tags. Items read `prefix:X` and `tag:x`.
///
/// A candidate prefix is what follows `[[` and an optional `:`: 1 to 30 characters without whitespace or
/// any of `|[]{}#<>:`, then `:` and a non-space character. All-digit candidates are ignored. The scan cannot
/// tell a prefix from an article title that contains a colon, so callers keep an allow list of the titles.
pub fn unknown_prefixes_and_tags<'a>(s: &Settings, texts: impl Iterator<Item = &'a str>) -> BTreeSet<String> {
    static RE: OnceLock<(Regex, Regex)> = OnceLock::new();
    let (prefix_re, tag_re) = RE.get_or_init(|| {
        (
            Regex::new(r"\[\[:?([^\s|\[\]{}#<>:]{1,30}):\S").expect("prefix regex"),
            Regex::new(r"</?([A-Za-z][A-Za-z0-9-]*)").expect("tag regex"),
        )
    });
    let mut unknown = BTreeSet::new();
    for text in texts {
        for c in prefix_re.captures_iter(text) {
            let p = &c[1];
            if p.chars().all(|ch| ch.is_ascii_digit()) || s.knows_prefix(p) {
                continue;
            }
            unknown.insert(format!("prefix:{p}"));
        }
        for c in tag_re.captures_iter(text) {
            let t = c[1].to_lowercase();
            if !s.knows_tag(&t) {
                unknown.insert(format!("tag:{t}"));
            }
        }
    }
    unknown
}
