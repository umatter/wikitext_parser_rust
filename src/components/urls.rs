//! URL analysis: entity decoding, archive unwrapping, host and registrable domain.

use regex::Regex;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlInfo {
    pub url: String,
    pub archive_url: Option<String>,
    pub host: Option<String>,
    pub registrable_domain: Option<String>,
}

fn decode_entities(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let decoded = rest.find(';').filter(|&e| e <= 10).and_then(|e| {
            let name = &rest[1..e];
            let c = match name {
                "amp" => Some('&'),
                "lt" => Some('<'),
                "gt" => Some('>'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                "nbsp" => Some(' '),
                _ => name.strip_prefix('#').and_then(|n| {
                    match n.strip_prefix(['x', 'X']) {
                        Some(h) => u32::from_str_radix(h, 16).ok(),
                        None => n.parse().ok(),
                    }
                    .and_then(char::from_u32)
                }),
            };
            c.map(|c| (c, e + 1))
        });
        match decoded {
            Some((c, n)) => {
                out.push(c);
                rest = &rest[n..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

fn has_scheme(s: &str) -> bool {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^[A-Za-z][A-Za-z0-9+.\-]*://").unwrap()).is_match(s)
}

/// Give an archived original a scheme: `http:/x` becomes `http://x`, a bare
/// `x.ru/a` gets `http://`.
fn with_scheme(orig: &str) -> Option<String> {
    let orig = orig.trim();
    if orig.is_empty() {
        return None;
    }
    if has_scheme(orig) {
        return Some(orig.to_string());
    }
    for p in ["https:/", "http:/"] {
        if orig.len() > p.len() && orig[..p.len()].eq_ignore_ascii_case(p) && !orig[p.len()..].starts_with('/') {
            return Some(format!("{}/{}", &orig[..p.len()], &orig[p.len()..]));
        }
    }
    Some(format!("http://{orig}"))
}

pub fn original_from_archive(url: &str) -> Option<String> {
    static WAY: OnceLock<Regex> = OnceLock::new();
    static TODAY: OnceLock<Regex> = OnceLock::new();
    static WEBCIT: OnceLock<Regex> = OnceLock::new();
    let way = WAY.get_or_init(|| {
        Regex::new(r"(?i)^https?://(?:www\.)?(?:web\.)?archive\.org/web/\d+[a-z_]*/(.+)$").unwrap()
    });
    let today = TODAY.get_or_init(|| {
        Regex::new(r"(?i)^https?://(?:www\.)?archive\.(?:today|ph|is|li)/\d[0-9A-Za-z.\-]*/(.+)$").unwrap()
    });
    let webcit = WEBCIT.get_or_init(|| {
        Regex::new(r"(?i)^https?://(?:www\.)?webcitation\.org/[^?#]*\?([^#]*)").unwrap()
    });
    if let Some(c) = way.captures(url).or_else(|| today.captures(url)) {
        return with_scheme(&c[1]);
    }
    let c = webcit.captures(url)?;
    let (_, v) = url::form_urlencoded::parse(c[1].as_bytes()).find(|(k, _)| k.eq_ignore_ascii_case("url"))?;
    with_scheme(&v)
}

pub fn wayback_url(orig: &str, date: &str) -> String {
    format!("https://web.archive.org/web/{date}/{orig}")
}

/// Host (Unicode, lowercase, no port, no leading `www.`) of a URL, if it has one.
fn host_of(url: &str) -> Option<String> {
    let u = url::Url::parse(url).ok()?;
    let h = match u.host()? {
        url::Host::Domain(d) => idna::domain_to_unicode(d).0,
        url::Host::Ipv4(a) => a.to_string(),
        url::Host::Ipv6(a) => a.to_string(),
    };
    let h = h.to_lowercase();
    let h = h.strip_prefix("www.").unwrap_or(&h).trim_end_matches('.');
    (!h.is_empty()).then(|| h.to_string())
}

fn registrable(host: &str) -> Option<String> {
    let ascii = idna::domain_to_ascii(host).ok()?;
    let reg = psl::domain_str(&ascii)?;
    Some(idna::domain_to_unicode(reg).0)
}

pub fn analyse(raw: &str) -> UrlInfo {
    let mut url = decode_entities(raw).trim().to_string();
    if url.starts_with("//") {
        url = format!("https:{url}");
    }
    let mut archive_url = None;
    if let Some(orig) = original_from_archive(&url) {
        archive_url = Some(std::mem::replace(&mut url, orig));
    }
    let host = host_of(&url);
    let registrable_domain = host.as_deref().and_then(registrable);
    UrlInfo { url, archive_url, host, registrable_domain }
}
