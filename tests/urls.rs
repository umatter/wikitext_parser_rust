use wikitext_components::fingerprint::{fp, norm};
use wikitext_components::spans::CharIndex;
use wikitext_components::urls::{analyse, wayback_url};

#[test]
fn hosts() {
    assert_eq!(analyse("https://Президент.рф/a").host.as_deref(), Some("президент.рф"));
    assert_eq!(analyse("//tass.ru/x").host.as_deref(), Some("tass.ru"));
    assert_eq!(analyse("http://WWW.Kremlin.RU:80/a").host.as_deref(), Some("kremlin.ru"));
    assert_eq!(analyse("https://news.bbc.co.uk/x").registrable_domain.as_deref(), Some("bbc.co.uk"));
    assert_eq!(analyse("https://a.ru/?x=1&amp;y=2").url, "https://a.ru/?x=1&y=2");
    assert_eq!(analyse("https://www.президент.рф/a").registrable_domain.as_deref(), Some("президент.рф"));
    assert_eq!(analyse("https://a.ru/?q=&#38;z&#x26;w").url, "https://a.ru/?q=&z&w");
}

#[test]
fn archive_unwrapping() {
    let u = analyse("https://web.archive.org/web/20200101000000/https://tass.ru/politika/1");
    assert_eq!(u.url, "https://tass.ru/politika/1");
    assert_eq!(u.archive_url.as_deref(), Some("https://web.archive.org/web/20200101000000/https://tass.ru/politika/1"));
    assert_eq!(u.host.as_deref(), Some("tass.ru"));
    assert_eq!(analyse("https://archive.ph/2021.01.01-000000/http://gov.ru/a").url, "http://gov.ru/a");
    assert_eq!(analyse("https://web.archive.org/web/2019id_/www.bbc.co.uk/news/1").url, "http://www.bbc.co.uk/news/1");
    assert_eq!(analyse("https://web.archive.org/web/2019/http:/x.ru/a").url, "http://x.ru/a");
    assert_eq!(analyse("https://archive.org/web/2019/https://x.ru/a").url, "https://x.ru/a");
    assert_eq!(analyse("http://www.webcitation.org/query?url=http%3A%2F%2Fx.ru%2Fa&date=1").url, "http://x.ru/a");
    assert_eq!(analyse("http://www.webcitation.org/query?x=1&url=http://x.ru/a").url, "http://x.ru/a");
    assert_eq!(wayback_url("http://x.ru/a", "2020"), "https://web.archive.org/web/2020/http://x.ru/a");
}

#[test]
fn never_panics() {
    for s in ["", "   ", "garbage text", "http://", "https:///", "tass.ru/x", "http://a b.ru/x", "//", "&amp;", "&#99999999999;", "\u{0}", "http://[::1", "https://web.archive.org/web/2019/"] {
        let u = analyse(s);
        assert!(u.registrable_domain.is_none() || u.host.is_some(), "{s:?}");
    }
    assert_eq!(analyse("").host, None);
    assert_eq!(analyse("garbage text").host, None);
    assert_eq!(analyse("http://").host, None);
    assert_eq!(analyse("tass.ru/x").host, None);
    assert_eq!(analyse("http://a b.ru/x").host, None);
    assert_eq!(analyse("garbage text").url, "garbage text");
}

#[test]
fn fingerprint_shape() {
    let f = fp(&norm("  Привет   МИР "));
    assert_eq!(f.len(), 16);
    assert_eq!(f, fp("привет мир"));
    assert_eq!(norm("a\u{a0}\u{2003}B\n"), "a b");
}

#[test]
fn char_index() {
    let t = "аб c";
    let ci = CharIndex::new(t);
    assert_eq!(ci.char_of(0), 0);
    assert_eq!(ci.char_of(4), 2);
    assert_eq!(ci.char_of(t.len()), 4);
    assert_eq!(ci.char_of(1), 0); // inside a multi-byte char
    assert_eq!(ci.char_of(3), 1);
    assert_eq!(ci.char_of(999), 4);
    assert_eq!(CharIndex::new("").char_of(5), 0);
    let big = "я".repeat(300_000) + "x";
    let ci = CharIndex::new(&big);
    assert_eq!(ci.char_of(big.len() - 1), 300_000);
    assert_eq!(ci.char_of(big.len()), 300_001);
}
