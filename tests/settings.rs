mod common;

use common::ru;
use std::collections::BTreeSet;
use std::path::Path;
use wikitext_components::frontend;
use wikitext_components::settings::{self, Settings};

fn has_node(parsed: &frontend::Parsed, name: &str) -> bool {
    // Node derives Debug, and the Debug form lists every nested node, so a text search is a recursive walk.
    format!("{:?}", parsed.output.nodes).contains(&format!("{name} {{"))
}

#[test]
fn russian_namespaces() {
    let s = ru();
    assert_eq!(s.namespace("Файл"), Some(6));
    assert_eq!(s.namespace("изображение"), Some(6));
    assert_eq!(s.namespace("К"), Some(14));
    assert_eq!(s.namespace("Ш"), Some(10));
    assert_eq!(s.namespace("Модуль"), Some(828));
    assert_eq!(s.namespace("Москва"), None);
    // Further namespaces, aliases, underscores and a few that matter for classifying links.
    assert_eq!(s.namespace("Служебная"), Some(-1));
    assert_eq!(s.namespace("Обсуждение"), Some(1));
    assert_eq!(s.namespace("Участница"), Some(2));
    assert_eq!(s.namespace("ВП"), Some(4));
    assert_eq!(s.namespace("Обсуждение_шаблона"), Some(11));
    assert_eq!(s.namespace("Портал"), Some(100));
    assert_eq!(s.namespace("Арбитраж"), Some(106));
    assert_eq!(s.namespace("Инкубатор"), Some(102));
    assert_eq!(s.namespace("Гаджет"), Some(2300));
}

#[test]
fn file_and_category_nodes() {
    let s = ru();
    let p = frontend::parse("[[Файл:A.jpg|мини|Подпись]] [[К:Столицы]]", s.configuration(), 0).unwrap();
    assert!(has_node(&p, "Image"));
    assert!(has_node(&p, "Category"));
}

#[test]
fn cyrillic_link_trail() {
    let s = ru();
    let text = "[[Москва|Москв]]е";
    let p = frontend::parse(text, s.configuration(), 0).unwrap();
    let ends: Vec<usize> = p
        .output
        .nodes
        .iter()
        .filter_map(|n| match n {
            parse_wiki_text_2::Node::Link { end, .. } => Some(*end),
            _ => None,
        })
        .collect();
    assert_eq!(ends, vec![text.len()]);
}

#[test]
fn russian_redirect() {
    let s = ru();
    for w in ["#перенаправление [[Москва]]", "#перенапр [[Москва]]", "#REDIRECT [[Москва]]"] {
        let p = frontend::parse(w, s.configuration(), 0).unwrap();
        assert!(has_node(&p, "Redirect"), "{w}");
    }
    assert!(s.redirect_words().iter().any(|w| w == "перенаправление"));
}

#[test]
fn every_redirect_word_and_magic_word_is_recognised() {
    let text = std::fs::read_to_string("config/sites/ru.wikipedia.toml").unwrap();
    let table: toml::Table = text.parse().unwrap();
    let words = |key: &str| -> Vec<String> {
        table[key].as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_string()).collect()
    };
    let s = ru();
    for w in words("redirect_magic_words") {
        let src = format!("#{w} [[Москва]]");
        let p = frontend::parse(&src, s.configuration(), 0).unwrap();
        assert!(matches!(p.output.nodes.first(), Some(parse_wiki_text_2::Node::Redirect { .. })), "{src}");
    }
    for w in words("magic_words") {
        let src = format!("__{w}__");
        let p = frontend::parse(&src, s.configuration(), 0).unwrap();
        assert!(matches!(p.output.nodes.first(), Some(parse_wiki_text_2::Node::MagicWord { .. })), "{src}");
    }
}

#[test]
fn prefix_aliases_are_not_dropped() {
    let s = ru();
    for src in ["#перенапр [[Москва]]", "__ОГЛ__", "__БЕЗ_ОГЛ__"] {
        let p = frontend::parse(src, s.configuration(), 0).unwrap();
        let first = &p.output.nodes[0];
        assert!(
            matches!(first, parse_wiki_text_2::Node::Redirect { .. } | parse_wiki_text_2::Node::MagicWord { .. }),
            "{src}: {first:?}"
        );
    }
}

#[test]
fn template_name_normalisation() {
    let s = ru();
    for raw in ["Cite_web", " cite web ", "Шаблон:Cite web", "Ш:cite  web", "Template:Cite web"] {
        assert_eq!(s.template_name(raw), "cite web", "{raw:?}");
    }
    // A prefix that is not a template namespace stays part of the name.
    assert_eq!(s.template_name("Участник:Вася/шаблон"), "участник:Вася/шаблон");
}

#[test]
fn image_options() {
    let s = ru();
    for o in ["мини", "thumb", "справа", "200px", "200пкс", "x120px", "upright=1.2", "альт=Текст", "ссылка=", "Мини"] {
        assert!(s.is_image_option(o), "{o}");
    }
    assert!(!s.is_image_option("Вид на город"));
    assert!(!s.is_image_option("px"));
}

#[test]
fn language_prefixes() {
    let s = ru();
    assert!(s.is_language_prefix("en"));
    assert!(s.is_interwiki_prefix("wikt"));
    assert!(!s.is_language_prefix("Москва"));
    assert!(!s.is_interwiki_prefix("Москва"));
}

#[test]
fn w_is_the_wikipedia_interwiki_prefix_not_a_language() {
    for s in [ru(), Settings::load(Path::new("config/sites/ruwiki.toml")).unwrap()] {
        assert!(s.is_interwiki_prefix("w"), "{}", s.site());
        assert!(!s.is_language_prefix("w"), "{}", s.site());
    }
}

#[test]
fn variables_and_parser_functions() {
    let s = ru();
    assert!(s.is_variable("PAGENAME"));
    assert!(s.is_variable("НАЗВАНИЕ_СТРАНИЦЫ"));
    assert!(s.is_variable("название страницы"));
    assert!(!s.is_variable("cite web"));
    for f in ["lc", "uc", "formatnum", "padleft", "#if", "#invoke", "МАЛЕНЬКИМИ_БУКВАМИ"] {
        assert!(s.is_parser_function(f), "{f}");
    }
    assert!(!s.is_parser_function("cite web"));
}

#[test]
fn site_and_hash() {
    let s = ru();
    assert_eq!(s.site(), "wikipedia");
    assert_eq!(s.hash().len(), 64);
    assert_eq!(s.hash(), ru().hash());
    let r = Settings::load(Path::new("config/sites/ruwiki.toml")).unwrap();
    assert_eq!(r.site(), "ruwiki");
    assert_ne!(r.hash(), s.hash());
}

#[test]
fn scan_finds_unknown_prefixes_and_tags() {
    let s = ru();
    let text = "[[Файл:A.jpg]] [[:en:Moscow]] [[Wikt:x]] [[Zzzz:Foo]] [[12:30]] <ref>a</ref> <br/> <Qqq>x</Qqq> [[Звёздные войны: Эпизод I]]";
    let got = settings::unknown_prefixes_and_tags(&s, std::iter::once(text));
    let want: BTreeSet<String> = ["prefix:Zzzz", "tag:qqq"].iter().map(|x| x.to_string()).collect();
    assert_eq!(got, want);
}

/// Candidates that are really article titles, not prefixes (R8). Each also occurs in the public Wikipedia corpus.
const ALLOW_PUBLIC: &[&str] = &[];

fn allowed() -> BTreeSet<String> {
    let mut set: BTreeSet<String> = ALLOW_PUBLIC.iter().map(|x| x.to_string()).collect();
    // Titles that occur only in Ruwiki stay out of git.
    if let Ok(text) = std::fs::read_to_string("tests/private/prefix_allow.txt") {
        set.extend(text.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')).map(String::from));
    }
    set
}

#[test]
fn wikipedia_markup_uses_no_unknown_prefix_or_tag() {
    let s = ru();
    let rows: Vec<_> = common::public_corpus();
    let allow = allowed();
    let unknown: BTreeSet<String> =
        settings::unknown_prefixes_and_tags(&s, rows.iter().map(|r| r.wikitext.as_str()))
            .into_iter()
            .filter(|u| !allow.contains(u))
            .collect();
    assert!(unknown.is_empty(), "add to ru.wikipedia.toml or ALLOW_PUBLIC: {unknown:?}");
}

#[test]
fn ruwiki_markup_uses_no_unknown_prefix_or_tag() {
    let Some(rows) = common::private_corpus("ruwiki_markup_uses_no_unknown_prefix_or_tag") else { return };
    let s = Settings::load("config/sites/ruwiki.toml".as_ref()).unwrap();
    let allow = allowed();
    let unknown: BTreeSet<String> =
        settings::unknown_prefixes_and_tags(&s, rows.iter().map(|r| r.wikitext.as_str()))
            .into_iter()
            .filter(|u| !allow.contains(u))
            .collect();
    assert!(unknown.is_empty(), "add to ruwiki.toml: {unknown:?}");
}
