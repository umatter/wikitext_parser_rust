mod common;

use common::{defaults, run_page, run_with};
use wikitext_components::fingerprint::{fp, norm};
use wikitext_components::frontend;
use wikitext_components::model::{Components, ParagraphRow};
use wikitext_components::page::{Limits, DEFAULT_DEPTH_CAP, DEFAULT_PARSE_BUDGET, DEFAULT_WALK_BUDGET};
use wikitext_components::spans::CharIndex;
use wikitext_components::walk;

fn components(text: &str) -> Components {
    let r = run_page(text, "wikipedia");
    assert_eq!(r.page.status, "ok", "{:?}", r.page.status_detail);
    r.components
}

fn texts(text: &str) -> Vec<String> {
    components(text).paragraphs.into_iter().map(|p| p.text).collect()
}

fn kinds(ps: &[ParagraphRow]) -> Vec<(&str, Option<i32>, Option<i32>, &str)> {
    ps.iter().map(|p| (p.kind.as_str(), p.list_id, p.item_depth, p.text.as_str())).collect()
}

const FIXTURES: [&str; 10] = [
    "archive_deadlink",
    "efn",
    "malformed",
    "refs_basic",
    "refs_groups_listdefined",
    "refs_in_template",
    "refs_reuse_order",
    "sections_paragraphs",
    "sfn",
    "templates",
];

#[test]
fn sections_and_paths() {
    let c = run_page(&common::fixture("sections_paragraphs"), "wikipedia").components;
    let s: Vec<_> =
        c.sections.iter().map(|s| (s.id, s.level, s.title_text.as_str(), s.parent_section_id)).collect();
    assert_eq!(s, vec![(0, 0, "", 0), (1, 2, "История", 0), (2, 3, "Ранние годы", 1), (3, 2, "Примечания", 0)]);
    assert_eq!(c.sections[2].path, vec!["история", "ранние годы"]);
    assert!(c.sections.iter().all(|s| s.role == "content")); // roles arrive with the registry (Task 14)
}

#[test]
fn paragraph_text_link_trail_files_excluded() {
    let c = run_page(&common::fixture("sections_paragraphs"), "wikipedia").components;
    let p: Vec<_> = c.paragraphs.iter().map(|p| (p.section_id, p.kind.as_str(), p.text.as_str())).collect();
    assert_eq!(
        p,
        vec![
            (0, "prose", "Москва — столица России. Город в Москве."),
            (1, "prose", "Текст истории."),
            (2, "list_item", "Пункт один"),
            (2, "list_item", "Пункт два")
        ]
    );
}

#[test]
fn section_spans_and_fields() {
    let t = common::fixture("sections_paragraphs");
    let c = components(&t);
    let at = |s: &str| t.find(s).unwrap() as i32;
    let (h1, h2, h3) = (at("== История"), at("=== Ранние"), at("== Примечания"));
    let n = t.len() as i32;
    let v: Vec<_> = c.sections.iter().map(|s| (s.start, s.end, s.subtree_end, s.heading_start)).collect();
    assert_eq!(v, vec![(0, h1, n, None), (h1, h2, h3, Some(h1)), (h2, h3, h3, Some(h2)), (h3, n, n, Some(h3))]);
    let s = &c.sections[2];
    assert_eq!(&t[s.heading_start.unwrap() as usize..s.heading_end.unwrap() as usize], "=== Ранние годы ===");
    assert_eq!((s.title_raw.as_str(), s.title_norm.as_str()), ("Ранние годы", "ранние годы"));
    assert_eq!(s.fp, fp("история\u{1f}ранние годы"));
    assert_eq!(c.sections[0].fp, fp(""));
    assert!(c.sections[0].path.is_empty() && c.sections[0].heading_end.is_none());
    for (i, s) in c.sections.iter().enumerate() {
        assert_eq!((s.id, s.section_id, s.para_id, s.para_offset), (i as i32, i as i32, None, None));
        assert_eq!((s.parent_type.as_deref(), s.parent_id, s.fallback), (None, None, false));
    }
}

#[test]
fn heading_title_text_follows_the_text_rules() {
    let c = components("== [[Москва|Город]] {{x}} &amp; <ref>r</ref> ==\nТекст");
    let s = &c.sections[1];
    assert_eq!(s.title_text, "Город &");
    assert_eq!(s.title_raw, "[[Москва|Город]] {{x}} &amp; <ref>r</ref>");
    assert_eq!(s.path, vec!["город &"]);
    // A page that opens with a heading still has its (empty) lead.
    assert_eq!((c.sections[0].start, c.sections[0].end), (0, 0));
}

#[test]
fn nested_levels_and_subtree() {
    let t = "Лид\n== A ==\n==== B ====\n=== C ===\n= D =\nтекст";
    let c = components(t);
    let v: Vec<_> = c.sections.iter().map(|s| (s.level, s.parent_section_id, s.path.join("/"))).collect();
    let want = [(0, 0, ""), (2, 0, "a"), (4, 1, "a/b"), (3, 1, "a/c"), (1, 0, "d")];
    assert_eq!(v, want.map(|(l, p, s)| (l, p, s.to_string())));
    let d = t.find("= D").unwrap() as i32;
    assert_eq!(c.sections[1].subtree_end, d);
    assert_eq!(c.sections[2].subtree_end, t.find("=== C").unwrap() as i32);
    assert_eq!(c.sections[3].subtree_end, d);
    assert_eq!(c.sections[4].subtree_end, t.len() as i32);
}

#[test]
fn whitespace_entities_and_markers() {
    let t = "Первый  абзац\nпродолжение &amp; '''ещё''' ''раз''  ";
    assert_eq!(texts(t), vec!["Первый абзац продолжение & ещё раз"]);
    assert_eq!(texts("a\t\u{a0} b"), vec!["a b"]);
}

#[test]
fn links() {
    let t = "[[Википедия:Правила]] и [[:Категория:Столицы]], [[Москва]]х";
    assert_eq!(texts(t), vec!["Правила и Столицы, Москвах"]);
    // The vendored parser repeats a piped link's trail as a sibling text node; it is added once.
    assert_eq!(texts("[[Москва|Москв]]ой и [[Россия|России]]."), vec!["Москвой и России."]);
    // Interlanguage links add nothing; interwiki and leading-colon links read like any link.
    assert_eq!(texts("Город [[en:Moscow]]"), vec!["Город"]);
    assert_eq!(texts("[[w:Москва|Москва]]"), vec!["Москва"]);
    assert_eq!(texts("[[:en:Moscow|Moscow]] и [[wikt:слово]]"), vec!["Moscow и wikt:слово"]);
    assert_eq!(texts("[[Файл:A.jpg|мини|Подпись [[Москва]]]] текст [[Категория:X]]"), vec!["текст"]);
}

#[test]
fn external_links() {
    assert_eq!(texts("См. [http://x.ru Сайт ''Кремля''] и [https://y.ru] тоже."), vec!["См. Сайт Кремля и тоже."]);
}

#[test]
fn excluded_nodes_add_nothing() {
    assert_eq!(texts("a{{x|y}}b<!-- c -->c__TOC__d{{{1|e}}}f"), vec!["abcdf"]);
    assert_eq!(texts("a<ref>r</ref><ref name=x/>b<math>x^2</math>c<references/>d"), vec!["abcd"]);
    assert_eq!(texts("a<gallery>\nФайл:X.jpg|z\n</gallery>b<syntaxhighlight>q</syntaxhighlight>c"), vec!["abc"]);
}

#[test]
fn tags() {
    assert_eq!(texts("a<nowiki>[[x]] {{y}}</nowiki>b"), vec!["a[[x]] {{y}}b"]);
    assert_eq!(texts("<poem>Строка\nвторая</poem>"), vec!["Строка вторая"]);
    assert_eq!(texts("a<br/>b<br>c</br>d"), vec!["a b c d"]);
    assert_eq!(texts("x<sup>2</sup> <span style=\"a\">s</span><small>m</small>"), vec!["x2 sm"]);
    assert_eq!(texts("a<onlyinclude>b</onlyinclude>c"), vec!["abc"]);
}

#[test]
fn paragraphs_break_and_empty_ones_are_dropped() {
    let c = components("{{Карточка}}\n\nПервый.\n\n\n\nВторой\nстрочка.\n\n<!-- c -->\n\n----\nТретий");
    let v: Vec<_> = c.paragraphs.iter().map(|p| (p.id, p.para_id, p.text.as_str())).collect();
    assert_eq!(v, vec![(0, Some(0), "Первый."), (1, Some(1), "Второй строчка."), (2, Some(2), "Третий")]);
    for p in &c.paragraphs {
        assert_eq!((p.para_offset, p.parent_type.as_deref(), p.parent_id, p.fallback), (None, None, None, false));
        assert_eq!(p.fp, fp(&norm(&p.text)));
        assert_eq!(p.n_chars, p.text.chars().count() as i32);
    }
}

#[test]
fn lists_definitions_preformatted_tables() {
    let t = "До\n# a\n## b\n#* c\n# d\n; термин\n: опред\nМежду\n pre  line\n pre2\n{|\n| ячейка\n|}\nПосле";
    let c = components(t);
    assert_eq!(
        kinds(&c.paragraphs),
        vec![
            ("prose", None, None, "До"),
            ("list_item", Some(0), Some(1), "a"),
            ("list_item", Some(1), Some(2), "b"),
            ("list_item", Some(2), Some(2), "c"),
            ("list_item", Some(0), Some(1), "d"),
            ("definition_term", Some(3), Some(1), "термин"),
            ("definition", Some(3), Some(1), "опред"),
            ("prose", None, None, "Между"),
            ("preformatted", None, None, "pre line pre2"),
            ("prose", None, None, "После"),
        ]
    );
}

#[test]
fn paragraph_spans() {
    // A span runs from the first node to the last, excluded nodes included, edge whitespace not.
    let t = "Ноль.\n\n<!-- c -->\n\n{{a}}Текст.<ref>x</ref>  \n\n* пункт {{b}}\n";
    let c = components(t);
    let p = &c.paragraphs[1];
    assert_eq!(&t[p.start as usize..p.end as usize], "{{a}}Текст.<ref>x</ref>");
    let item = &c.paragraphs[2];
    assert_eq!(&t[item.start as usize..item.end as usize], " пункт {{b}}");
    assert_eq!(item.kind, "list_item");
}

#[test]
fn spans_round_trip() {
    let mut texts: Vec<String> = FIXTURES.iter().map(|f| common::fixture(f)).collect();
    texts.extend(common::public_corpus().into_iter().map(|r| r.wikitext));
    let mut checked = 0;
    for text in &texts {
        let c = run_page(text, "wikipedia").components;
        let chars = CharIndex::new(text);
        let spans = c
            .sections
            .iter()
            .map(|s| (s.start, s.end, s.start_char, s.end_char))
            .chain(c.paragraphs.iter().map(|p| (p.start, p.end, p.start_char, p.end_char)));
        for (start, end, start_char, end_char) in spans {
            assert!(start <= end, "{start}..{end}");
            assert!(text.get(start as usize..end as usize).is_some(), "{start}..{end} not on char boundaries");
            assert_eq!(start_char, chars.char_of(start as usize));
            assert_eq!(end_char, chars.char_of(end as usize));
            checked += 1;
        }
        // Paragraphs are in document order and sit inside their section. (They may overlap: a list item's
        // span is its whole ListItem node, nested lists included.)
        for w in c.paragraphs.windows(2) {
            assert!(w[0].start <= w[1].start, "paragraphs out of order");
        }
        for p in &c.paragraphs {
            let s = &c.sections[p.section_id as usize];
            assert!(s.start <= p.start && p.end <= s.end, "paragraph outside its section");
            assert!(!p.text.is_empty() && p.text.trim() == p.text && !p.text.contains("  "));
        }
    }
    assert!(checked > 1000, "{checked}");
}

#[test]
fn depth_limit_and_budget() {
    let deep = "{{a|".repeat(500) + &"}}".repeat(500);
    assert_eq!(run_with(&deep, Limits { fallback: false, ..defaults() }).page.status, "depth_limit");
    let r = run_with(&deep, Limits { depth_cap: 500, fallback: false, ..defaults() });
    assert_eq!(r.page.status, "ok");
    let t = common::fixture("refs_basic");
    let r = run_with(&t, Limits { walk_budget: 5, fallback: false, ..defaults() });
    assert_eq!(r.page.status, "budget");
    assert!(r.page.status_detail.unwrap().starts_with("budget: "));
    assert_eq!(run_with(&t, Limits { walk_budget: 5, ..defaults() }).page.status, "fallback");
}

#[test]
fn stats_count_what_the_walk_counts() {
    let s = common::ru();
    let t = common::fixture("refs_basic");
    let parsed = frontend::parse(&t, s.configuration(), 0).unwrap();
    let st = walk::stats(&parsed);
    let lim = |budget, depth_cap| walk::WalkLimits { budget, depth_cap };
    let reg = common::reg();
    assert!(walk::walk(&t, &parsed, &s, &reg, &lim(st.nodes, st.max_depth)).is_ok());
    assert_eq!(
        walk::walk(&t, &parsed, &s, &reg, &lim(st.nodes - 1, st.max_depth)).unwrap_err(),
        walk::WalkError::Budget { nodes: st.nodes }
    );
    assert_eq!(
        walk::walk(&t, &parsed, &s, &reg, &lim(st.nodes, st.max_depth - 1)).unwrap_err(),
        walk::WalkError::DepthLimit { depth: st.max_depth }
    );
}

#[test]
fn walk_constants_have_headroom_on_corpus() {
    let s = common::ru();
    let (mut max_nodes, mut max_depth) = (0u64, 0usize);
    let rows = common::public_corpus();
    for r in &rows {
        let parsed = frontend::parse(&r.wikitext, s.configuration(), DEFAULT_PARSE_BUDGET).expect("parses");
        let st = walk::stats(&parsed);
        max_nodes = max_nodes.max(st.nodes);
        max_depth = max_depth.max(st.max_depth);
    }
    eprintln!("public corpus ({} rows): max nodes {max_nodes}, max depth {max_depth}", rows.len());
    assert!(max_nodes * 10 <= DEFAULT_WALK_BUDGET, "max nodes {max_nodes}");
    assert!(max_depth * 4 <= DEFAULT_DEPTH_CAP, "max depth {max_depth}");
}
