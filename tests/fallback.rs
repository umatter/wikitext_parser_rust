//! The fallback scanner (Task 18; rulings R52-R54): one linear pass over the markup that yields sections, refs,
//! ref lists, templates and citations through the same builders as the walk.

mod common;

use common::check::{check_page, Page};
use common::{defaults, reg, ru, run_page, run_with};
use wikitext_components::fallback;
use wikitext_components::model::{CitationRow, Components, RefListRow, RefRow, SectionRow, TemplateRow};
use wikitext_components::page::{Limits, DEFAULT_DEPTH_CAP, DEFAULT_PARSE_BUDGET};

/// (kind, group, name, def_ref_id, resolved) of each ref, in order.
fn summ_refs(c: &Components) -> Vec<(String, String, Option<String>, Option<i32>, bool)> {
    c.refs.iter().map(|r| (r.kind.clone(), r.group.clone(), r.name.clone(), r.def_ref_id, r.resolved)).collect()
}

type CiteSumm = (String, Option<String>, Option<String>, Option<String>, bool, Option<String>);

/// (family, url, archive_url, host, dead_link, harv_key) of each citation, in order.
fn summ_cites(c: &Components) -> Vec<CiteSumm> {
    c.citations
        .iter()
        .map(|x| (x.family.clone(), x.url.clone(), x.archive_url.clone(), x.host.clone(), x.dead_link, x.harv_key.clone()))
        .collect()
}

fn scan(text: &str) -> Components {
    fallback::scan(text, &ru(), &reg())
}

const WELL_FORMED: [&str; 9] = [
    "refs_basic",
    "refs_groups_listdefined",
    "refs_reuse_order",
    "sfn",
    "archive_deadlink",
    "efn",
    "templates",
    "refs_in_template",
    "sections_paragraphs",
];

#[test]
fn parity_with_parser_on_well_formed_fixtures() {
    for f in ["refs_basic", "refs_groups_listdefined", "refs_reuse_order", "sfn", "archive_deadlink", "efn"] {
        let t = common::fixture(f);
        let a = run_page(&t, "wikipedia").components;
        let b = fallback::scan(&t, &ru(), &reg());
        assert_eq!(summ_refs(&a), summ_refs(&b), "{f}");
        assert_eq!(summ_cites(&a), summ_cites(&b), "{f}");
    }
}

/// The row as the parsed path would give it: not a fallback row, with the paragraph place of `parsed`.
fn unmark<T: Clone>(rows: &[T], parsed: &[T], f: impl Fn(&mut T, &T)) -> Vec<T> {
    rows.iter().zip(parsed).map(|(r, p)| {
        let mut r = r.clone();
        f(&mut r, p);
        r
    }).collect()
}

/// On well-formed markup, every row of the five tables is the parser's, but for `fallback` and the paragraph
/// place: one set of builders (R52) and a strip that matches the text rules there (R53).
#[test]
fn rows_equal_the_parsers_on_well_formed_fixtures() {
    for f in WELL_FORMED {
        let t = common::fixture(f);
        let a = run_page(&t, "wikipedia").components;
        let b = scan(&t);
        assert_eq!(
            (b.templates.len(), b.refs.len(), b.ref_lists.len(), b.citations.len(), b.sections.len()),
            (a.templates.len(), a.refs.len(), a.ref_lists.len(), a.citations.len(), a.sections.len()),
            "{f}: row counts"
        );
        let t: Vec<TemplateRow> = unmark(&b.templates, &a.templates, |r, p| {
            (r.fallback, r.para_id, r.para_offset) = (p.fallback, p.para_id, p.para_offset)
        });
        assert_eq!(t, a.templates, "{f}: templates");
        let r: Vec<RefRow> =
            unmark(&b.refs, &a.refs, |r, p| (r.fallback, r.para_id, r.para_offset) = (p.fallback, p.para_id, p.para_offset));
        assert_eq!(r, a.refs, "{f}: refs");
        let l: Vec<RefListRow> = unmark(&b.ref_lists, &a.ref_lists, |r, p| {
            (r.fallback, r.para_id, r.para_offset) = (p.fallback, p.para_id, p.para_offset)
        });
        assert_eq!(l, a.ref_lists, "{f}: ref lists");
        let c: Vec<CitationRow> = unmark(&b.citations, &a.citations, |r, p| {
            (r.fallback, r.para_id, r.para_offset) = (p.fallback, p.para_id, p.para_offset)
        });
        assert_eq!(c, a.citations, "{f}: citations");
        let s: Vec<SectionRow> = unmark(&b.sections, &a.sections, |r, p| r.fallback = p.fallback);
        assert_eq!(s, a.sections, "{f}: sections");
    }
}

#[test]
fn rows_are_fallback_rows_without_paragraphs() {
    for f in WELL_FORMED.iter().chain(&["malformed"]) {
        let c = scan(&common::fixture(f));
        assert!(c.paragraphs.is_empty(), "{f}");
        assert!(!c.sections.is_empty(), "{f}: the lead is always a section");
        let mut common_fields = Vec::new();
        common_fields.extend(c.sections.iter().map(|r| (r.fallback, r.para_id, r.para_offset)));
        common_fields.extend(c.templates.iter().map(|r| (r.fallback, r.para_id, r.para_offset)));
        common_fields.extend(c.refs.iter().map(|r| (r.fallback, r.para_id, r.para_offset)));
        common_fields.extend(c.ref_lists.iter().map(|r| (r.fallback, r.para_id, r.para_offset)));
        common_fields.extend(c.citations.iter().map(|r| (r.fallback, r.para_id, r.para_offset)));
        assert!(common_fields.iter().all(|&f| f == (true, None, None)), "{f}");
    }
}

#[test]
fn budget_failure_falls_back_with_refs() {
    let t = common::fixture("refs_basic");
    let o = run_with(&t, Limits { parse_budget: 10, fallback: true, ..defaults() });
    assert_eq!(o.page.status, "fallback");
    assert_eq!(o.components.refs.len(), 3);
    assert!(o.components.refs.iter().all(|r| r.fallback));
    // The page row keeps why the parse failed, and counts the fallback rows.
    let detail = o.page.status_detail.as_deref().unwrap();
    assert!(detail.starts_with("budget: parse exceeded 10 steps"), "{detail}");
    assert_eq!((o.page.n_refs, o.page.n_ref_lists, o.page.n_citations, o.page.n_paragraphs), (3, 1, 2, 0));
}

#[test]
fn depth_limit_falls_back_with_templates() {
    let n = DEFAULT_DEPTH_CAP + 100;
    let t = "{{a|".repeat(n) + &"}}".repeat(n) + "<ref>x</ref>";
    let o = run_with(&t, defaults());
    assert_eq!(o.page.status, "fallback");
    assert!(o.page.status_detail.as_deref().unwrap().starts_with("depth_limit: "));
    assert_eq!(o.components.templates.len(), DEFAULT_DEPTH_CAP);
    assert_eq!(o.components.refs.len(), 1);
}

#[test]
fn linear_time_on_adversarial_input() {
    let t = "{{a|".repeat(200_000) + &"[[".repeat(200_000) + &"<ref>".repeat(100_000);
    let s = std::time::Instant::now();
    let _ = fallback::scan(&t, &ru(), &reg());
    assert!(s.elapsed().as_secs_f64() < 2.0);
}

#[test]
fn linear_time_on_other_adversarial_inputs() {
    let cases = [
        "<ref name=a ".repeat(100_000),
        "<nowiki>".repeat(100_000) + &"<pre>".repeat(100_000),
        "[[a|".repeat(200_000),
        "[http://a.ru ".repeat(100_000) + "\n",
        "{{{".repeat(200_000) + &"}}".repeat(200_000),
        "</ref>{{a|b=".repeat(100_000),
        "== a {{b ==\n".repeat(100_000),
        "{{a|".repeat(300) + &"}}".repeat(300) + &"<ref>x</ref>".repeat(100_000),
        // A region's closing tag searched for again each time an element that would cut it is dropped: the
        // quadratic search took 41 s and 35 s on these (1.8 MB and 1.6 MB).
        "{{a|<ref><nowiki>}}{{a|<references><nowiki>}}".repeat(40_000) + "</references></ref>",
        "{{a|<ref><pre>}}{{a|<references><pre>}}".repeat(40_000) + "</references></ref>",
        // Nested external links in a ref, each label holding the inner ones: quadratic in time and memory
        // (2.7 s at 1.5 MB).
        "<ref>".to_string() + &"[http://a.ru x ".repeat(399) + &"y".repeat(2_000_000) + &"]".repeat(399) + "</ref>",
    ];
    for t in &cases {
        let s = std::time::Instant::now();
        let _ = fallback::scan(t, &ru(), &reg());
        let secs = s.elapsed().as_secs_f64();
        assert!(secs < 2.0, "{secs} s on {:?}...", &t[..20]);
    }
}

#[test]
fn slow_corpus_pages_yield_refs() {
    let (mut ok, mut fell_back) = (0, 0);
    for r in common::public_corpus().iter().filter(|r| r.case.as_deref() == Some("slow")) {
        let o = run_with(&r.wikitext, Limits { parse_budget: DEFAULT_PARSE_BUDGET, ..defaults() });
        match o.page.status.as_str() {
            "ok" => ok += 1,
            "fallback" => {
                fell_back += 1;
                if r.wikitext.contains("<ref") {
                    assert!(!o.components.refs.is_empty(), "slow page {}: no refs on fallback", r.page_id);
                }
            }
            s => panic!("slow page {}: status {s}", r.page_id),
        }
        // Whether or not the page falls back, the scanner finds its refs.
        if r.wikitext.contains("<ref") {
            assert!(!scan(&r.wikitext).refs.is_empty(), "slow page {}: the scan finds no refs", r.page_id);
        }
    }
    eprintln!("slow pages: {ok} ok, {fell_back} fallback");
    assert_eq!(ok + fell_back, 10);
}

#[test]
fn malformed_never_panics() {
    let o = run_page(&common::fixture("malformed"), "wikipedia");
    assert!(matches!(o.page.status.as_str(), "ok" | "fallback"));
    let o = run_with(&common::fixture("malformed"), Limits { parse_budget: 10, ..defaults() });
    assert_eq!(o.page.status, "fallback");
}

#[test]
fn unclosed_openers_are_not_components_and_their_text_stays() {
    let t = "А {{b|c <ref>d\n== H ==\nE<ref name=x>f</ref> {{g|1}}";
    let c = scan(t);
    let v: Vec<_> = c.templates.iter().map(|x| (x.name.as_str(), x.parent_type.as_deref())).collect();
    assert_eq!(v, vec![("g", None)]);
    let r: Vec<_> = c.refs.iter().map(|x| (x.name.as_deref(), x.body_raw.as_str(), x.parent_type.as_deref())).collect();
    assert_eq!(r, vec![(Some("x"), "f", None)]);
    // The heading inside the unclosed `{{` and `<ref>` counts: neither is a component.
    let s: Vec<_> = c.sections.iter().map(|s| s.title_text.as_str()).collect();
    assert_eq!(s, vec!["", "H"]);
    assert_eq!((c.refs[0].section_id, c.templates[0].section_id), (1, 1));
    // An unclosed `{{` inside a value is text.
    let c = scan("<ref>{{cite web |url=http://a.ru |title=X</ref>");
    assert_eq!((c.templates.len(), c.refs.len()), (0, 1));
    assert_eq!(c.refs[0].body_text, "{{cite web |url=http://a.ru |title=X");
    assert_eq!(c.citations[0].family, "plain_text");
    // A closer finds its opener under unclosed ones, which are dropped.
    let c = scan("{{a|<ref>x}} <ref>{{b|y</ref>");
    let v: Vec<_> = c.templates.iter().map(|x| x.name.as_str()).collect();
    assert_eq!((v, c.refs.len()), (vec!["a"], 1));
    assert_eq!(c.refs[0].body_raw, "{{b|y");
}

#[test]
fn headings_count_only_outside_components_and_skipped_regions() {
    let t = "Лид\n== А ==\n{{x|\n== Б ==\n}}\n<ref>\n== В ==\n</ref>\n<!--\n== Г ==\n-->\n<nowiki>\n== Д ==\n</nowiki>\n\
             <includeonly>\n== Ж ==\n</includeonly>\n=== З {{y}} ===  \n==== И ===\n= К =\n==\n";
    let c = scan(t);
    let s: Vec<_> = c.sections.iter().map(|s| (s.level, s.title_raw.as_str(), s.title_text.as_str())).collect();
    assert_eq!(s, vec![(0, "", ""), (2, "А", "А"), (3, "З {{y}}", "З"), (3, "= И", "= И"), (1, "К", "К")]);
    // As the parser reads them.
    let a = run_page(t, "wikipedia").components.sections;
    let key = |s: &SectionRow| (s.level, s.title_raw.clone(), s.title_text.clone(), s.heading_start, s.heading_end, s.start, s.end, s.subtree_end, s.parent_section_id, s.path.clone(), s.role.clone(), s.fp.clone());
    assert_eq!(c.sections.iter().map(key).collect::<Vec<_>>(), a.iter().map(key).collect::<Vec<_>>());
    // `<pre>` is a skipped region (R54); the parser reads it as an HTML tag and its content as wikitext, so
    // here the two paths differ by design.
    let t = "== А ==\n<pre>\n== Е ==\n</pre>\n== Б ==\n";
    let s: Vec<_> = scan(t).sections.iter().map(|s| s.title_raw.clone()).collect();
    assert_eq!(s, vec!["", "А", "Б"]);
    let s: Vec<_> = run_page(t, "wikipedia").components.sections.iter().map(|s| s.title_raw.clone()).collect();
    assert_eq!(s, vec!["", "А", "Е", "Б"]);
}

#[test]
fn nesting_past_the_depth_cap_keeps_counting() {
    let n = DEFAULT_DEPTH_CAP + 100;
    let t = "{{a|".repeat(n) + &"}}".repeat(n) + "<ref>x</ref>\n== H ==\n{{b}}";
    let c = scan(&t);
    // The outermost templates up to the cap are components; their closes are found past the uncounted ones.
    assert_eq!(c.templates.len(), DEFAULT_DEPTH_CAP + 1);
    assert_eq!((c.templates[0].start, c.templates[0].end as usize), (0, n * 6));
    assert_eq!(c.templates[DEFAULT_DEPTH_CAP - 1].depth as usize, DEFAULT_DEPTH_CAP - 1);
    assert_eq!(c.templates[DEFAULT_DEPTH_CAP].name, "b");
    assert_eq!((c.refs.len(), c.sections.len(), c.templates[DEFAULT_DEPTH_CAP].section_id), (1, 2, 1));
}

#[test]
fn value_text_is_a_linear_strip_of_value_raw() {
    let t = "{{a|1=x<!--c--><nowiki>[[n]]</nowiki> {{y|z}} [[p|q]] [[r]]s [http://u.ru l] [http://v.ru] ''i'' '''b''' <span class=\"k\">t</span>}}";
    let c = scan(t);
    assert_eq!(c.templates[0].params[0].value_text, "x[[n]] q rs l i b t");
    assert_eq!(c.templates[0].params[0].value_text, run_page(t, "wikipedia").components.templates[0].params[0].value_text);
    // Includeonly content adds nothing (R29); the components inside it are still rows, as on the parsed path.
    let t = "{{a|1=x<includeonly>y{{c}}</includeonly>z}}";
    let (a, b) = (run_page(t, "wikipedia").components, scan(t));
    assert_eq!(b.templates[0].params[0].value_text, "xz");
    assert_eq!(a.templates[0].params[0].value_text, "xz");
    assert_eq!(b.templates.len(), a.templates.len());
}

#[test]
fn tags_are_case_insensitive_and_bodies_skip_nowiki_comments_and_pre() {
    let t = "А<REF Name=\"a\">x</Ref >Б<ref name=a/><References/> <nowiki><ref>n</ref></nowiki><!-- <ref>c</ref> --><pre><ref>p</ref></pre>";
    let c = scan(t);
    let v: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.name.as_deref(), r.body_raw.as_str())).collect();
    assert_eq!(v, vec![("named", Some("a"), "x"), ("reuse", Some("a"), "")]);
    assert_eq!(c.ref_lists.len(), 1);
    assert_eq!(&t[c.refs[0].start as usize..c.refs[0].end as usize], "<REF Name=\"a\">x</Ref >");
}

/// Where the parser's rules for malformed markup are the scanner's too, the two paths agree row for row:
/// a closing tag of an open element ends a comment or a `<nowiki>` it is met in, a heading's content runs
/// through an element that spans lines, a link in a link's label makes the outer one text, and the `=`s of a
/// heading line inside a template name its parameter.
#[test]
fn malformed_markup_read_as_the_parser_reads_it() {
    let key = |c: &Components| {
        (
            c.templates.iter().map(|x| (x.start, x.end, x.fp.clone())).collect::<Vec<_>>(),
            c.refs.iter().map(|x| (x.start, x.end, x.fp.clone(), x.body_text.clone())).collect::<Vec<_>>(),
            c.ref_lists.iter().map(|x| (x.start, x.end)).collect::<Vec<_>>(),
            c.citations.iter().map(|x| (x.start, x.end, x.fp.clone())).collect::<Vec<_>>(),
            c.sections.iter().map(|x| (x.heading_start, x.heading_end, x.title_text.clone())).collect::<Vec<_>>(),
        )
    };
    for t in [
        "<ref>a<!-- b</ref> {{c}} <ref>d</ref>",
        "<references>{{efn|z}}<!--</references> {{x}}",
        "<ref>a <nowiki>[1]</ref> {{c}} </nowiki>",
        "== a {{b\n|c}} ==\nтекст\n== d ==\n",
        "=== \n{{cite web|url=http://b.ru|title=T}}",
        "{{a|1=[[x|b [[c]] d]] e|f}}",
        "{{a|\n== b ==\n}}",
        "<ref =={{sfn|A|2000}}[&nbsp;<ref>--></references>",
    ] {
        let a = run_page(t, "wikipedia");
        assert_eq!(a.page.status, "ok", "{t:?}");
        assert_eq!(key(&scan(t)), key(&a.components), "{t:?}");
    }
}

/// The invariants of the walk's rows (spans, char offsets, ids, parents, depths, resolution) hold on the
/// fallback rows of every corpus page, and every row is marked `fallback`. Private pages fail without values.
#[test]
fn corpus_invariants_hold_on_fallback_rows() {
    let mut texts: Vec<(String, &str, bool)> =
        WELL_FORMED.iter().chain(&["malformed"]).map(|f| (common::fixture(f), "wikipedia", false)).collect();
    texts.extend(common::public_corpus().into_iter().map(|r| (r.wikitext, "wikipedia", false)));
    let public = texts.len();
    if let Some(rows) = common::private_corpus("corpus_invariants_hold_on_fallback_rows (private part)") {
        texts.extend(rows.into_iter().map(|r| (r.wikitext, "ruwiki", true)));
    }
    let reg = reg();
    let mut n = [0usize; 5];
    let mut public_n = [0usize; 5];
    for (index, (text, site, private)) in texts.iter().enumerate() {
        let c = fallback::scan(text, &common::settings_for(site), &reg);
        let index = if *private { index - public } else { index };
        let pg = Page { index, private: *private, text };
        check_page(&pg, &c, &reg, site, &mut n);
        pg.check(c.paragraphs.is_empty(), "paragraph", 0, "no paragraphs", String::new);
        let marks = c
            .sections
            .iter()
            .map(|r| ("section", r.id, r.fallback, r.para_id, r.para_offset))
            .chain(c.templates.iter().map(|r| ("template", r.id, r.fallback, r.para_id, r.para_offset)))
            .chain(c.refs.iter().map(|r| ("ref", r.id, r.fallback, r.para_id, r.para_offset)))
            .chain(c.ref_lists.iter().map(|r| ("ref list", r.id, r.fallback, r.para_id, r.para_offset)))
            .chain(c.citations.iter().map(|r| ("citation", r.id, r.fallback, r.para_id, r.para_offset)));
        for (table, id, fb, para_id, para_offset) in marks {
            pg.check(fb, table, id, "fallback", String::new);
            pg.check(para_id.is_none() && para_offset.is_none(), table, id, "para_id and para_offset", String::new);
        }
        for (i, s) in c.sections.iter().enumerate() {
            pg.check(s.id == i as i32 && s.section_id == s.id, "section", s.id, "id", String::new);
            pg.check(s.parent_section_id < s.id || s.id == 0, "section", s.id, "parent_section_id", String::new);
            let ok = text.get(s.start as usize..s.end as usize).is_some() && s.start <= s.end && s.end <= s.subtree_end;
            pg.check(ok, "section", s.id, "span", || format!("{}..{} {}", s.start, s.end, s.subtree_end));
            if let (Some(a), Some(b)) = (s.heading_start, s.heading_end) {
                let h = text.get(a as usize..b as usize).unwrap_or("");
                pg.check(h.starts_with('=') && h.ends_with('='), "section", s.id, "heading span", || format!("{h:?}"));
            }
        }
        if index + 1 == public && !private {
            public_n = n;
        }
    }
    let [templates, params, refs, lists, citations] = public_n;
    eprintln!(
        "fallback rows, public corpus and fixtures: {templates} templates, {params} parameters, {refs} refs, {lists} ref lists, {citations} citations"
    );
    assert!(templates > 1000 && refs > 1000 && lists > 100 && citations > 1000, "{public_n:?}");
}

/// R53's metric, reported and not asserted: of the rows of the five tables, matched between the parsed path and
/// the fallback by table and order, the share whose `fp` agrees. The public corpus, and the private corpus when
/// present, as counts only.
#[test]
fn fp_agreement_with_the_parsed_path() {
    const TABLES: [&str; 5] = ["templates", "refs", "ref_lists", "citations", "sections"];
    fn fps(c: &Components) -> [Vec<&str>; 5] {
        [
            c.templates.iter().map(|x| x.fp.as_str()).collect(),
            c.refs.iter().map(|x| x.fp.as_str()).collect(),
            c.ref_lists.iter().map(|x| x.fp.as_str()).collect(),
            c.citations.iter().map(|x| x.fp.as_str()).collect(),
            c.sections.iter().map(|x| x.fp.as_str()).collect(),
        ]
    }
    // Per table: [agree, matched by order, total of the larger side].
    let agreement = |rows: Vec<String>, site: &str| {
        let (settings, reg) = (common::settings_for(site), reg());
        let mut n = [[0usize; 3]; 5];
        for text in rows {
            let a = run_page(&text, site).components;
            let b = fallback::scan(&text, &settings, &reg);
            for (n, (a, b)) in n.iter_mut().zip(fps(&a).iter().zip(fps(&b).iter())) {
                n[0] += a.iter().zip(b).filter(|(x, y)| x == y).count();
                n[1] += a.len().min(b.len());
                n[2] += a.len().max(b.len());
            }
        }
        n
    };
    let report = |corpus: &str, n: &[[usize; 3]; 5]| {
        let parts: Vec<String> = TABLES
            .iter()
            .zip(n)
            .map(|(t, n)| format!("{t} {}/{} ({:.2}%, {} matched by order)", n[0], n[2], 100.0 * n[0] as f64 / n[2].max(1) as f64, n[1]))
            .collect();
        eprintln!("fp agreement, {corpus}: {}", parts.join(", "));
    };
    let public = agreement(common::public_corpus().into_iter().map(|r| r.wikitext).collect(), "wikipedia");
    report("public corpus", &public);
    assert!(public.iter().all(|n| n[2] > 100), "{public:?}");
    if let Some(rows) = common::private_corpus("fp_agreement_with_the_parsed_path (private part)") {
        report("private corpus", &agreement(rows.into_iter().map(|r| r.wikitext).collect(), "ruwiki"));
    }
}

/// In an external link's label, a nested external link adds nothing, as the walk reads a label.
#[test]
fn ext_link_labels_skip_nested_links_as_the_walker_does() {
    let t = "<ref>[http://a.ru x [http://b.ru y] z]</ref>";
    let titles = |c: &Components| c.citations.iter().map(|c| c.title.clone()).collect::<Vec<_>>();
    let want = vec![Some("x z".to_string()), Some("y".to_string())];
    assert_eq!(titles(&run_page(t, "wikipedia").components), want);
    assert_eq!(titles(&scan(t)), want);
}

/// Past the depth cap, a tag opens no element but pairs with its own closing tag (R56), so it never closes an
/// element it did not open.
#[test]
fn tags_past_the_depth_cap_pair_with_their_own_close() {
    let n = DEFAULT_DEPTH_CAP + 10;
    let t = "<ref>".to_string() + &"{{a|".repeat(n) + "<ref>inner</ref><references>x</references>" + &"}}".repeat(n) + "outer</ref>";
    let c = scan(&t);
    // The ref is the first open element, so templates fill the cap less one.
    assert_eq!(c.templates.len(), DEFAULT_DEPTH_CAP - 1);
    let r: Vec<_> = c.refs.iter().map(|r| (r.start as usize, r.end as usize)).collect();
    assert_eq!(r, vec![(0, t.len())]);
    assert!(c.ref_lists.is_empty());
    assert!(c.refs[0].body_raw.ends_with("outer"));
}
