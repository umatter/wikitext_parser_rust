mod common;

use common::check::{check_page, Page};
use common::run_page;
use wikitext_components::fingerprint::fp;
use wikitext_components::model::{ParamRow, TemplateRow};

fn templates(text: &str) -> Vec<TemplateRow> {
    let r = run_page(text, "wikipedia");
    assert_eq!(r.page.status, "ok", "{:?}", r.page.status_detail);
    r.components.templates
}

fn params(p: &[ParamRow]) -> Vec<(&str, bool, &str)> {
    p.iter().map(|p| (p.name.as_str(), p.positional, p.value_raw.as_str())).collect()
}

#[test]
fn templates_params_nesting_kinds() {
    let t = run_page(&common::fixture("templates"), "wikipedia").components.templates;
    let v: Vec<_> = t.iter().map(|t| (t.name.as_str(), t.kind.as_str(), t.depth, t.parent_id)).collect();
    assert_eq!(
        v,
        vec![
            ("карточка города", "template", 0, None),
            ("num", "template", 1, Some(0)),
            ("нп3", "template", 0, None),
            ("#if", "parser_function", 0, None)
        ]
    );
    let p: Vec<_> = t[0].params.iter().map(|p| (p.name.as_str(), p.positional, p.value_raw.as_str())).collect();
    assert_eq!(
        p,
        vec![("название", false, "Москва"), ("население", false, "{{num|13000000}}"), ("1", false, "позиционный")]
    );
    assert_eq!(t[2].params.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(), vec!["1", "2", "3", "4"]);
    assert!(t[2].params.iter().all(|p| p.positional));
    let f: Vec<_> = t[3].params.iter().map(|p| (p.name.as_str(), p.value_raw.as_str())).collect();
    assert_eq!(f, vec![("1", "x"), ("2", "да"), ("3", "нет")]);
}

#[test]
fn common_fields_and_values() {
    let text = common::fixture("templates");
    let t = templates(&text);
    assert_eq!(t.iter().map(|t| t.id).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    assert_eq!(t[0].name_raw, "Карточка города");
    assert_eq!(t[1].parent_type.as_deref(), Some("template"));
    assert_eq!(t[0].parent_type, None);
    assert!(t.iter().all(|t| !t.fallback && t.role == "other" && !t.looks_like_infobox && t.rendered_len == 0));
    assert_eq!(&text[t[1].start as usize..t[1].end as usize], "{{num|13000000}}");
    assert_eq!(t[3].name_raw, "#if:x");
    assert_eq!(t[0].params[1].value_text, ""); // a template adds no text
    assert_eq!(t[0].params[0].value_text, "Москва");
    assert_eq!(t[2].params[1].value_text, "Tahmineh Milani");
    // One newline does not break the paragraph: the card opens it, the second template sits after "Текст".
    assert_eq!((t[0].para_id, t[0].para_offset), (Some(0), Some(0)));
    assert_eq!((t[2].para_id, t[2].para_offset), (Some(0), Some(5)));
    assert_eq!(t[2].fp.len(), 16);
}

#[test]
fn nested_two_deep_has_a_parent_chain() {
    let t = templates("{{a|{{b|{{c}}}}}}");
    let v: Vec<_> = t.iter().map(|t| (t.name.as_str(), t.depth, t.parent_id)).collect();
    assert_eq!(v, vec![("a", 0, None), ("b", 1, Some(0)), ("c", 2, Some(1))]);
}

#[test]
fn template_in_a_link_label_or_ref_takes_the_excluded_ancestors_offset() {
    let t = templates("Город [[Москва|столица {{a}}]] и<ref>{{b}}</ref> стоит.");
    let v: Vec<_> =
        t.iter().map(|t| (t.name.as_str(), t.para_id, t.para_offset, t.parent_type.as_deref(), t.parent_id)).collect();
    // The link label is text, so {{a}} sits after "Город столица"; {{b}} sits where the ref is, after "и",
    // and the ref is its parent (R33).
    assert_eq!(v, vec![("a", Some(0), Some(13), None, None), ("b", Some(0), Some(15), Some("ref"), Some(0))]);
    let t = templates("Текст<ref>{{x|{{y}}}}</ref> дальше");
    assert_eq!((t[0].para_offset, t[1].para_offset), (Some(5), Some(5)));
    assert_eq!((t[0].parent_type.as_deref(), t[1].parent_type.as_deref(), t[1].parent_id), (Some("ref"), Some("template"), Some(0)));
    assert_eq!((t[0].depth, t[1].depth), (0, 1));
}

#[test]
fn module_parser_function_and_variable_kinds() {
    let t = templates("{{#invoke:Module|fn|a}}");
    assert_eq!((t[0].name.as_str(), t[0].kind.as_str()), ("#invoke", "module"));
    assert_eq!(params(&t[0].params), vec![("1", true, "Module"), ("2", true, "fn"), ("3", true, "a")]);
    let t = templates("{{lc:ABC}}");
    assert_eq!((t[0].name.as_str(), t[0].kind.as_str()), ("lc", "parser_function"));
    assert_eq!(params(&t[0].params), vec![("1", true, "ABC")]);
    assert_eq!(t[0].params[0].value_text, "ABC");
    let t = templates("{{PAGENAME}}");
    assert_eq!(t[0].kind, "variable");
    assert!(t[0].params.is_empty());
    let t = templates("{{Шаблон:Foo|x}}");
    assert_eq!((t[0].name.as_str(), t[0].kind.as_str()), ("foo", "template"));
}

#[test]
fn pipe_and_equals_magic_words_are_variables() {
    for site in ["wikipedia", "ruwiki"] {
        let t = run_page("a{{!}}b{{=}}c", site).components.templates;
        let v: Vec<_> = t.iter().map(|t| (t.name.as_str(), t.kind.as_str())).collect();
        assert_eq!(v, vec![("!", "variable"), ("=", "variable")], "{site}");
    }
}

#[test]
fn colon_parameter_span_and_text() {
    let text = "{{ #if: [[a|b]] {{c}} d |y}}";
    let t = templates(text);
    let p = &t[0].params[0];
    assert_eq!(p.value_raw, "[[a|b]] {{c}} d");
    assert_eq!(&text[p.start as usize..p.end as usize], p.value_raw);
    assert_eq!(p.value_text, "b d");
    assert_eq!(t[0].name_raw, "#if: [[a|b]] {{c}} d");
    assert_eq!(params(&t[0].params)[1], ("2", true, "y"));
}

#[test]
fn named_numbered_and_duplicate_parameters() {
    let t = templates("{{a|x|2=y|z|k=1|k=2}}");
    let v: Vec<_> = t[0].params.iter().map(|p| (p.name.as_str(), p.positional, p.value_raw.as_str())).collect();
    assert_eq!(v, vec![("1", true, "x"), ("2", false, "y"), ("2", true, "z"), ("k", false, "1"), ("k", false, "2")]);
}

#[test]
fn empty_names_do_not_panic() {
    for text in ["{{}}", "{{|x}}", "{{ }}", "{{:x}}", "{{#}}", "{{a|", "{{{{}}}}"] {
        let r = run_page(text, "wikipedia");
        assert_eq!(r.page.status, "ok", "{text}");
    }
    // The parser yields a row for an empty name, and none for an unterminated template or `{{{{}}}}`.
    for text in ["{{}}", "{{|x}}", "{{ }}"] {
        let t = templates(text);
        assert_eq!(t.len(), 1, "{text}");
        assert_eq!((t[0].name.as_str(), t[0].name_raw.as_str(), t[0].kind.as_str()), ("", "", "template"));
    }
    for text in ["{{a|", "{{{{}}}}"] {
        assert_eq!(templates(text).len(), 0, "{text}");
    }
    let t = templates("{{:x}}");
    assert_eq!((t.len(), t[0].name.as_str(), t[0].kind.as_str()), (1, ":x", "template"));
    let t = templates("{{#}}");
    assert_eq!((t.len(), t[0].name.as_str(), t[0].kind.as_str()), (1, "#", "parser_function"));
}

#[test]
fn namespace_prefix_makes_a_template() {
    let t = templates("{{Шаблон:PAGENAME}}");
    assert_eq!(t[0].kind, "template");
    let t = templates("{{DEFAULTSORT:X}}");
    assert_eq!((t[0].name.as_str(), t[0].kind.as_str()), ("defaultsort", "parser_function"));
    assert_eq!(params(&t[0].params), vec![("1", true, "X")]);
    for (text, name) in [("{{#IF:x|a}}", "#if"), ("{{LC:ABC}}", "lc"), ("{{#Invoke:M|f}}", "#invoke"), ("{{PAGENAME}}", "pagename")] {
        assert_eq!(templates(text)[0].name, name, "{text}");
    }
    let t = templates("{{PAGENAME:x}}");
    assert_eq!((t[0].name.as_str(), t[0].kind.as_str()), ("pagename", "parser_function"));
}

#[test]
fn fingerprint_ignores_order_and_spacing_of_parameters() {
    let a = templates("{{a|x=1|y=2}}");
    let b = templates("{{a| y = 2 |x=1}}");
    assert_eq!(a[0].fp, b[0].fp);
    assert_eq!(a[0].fp, fp("a\u{1f}x=1\u{1f}y=2"));
    assert_ne!(a[0].fp, templates("{{a|x=1|y=3}}")[0].fp);
}

#[test]
fn spans_round_trip_on_the_corpus() {
    let mut texts: Vec<(String, &str, bool)> =
        ["templates", "refs_in_template", "sfn", "efn", "malformed", "refs_basic", "refs_groups_listdefined", "refs_reuse_order"]
            .iter()
            .map(|f| (common::fixture(f), "wikipedia", false))
            .collect();
    texts.extend(common::public_corpus().into_iter().map(|r| (r.wikitext, "wikipedia", false)));
    let public = texts.len();
    // The private corpus too, when present. Its pages are checked with `private` set, so a failure names only
    // the page index, the table, the row id and the field; nothing of the markup is printed or kept.
    if let Some(rows) = common::private_corpus("spans_round_trip_on_the_corpus (private part)") {
        texts.extend(rows.into_iter().map(|r| (r.wikitext, "ruwiki", true)));
    }
    let reg = common::reg();
    let mut n = [0usize; 5];
    let mut public_n = [0usize; 5];
    for (index, (text, site, private)) in texts.iter().enumerate() {
        let c = run_page(text, site).components;
        let index = if *private { index - public } else { index };
        check_page(&Page { index, private: *private, text }, &c, &reg, site, &mut n);
        if index + 1 == public && !private {
            public_n = n;
        }
    }
    let [templates, params, refs, lists, citations] = public_n;
    eprintln!("public corpus and fixtures: {templates} templates, {params} parameters, {refs} refs, {lists} ref lists, {citations} citations");
    assert!(templates > 1000 && refs > 1000 && lists > 100 && citations > 1000, "{public_n:?}");
}

#[test]
fn private_failures_name_no_values() {
    let text = "Секрет{{a|b}}";
    let pg = Page { index: 3, private: true, text };
    let err = std::panic::catch_unwind(|| pg.check(false, "template", 0, "value", || text.to_string())).unwrap_err();
    let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
    assert_eq!(msg, "private page 3: template row 0: value");
    let pg = Page { index: 3, private: false, text };
    let err = std::panic::catch_unwind(|| pg.check(false, "template", 0, "value", || "detail".into())).unwrap_err();
    assert!(err.downcast_ref::<String>().unwrap().ends_with(": detail"));
}

#[test]
fn magic_word_fingerprint_uses_the_lowercased_name() {
    assert_eq!(templates("{{LC:ABC}}")[0].fp, fp("lc\u{1f}1=abc"));
}
