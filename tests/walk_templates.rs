mod common;

use common::run_page;
use wikitext_components::fingerprint::fp;
use wikitext_components::model::{Components, ParamRow, RefRow, TemplateRow};
use wikitext_components::spans::CharIndex;

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

/// The innermost template or ref whose span contains `start..end` and is larger. A component of the same span
/// is the component itself or another view of its node (a footnote or ref-list template is also a template
/// row), never its parent. Of a footnote template's two views around something, the ref is the nearer.
fn innermost(c: &Components, (start, end): (i32, i32)) -> Option<(&'static str, i32)> {
    let templates = c.templates.iter().map(|t| ("template", t.id, t.start, t.end));
    let refs = c.refs.iter().map(|r| ("ref", r.id, r.start, r.end));
    templates
        .chain(refs)
        .filter(|&(_, _, s, e)| s <= start && end <= e && (s, e) != (start, end))
        .min_by_key(|&(ty, _, s, e)| (e - s, ty != "ref"))
        .map(|(ty, id, ..)| (ty, id))
}

fn assert_parent(c: &Components, what: &str, got: (Option<&str>, Option<i32>), span: (i32, i32), id: i32) {
    let want = innermost(c, span);
    assert_eq!(got, (want.map(|w| w.0), want.map(|w| w.1)), "{what} {id}");
}

fn check_page(text: &str, c: &Components, n: &mut [usize; 4]) {
    let chars = CharIndex::new(text);
    let span_ok = |what: &str, start: i32, end: i32, start_char: i32, end_char: i32| -> String {
        let s = text.get(start as usize..end as usize).unwrap_or_else(|| panic!("{what} span on char boundaries"));
        assert_eq!((start_char, end_char), (chars.char_of(start as usize), chars.char_of(end as usize)), "{what}");
        s.to_string()
    };
    for (i, t) in c.templates.iter().enumerate() {
        assert_eq!(t.id, i as i32);
        let s = span_ok("template", t.start, t.end, t.start_char, t.end_char);
        assert!(s.starts_with("{{"), "{s:?}");
        assert_parent(c, "template", (t.parent_type.as_deref(), t.parent_id), (t.start, t.end), t.id);
        let around = c.templates.iter().filter(|o| o.id != t.id && o.start <= t.start && t.end <= o.end).count();
        assert_eq!(t.depth as usize, around, "depth counts the templates around");
        assert!(c.sections.get(t.section_id as usize).is_some());
        assert_eq!(t.para_id.is_some(), t.para_offset.is_some());
        n[0] += 1;
        for (i0, p) in t.params.iter().enumerate() {
            let whole = text.get(p.start as usize..p.end as usize).expect("param span on char boundaries");
            assert!(t.start <= p.start && p.end <= t.end);
            // The colon parameter of a module or parser function: exactly its trimmed text. Any
            // other parameter's span holds its value.
            let colon_param = i0 == 0 && matches!(t.kind.as_str(), "module" | "parser_function") && t.name_raw.contains(':');
            if colon_param {
                assert_eq!(whole, p.value_raw);
            } else {
                assert!(whole.contains(&p.value_raw), "{whole:?} {:?}", p.value_raw);
            }
            assert_eq!(p.value_raw, p.value_raw.trim());
            assert!(!p.value_text.contains("  ") && p.value_text == p.value_text.trim());
            n[1] += 1;
        }
    }
    let has_body = |r: &RefRow| r.kind != "reuse" && !r.body_raw.is_empty();
    for (i, r) in c.refs.iter().enumerate() {
        assert_eq!(r.id, i as i32);
        let s = span_ok("ref", r.start, r.end, r.start_char, r.end_char);
        if r.kind == "template" {
            let t = c.templates.iter().find(|t| (t.start, t.end) == (r.start, r.end)).expect("a template ref is a template");
            assert_eq!(t.role, "footnote");
        } else {
            assert!(s.get(..4).is_some_and(|h| h.eq_ignore_ascii_case("<ref")) && s.ends_with('>'), "{s:?}");
            assert!(s.contains(&r.body_raw), "{s:?} {:?}", r.body_raw);
        }
        assert!(["inline", "named", "reuse", "list_defined", "template"].contains(&r.kind.as_str()), "{}", r.kind);
        assert_eq!(r.body_raw, r.body_raw.trim());
        assert!(!r.body_text.contains("  ") && r.body_text == r.body_text.trim());
        assert_parent(c, "ref", (r.parent_type.as_deref(), r.parent_id), (r.start, r.end), r.id);
        assert!(c.sections.get(r.section_id as usize).is_some());
        assert_eq!(r.para_id.is_some(), r.para_offset.is_some());
        assert_eq!(r.name.is_some(), r.name.as_deref() != Some("") && r.name.is_some());
        match r.def_ref_id {
            Some(d) => {
                let d = &c.refs[d as usize];
                assert!(has_body(d) && d.def_ref_id == Some(d.id));
                assert_eq!((&d.group, &d.name), (&r.group, &r.name));
                // The first ref with a body for its (group, name).
                assert!(!c.refs[..d.id as usize].iter().any(|o| has_body(o) && (&o.group, &o.name) == (&d.group, &d.name)));
            }
            None => assert!(r.name.is_none() || !c.refs.iter().any(|o| has_body(o) && (&o.group, &o.name) == (&r.group, &r.name))),
        }
        assert_eq!(r.n_uses.is_some(), r.name.is_some() && matches!(r.kind.as_str(), "named" | "list_defined"));
        assert_eq!(r.resolved, has_body(r) || r.def_ref_id.is_some());
        assert_eq!(r.fp.len(), 16);
        n[2] += 1;
    }
    for (i, l) in c.ref_lists.iter().enumerate() {
        assert_eq!(l.id, i as i32);
        let s = span_ok("ref list", l.start, l.end, l.start_char, l.end_char);
        match (l.kind.as_str(), l.template_id) {
            ("tag", None) => assert!(s.get(..11).is_some_and(|h| h.eq_ignore_ascii_case("<references")), "{s:?}"),
            ("template", Some(t)) => {
                let t = &c.templates[t as usize];
                assert_eq!(((t.start, t.end), t.role.as_str()), ((l.start, l.end), "ref_list"));
            }
            other => panic!("ref list kind {other:?}"),
        }
        assert_parent(c, "ref list", (l.parent_type.as_deref(), l.parent_id), (l.start, l.end), l.id);
        assert!(c.sections.get(l.section_id as usize).is_some());
        let defs = c.refs.iter().filter(|r| r.group == l.group && has_body(r) && r.def_ref_id.is_none_or(|d| d == r.id)).count();
        assert_eq!(l.n_entries as usize, defs);
        n[3] += 1;
    }
}

#[test]
fn spans_round_trip_on_the_corpus() {
    let mut texts: Vec<(String, &str)> =
        ["templates", "refs_in_template", "sfn", "efn", "malformed", "refs_basic", "refs_groups_listdefined", "refs_reuse_order"]
            .iter()
            .map(|f| (common::fixture(f), "wikipedia"))
            .collect();
    texts.extend(common::public_corpus().into_iter().map(|r| (r.wikitext, "wikipedia")));
    let public = texts.len();
    // The private corpus too, when present: only assertions, nothing of it is printed or kept.
    if let Some(rows) = common::private_corpus("spans_round_trip_on_the_corpus (private part)") {
        texts.extend(rows.into_iter().map(|r| (r.wikitext, "ruwiki")));
    }
    let mut n = [0usize; 4];
    let mut public_n = [0usize; 4];
    for (i, (text, site)) in texts.iter().enumerate() {
        let c = run_page(text, site).components;
        check_page(text, &c, &mut n);
        if i + 1 == public {
            public_n = n;
        }
    }
    let [templates, params, refs, lists] = public_n;
    eprintln!("public corpus and fixtures: {templates} templates, {params} parameters, {refs} refs, {lists} ref lists");
    assert!(templates > 1000 && refs > 1000 && lists > 100, "{public_n:?}");
}

#[test]
fn magic_word_fingerprint_uses_the_lowercased_name() {
    assert_eq!(templates("{{LC:ABC}}")[0].fp, fp("lc\u{1f}1=abc"));
}
