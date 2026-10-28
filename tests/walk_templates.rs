mod common;

use common::run_page;
use wikitext_components::fingerprint::fp;
use wikitext_components::model::{ParamRow, TemplateRow};
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
    let v: Vec<_> = t.iter().map(|t| (t.name.as_str(), t.para_id, t.para_offset, t.parent_id)).collect();
    // The link label is text, so {{a}} sits after "Город столица"; {{b}} sits where the ref is, after "и".
    assert_eq!(v, vec![("a", Some(0), Some(13), None), ("b", Some(0), Some(15), None)]);
    let t = templates("Текст<ref>{{x|{{y}}}}</ref> дальше");
    assert_eq!((t[0].para_offset, t[1].para_offset), (Some(5), Some(5)));
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
    assert_eq!((t[0].name.as_str(), t[0].kind.as_str()), ("dEFAULTSORT", "parser_function"));
    assert_eq!(params(&t[0].params), vec![("1", true, "X")]);
    let t = templates("{{PAGENAME:x}}");
    assert_eq!((t[0].name.as_str(), t[0].kind.as_str()), ("pAGENAME", "parser_function"));
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
    let mut texts: Vec<String> = ["templates", "refs_in_template", "sfn", "efn", "malformed", "refs_basic"]
        .iter()
        .map(|f| common::fixture(f))
        .collect();
    texts.extend(common::public_corpus().into_iter().map(|r| r.wikitext));
    let (mut n_templates, mut n_params) = (0, 0);
    for text in &texts {
        let c = run_page(text, "wikipedia").components;
        let chars = CharIndex::new(text);
        for (i, t) in c.templates.iter().enumerate() {
            assert_eq!(t.id, i as i32);
            let s = text.get(t.start as usize..t.end as usize).expect("template span on char boundaries");
            assert!(s.starts_with("{{"), "{s:?}");
            assert_eq!((t.start_char, t.end_char), (chars.char_of(t.start as usize), chars.char_of(t.end as usize)));
            match t.parent_id {
                Some(p) => {
                    let p = &c.templates[p as usize];
                    assert!(p.id < t.id && p.start <= t.start && t.end <= p.end);
                    assert_eq!(t.depth, p.depth + 1);
                }
                None => assert_eq!(t.depth, 0),
            }
            assert!(c.sections.get(t.section_id as usize).is_some());
            assert_eq!(t.para_id.is_some(), t.para_offset.is_some());
            n_templates += 1;
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
                n_params += 1;
            }
        }
    }
    eprintln!("public corpus: {n_templates} templates, {n_params} parameters");
    assert!(n_templates > 1000, "{n_templates}");
}
