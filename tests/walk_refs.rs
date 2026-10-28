mod common;

use common::run_page;
use wikitext_components::fingerprint::{fp, norm};
use wikitext_components::model::Components;

fn comps(text: &str) -> Components {
    let r = run_page(text, "wikipedia");
    assert_eq!(r.page.status, "ok", "{:?}", r.page.status_detail);
    r.components
}

#[test]
fn basic_named_reuse() {
    let c = run_page(&common::fixture("refs_basic"), "wikipedia").components;
    let v: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.name.as_deref(), r.def_ref_id, r.n_uses, r.resolved)).collect();
    assert_eq!(v, vec![("inline", None, None, None, true), ("named", Some("a"), Some(1), Some(2), true), ("reuse", Some("a"), Some(1), None, true)]);
    assert_eq!(c.refs[0].body_text, "Простой источник.");
    assert_eq!(c.ref_lists.len(), 1);
    assert_eq!(c.ref_lists[0].kind, "template");
    assert!(c.refs.iter().all(|r| r.para_id == Some(0)));
}

#[test]
fn groups_and_list_defined() {
    let c = run_page(&common::fixture("refs_groups_listdefined"), "wikipedia").components;
    let kinds: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.group.as_str(), r.name.as_deref())).collect();
    assert_eq!(
        kinds,
        vec![("inline", "К", None), ("reuse", "", Some("b")), ("reuse", "", Some("c")), ("list_defined", "", Some("b")), ("list_defined", "", Some("c"))]
    );
    assert!(c.refs[1].resolved && c.refs[2].resolved);
    assert_eq!(
        c.ref_lists.iter().map(|l| (l.kind.as_str(), l.group.as_str())).collect::<Vec<_>>(),
        vec![("tag", "К"), ("tag", ""), ("template", "")]
    );
}

#[test]
fn reuse_before_definition_and_unresolved() {
    let c = run_page(&common::fixture("refs_reuse_order"), "wikipedia").components;
    assert_eq!(c.refs[0].def_ref_id, Some(1));
    assert_eq!(c.refs[1].n_uses, Some(2));
    assert!(!c.refs[2].resolved);
    assert_eq!(c.refs[2].def_ref_id, None);
}

#[test]
fn ref_inside_template_parameter() {
    let c = run_page(&common::fixture("refs_in_template"), "wikipedia").components;
    assert_eq!(c.refs.len(), 1);
    assert_eq!(c.refs[0].parent_type.as_deref(), Some("template"));
    assert_eq!(c.refs[0].parent_id, Some(0));
}

#[test]
fn bodies_spans_and_common_fields() {
    let text = common::fixture("refs_basic");
    let c = comps(&text);
    let slice = |r: &wikitext_components::model::RefRow| &text[r.start as usize..r.end as usize];
    assert_eq!(slice(&c.refs[0]), "<ref>Простой источник.</ref>");
    assert_eq!(slice(&c.refs[2]), "<ref name=a />");
    assert_eq!(c.refs[0].body_raw, "Простой источник.");
    assert!(c.refs[1].body_raw.starts_with("{{cite web |url=https://www.kremlin.ru/events/1"), "{}", c.refs[1].body_raw);
    assert_eq!(c.refs[1].body_text, ""); // a template adds no text
    assert_eq!((c.refs[2].body_raw.as_str(), c.refs[2].body_text.as_str()), ("", ""));
    // Offsets: "Текст" (5), then "Текст и ещё" (11), then "Текст и ещё. Повтор" (19).
    assert_eq!(c.refs.iter().map(|r| r.para_offset).collect::<Vec<_>>(), vec![Some(5), Some(11), Some(19)]);
    assert!(c.refs.iter().all(|r| r.section_id == 0 && r.group.is_empty() && !r.fallback && r.parent_type.is_none()));
    assert!(c.refs.iter().all(|r| r.harv_key.is_none() && r.cites_citation_id.is_none()));
    assert_eq!(c.refs.iter().map(|r| r.id).collect::<Vec<_>>(), vec![0, 1, 2]);
    assert_eq!(c.refs[0].fp, fp(&format!("inline\u{1f}\u{1f}\u{1f}{}", norm("Простой источник."))));
    assert_eq!(c.refs[2].fp, fp("reuse\u{1f}\u{1f}a\u{1f}"));
    let l = &c.ref_lists[0];
    assert_eq!((l.section_id, l.template_id, l.n_entries, l.group.as_str()), (1, Some(1), 2, ""));
    assert_eq!(&text[l.start as usize..l.end as usize], "{{примечания}}");
    assert_eq!(c.templates[l.template_id.unwrap() as usize].role, "ref_list");
}

#[test]
fn list_defined_refs_take_the_lists_section_and_lists_count_their_group() {
    let c = comps(&common::fixture("refs_groups_listdefined"));
    assert_eq!((c.refs[3].section_id, c.refs[4].section_id), (1, 1));
    assert_eq!(c.refs[1].def_ref_id, Some(3));
    assert_eq!(c.refs[2].def_ref_id, Some(4));
    assert_eq!((c.refs[3].n_uses, c.refs[4].n_uses), (Some(2), Some(2)));
    assert_eq!((c.refs[3].parent_type.as_deref(), c.refs[3].parent_id), (Some("ref_list"), Some(1))); // in <references>
    assert_eq!((c.refs[4].parent_type.as_deref(), c.refs[4].parent_id), (Some("template"), Some(c.ref_lists[2].template_id.unwrap())));
    let v: Vec<_> = c.ref_lists.iter().map(|l| (l.n_entries, l.template_id.is_some())).collect();
    assert_eq!(v, vec![(1, false), (2, false), (2, true)]);
    assert!(c.ref_lists.iter().all(|l| l.section_id == 1 && l.parent_type.is_none()));
}

#[test]
fn template_inside_a_ref_inside_a_template_has_the_ref_as_parent() {
    let c = comps("{{a|x<ref>{{b|{{c}}}}</ref>}}");
    let t: Vec<_> = c.templates.iter().map(|t| (t.name.as_str(), t.parent_type.as_deref(), t.parent_id, t.depth)).collect();
    assert_eq!(t, vec![("a", None, None, 0), ("b", Some("ref"), Some(0), 1), ("c", Some("template"), Some(1), 2)]);
    assert_eq!((c.refs[0].parent_type.as_deref(), c.refs[0].parent_id), (Some("template"), Some(0)));
}

#[test]
fn footnote_templates_are_refs_and_templates() {
    let text = common::fixture("efn");
    let c = comps(&text);
    assert_eq!(c.refs.len(), 1);
    let r = &c.refs[0];
    // No group= parameter: the group is efn's default_group in the registry.
    assert_eq!((r.kind.as_str(), r.name.as_deref(), r.group.as_str(), r.def_ref_id, r.n_uses, r.resolved), ("template", None, "К", None, None, true));
    assert_eq!(r.body_raw, "Пояснение {{cite web |url=https://a.ru/p |title=A}}");
    assert_eq!(r.body_text, "Пояснение");
    // Two views of one node: the efn row in the templates table has the ref's span.
    let efn = &c.templates[0];
    assert_eq!((efn.name.as_str(), efn.start, efn.end, efn.parent_type.as_deref()), ("efn", r.start, r.end, None));
    // What the efn contains belongs to the ref.
    assert_eq!((c.templates[1].name.as_str(), c.templates[1].parent_type.as_deref(), c.templates[1].parent_id), ("cite web", Some("ref"), Some(0)));
    assert_eq!(c.templates[1].depth, 1);
    // {{notelist}} has no default_group (no corpus evidence), so it lists group "", which holds no definition.
    let l = &c.ref_lists[0];
    assert_eq!((l.kind.as_str(), l.group.as_str(), l.template_id, l.n_entries), ("template", "", Some(2), 0));
    assert_eq!(r.fp, fp(&format!("template\u{1f}К\u{1f}\u{1f}{}", norm("Пояснение"))));
}

#[test]
fn footnote_template_name_group_and_numbered_body() {
    let c = comps("А{{efn|name=n|group=g|1=Текст = с равенством}} Б{{efn|name=n|group=g}} В<ref name=n group=g/>\n{{notelist|group=g}}");
    let v: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.name.as_deref(), r.group.as_str(), r.def_ref_id, r.resolved)).collect();
    assert_eq!(v, vec![("template", Some("n"), "g", Some(0), true), ("template", Some("n"), "g", Some(0), true), ("reuse", Some("n"), "g", Some(0), true)]);
    assert_eq!((c.refs[0].body_raw.as_str(), c.refs[0].body_text.as_str()), ("Текст = с равенством", "Текст = с равенством"));
    assert_eq!(c.refs[1].body_raw, "");
    assert_eq!((c.ref_lists[0].group.as_str(), c.ref_lists[0].n_entries), ("g", 1));
}

#[test]
fn a_footnote_template_inside_a_ref_has_the_ref_as_parent() {
    let c = comps("Т<ref>См. {{sfn|Иванов|1990}}</ref>");
    let v: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.parent_type.as_deref(), r.parent_id)).collect();
    assert_eq!(v, vec![("inline", None, None), ("template", Some("ref"), Some(0))]);
    assert_eq!((c.templates[0].parent_type.as_deref(), c.templates[0].parent_id), (Some("ref"), Some(0)));
    assert_eq!((c.refs[1].body_raw.as_str(), c.refs[1].body_text.as_str()), ("Иванов|1990", "Иванов 1990"));
}

#[test]
fn short_footnote_bodies_and_merged_entries() {
    let c = comps("А{{sfn|Иванов|1990|с=5}} Б{{sfn|Иванов|2001|с=7}} В{{sfn|Иванов|1990|с=5}} Г{{sfn| Иванов |1990| с = 5 }}\n{{примечания}}");
    let b: Vec<_> = c.refs.iter().map(|r| (r.body_raw.as_str(), r.body_text.as_str())).collect();
    assert_eq!(
        b,
        vec![("Иванов|1990|с=5", "Иванов 1990 с=5"), ("Иванов|2001|с=7", "Иванов 2001 с=7"), ("Иванов|1990|с=5", "Иванов 1990 с=5"), ("Иванов |1990| с = 5", "Иванов 1990 с=5")]
    );
    assert_ne!(c.refs[0].fp, c.refs[1].fp);
    assert!(c.refs[0].fp == c.refs[2].fp && c.refs[0].fp == c.refs[3].fp);
    // R50: a short footnote is resolved when it cites a citation; this page has none.
    assert!(c.refs.iter().all(|r| r.kind == "template" && !r.resolved && r.def_ref_id.is_none()));
    // Identical short footnotes are one entry, as MediaWiki merges them: 1990 and 2001.
    assert_eq!(c.ref_lists[0].n_entries, 2);
    // An sfn alias is a short footnote too; one without parameters has no body.
    let c = comps("А{{sfnp|Петров|2001}} Б{{sfn}}");
    assert_eq!((c.refs[0].body_text.as_str(), c.refs[1].body_raw.as_str(), c.refs[1].resolved), ("Петров 2001", "", false));
}

#[test]
fn comment_only_bodies_are_empty() {
    let c = comps("а<ref name=a><!-- x --></ref> б<ref name=a>Y</ref> в<ref> <!-- только --> </ref> г{{efn|<!-- x -->}}");
    let v: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.def_ref_id, r.n_uses, r.resolved, r.body_raw.as_str())).collect();
    assert_eq!(
        v,
        vec![("reuse", Some(1), None, true, ""), ("named", Some(1), Some(2), true, "Y"), ("inline", None, None, false, ""), ("template", None, None, false, "")]
    );
    // A comment around text is still a body.
    let c = comps("а<ref name=a><!-- x -->Y</ref>");
    assert_eq!((c.refs[0].kind.as_str(), c.refs[0].body_text.as_str()), ("named", "Y"));
}

#[test]
fn template_groups_and_names_lose_their_quotes_and_take_the_default() {
    let c = comps("А{{efn|group=\"К\"|name='n'|Текст}} Б{{efn|Другой}} В{{efn|group=|Третий}} Г<ref name=n group=К/>\n{{комментарии}}\n{{примечания|group=\"~\"}}");
    let v: Vec<_> = c.refs.iter().map(|r| (r.group.as_str(), r.name.as_deref(), r.def_ref_id)).collect();
    // A group= given empty is the group "" ({{{group|default}}} takes the default only when group= is absent).
    assert_eq!(v, vec![("К", Some("n"), Some(0)), ("К", None, None), ("", None, None), ("К", Some("n"), Some(0))]);
    let l: Vec<_> = c.ref_lists.iter().map(|l| (l.group.as_str(), l.n_entries)).collect();
    assert_eq!(l, vec![("К", 2), ("~", 0)]);
    // Quotes as #tag strips them: a matching or mismatched pair around at least one character, or "" or ''.
    let groups = |src: &str| comps(src).refs.iter().map(|r| r.group.clone()).collect::<Vec<_>>();
    assert_eq!(groups("{{efn|group=''|a}}{{efn|group=\"|b}}{{efn|group='x\"|c}}{{efn|group=\"'|d}}"), vec!["", "\"", "x", "\"'"]);
}

#[test]
fn references_tag_is_the_parent_of_what_it_holds() {
    let c = comps("Т<ref name=a/>\n<references>\n<ref name=a>{{cite web|url=x}}</ref>\n{{x}}\n</references>\n{{примечания|refs=<ref name=b>Y</ref>}}");
    let r: Vec<_> = c.refs.iter().map(|r| (r.parent_type.as_deref(), r.parent_id)).collect();
    assert_eq!(r, vec![(None, None), (Some("ref_list"), Some(0)), (Some("template"), Some(2))]);
    let t: Vec<_> = c.templates.iter().map(|t| (t.name.as_str(), t.parent_type.as_deref(), t.parent_id)).collect();
    assert_eq!(t, vec![("cite web", Some("ref"), Some(1)), ("x", Some("ref_list"), Some(0)), ("примечания", None, None)]);
    let c = comps("<references>\n<references/>\n</references>");
    assert_eq!((c.ref_lists[1].parent_type.as_deref(), c.ref_lists[1].parent_id), (Some("ref_list"), Some(0)));
}

#[test]
fn ref_list_fingerprints_name_the_template() {
    let c = comps("Т<ref>x</ref>\n{{примечания}}\n{{reflist}}\n<references/>");
    let f: Vec<_> = c.ref_lists.iter().map(|l| l.fp.as_str()).collect();
    assert_eq!(f, vec![fp("template\u{1f}\u{1f}примечания"), fp("template\u{1f}\u{1f}reflist"), fp("tag\u{1f}\u{1f}")]);
}

#[test]
fn duplicate_definitions_point_to_the_first() {
    let c = comps("а<ref name=a>X</ref> б<ref name=\"a\">Y</ref> в<ref name=a/>\n<references/>");
    let v: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.def_ref_id, r.n_uses, r.resolved)).collect();
    assert_eq!(v, vec![("named", Some(0), Some(3), true), ("named", Some(0), Some(3), true), ("reuse", Some(0), None, true)]);
    assert_eq!(c.ref_lists[0].n_entries, 1); // the later definition is not a second entry
}

#[test]
fn the_last_name_and_group_attribute_wins() {
    let c = comps("а<ref name=a name=b group=x group=y>X</ref> б<ref group=y name=b/> в<ref name=a group=y/>");
    let v: Vec<_> = c.refs.iter().map(|r| (r.name.as_deref(), r.group.as_str(), r.def_ref_id, r.resolved)).collect();
    assert_eq!(v, vec![(Some("b"), "y", Some(0), true), (Some("b"), "y", Some(0), true), (Some("a"), "y", None, false)]);
}

#[test]
fn group_scopes_resolution_and_list_defined_refs_take_their_lists_group() {
    let c = comps(
        "а<ref name=a group=g/> б<ref name=a/> в<ref name=b group=h/>\n<references group=g>\n<ref name=a>X</ref>\n</references>\n{{примечания|group=h|refs=<ref name=b>Y</ref>}}",
    );
    let v: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.group.as_str(), r.def_ref_id, r.resolved)).collect();
    assert_eq!(
        v,
        vec![("reuse", "g", Some(3), true), ("reuse", "", None, false), ("reuse", "h", Some(4), true), ("list_defined", "g", Some(3), true), ("list_defined", "h", Some(4), true)]
    );
    let l: Vec<_> = c.ref_lists.iter().map(|l| (l.kind.as_str(), l.group.as_str(), l.n_entries)).collect();
    assert_eq!(l, vec![("tag", "g", 1), ("template", "h", 1)]);
}

#[test]
fn empty_bodies() {
    // A named ref with an empty body is a reuse (MediaWiki treats `<ref name=a></ref>` as `<ref name=a/>`);
    // an unnamed one has no body and nothing to resolve.
    let c = comps("а<ref name=a>X</ref> б<ref name=a> </ref> в<ref></ref>");
    let v: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.def_ref_id, r.resolved)).collect();
    assert_eq!(v, vec![("named", Some(0), true), ("reuse", Some(0), true), ("inline", None, false)]);
}

#[test]
fn ref_body_text_follows_the_text_rules() {
    let c = comps("а<ref>См. [[Москва|столицу]] и [https://x.ru сайт] {{cite web|url=u}}&nbsp;''тут''.</ref>");
    assert_eq!(c.refs[0].body_text, "См. столицу и сайт тут.");
    assert_eq!(c.paragraphs[0].text, "а");
}

#[test]
fn malformed_refs_do_not_panic() {
    // (input, refs, ref lists)
    let cases: &[(&str, usize, usize)] = &[
        ("Текст<ref>без конца", 0, 0),          // an unclosed <ref> is text, as in MediaWiki
        ("Текст<ref name=a>без конца", 0, 0),
        ("а<ref>x<ref>y</ref>z</ref>", 2, 0),   // nested: two rows, the inner one a child of the outer
        ("а<ref>x<references/>y</ref>", 1, 1),  // a ref list inside a ref
        ("а<ref>x<references>", 0, 0),
        ("<references><ref name=a>x</ref>", 1, 0), // an unclosed <references>: the ref is still a ref
        ("а</ref>б</references>", 0, 0),
        ("а<ref name=\"a>x</ref>", 1, 0),
        ("а<ref name=a group=b", 0, 0),
        ("а<ref/>б<ref />в<references/>", 2, 1),
        ("{{efn}}{{efn|}}{{примечания|refs=}}", 2, 1),
    ];
    for &(text, n_refs, n_lists) in cases {
        let r = run_page(text, "wikipedia");
        assert_eq!(r.page.status, "ok", "{text}");
        let c = &r.components;
        assert_eq!((c.refs.len(), c.ref_lists.len()), (n_refs, n_lists), "{text}: {:?}", c.refs);
        assert_eq!((r.page.n_refs as usize, r.page.n_ref_lists as usize), (n_refs, n_lists), "{text}");
        for x in &c.refs {
            assert!(text.get(x.start as usize..x.end as usize).is_some(), "{text}");
        }
    }
    let c = comps("а<ref>x<ref>y</ref>z</ref>");
    assert_eq!((c.refs[1].parent_type.as_deref(), c.refs[1].parent_id, c.refs[1].kind.as_str()), (Some("ref"), Some(0), "inline"));
    let c = comps("а<ref/>б");
    assert_eq!((c.refs[0].kind.as_str(), c.refs[0].resolved), ("inline", false));
}

#[test]
fn ruwiki_site_uses_the_same_rules() {
    let r = run_page(&common::fixture("refs_groups_listdefined"), "ruwiki");
    let w = run_page(&common::fixture("refs_groups_listdefined"), "wikipedia");
    assert_eq!(r.components.refs, w.components.refs);
    assert_eq!(r.components.ref_lists, w.components.ref_lists);
}
