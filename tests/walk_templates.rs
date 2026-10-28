mod common;

use common::run_page;
use wikitext_components::fingerprint::fp;
use wikitext_components::model::{Components, ParamRow, RefRow, TemplateRow};
use wikitext_components::registry::Registry;
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

/// The innermost template, ref or `<references>` tag whose span contains `start..end` and is larger. A
/// component of the same span is the component itself or another view of its node (a footnote or ref-list
/// template is also a template row), never its parent. Of a footnote template's two views, the ref is nearer.
fn innermost(c: &Components, (start, end): (i32, i32)) -> Option<(&'static str, i32)> {
    let templates = c.templates.iter().map(|t| ("template", t.id, t.start, t.end));
    let refs = c.refs.iter().map(|r| ("ref", r.id, r.start, r.end));
    let lists = c.ref_lists.iter().filter(|l| l.kind == "tag").map(|l| ("ref_list", l.id, l.start, l.end));
    templates
        .chain(refs)
        .chain(lists)
        .filter(|&(_, _, s, e)| s <= start && end <= e && (s, e) != (start, end))
        .min_by_key(|&(ty, _, s, e)| (e - s, ["ref", "ref_list", "template"].iter().position(|t| *t == ty)))
        .map(|(ty, id, ..)| (ty, id))
}

/// One page under check. A failed check on a private page names only the page's index, the table, the row
/// id and the field: no slice of the text and no value, since those would print Ruwiki markup.
struct Page<'a> {
    index: usize,
    private: bool,
    text: &'a str,
}

impl Page<'_> {
    fn check(&self, ok: bool, table: &str, id: i32, field: &str, detail: impl FnOnce() -> String) {
        if ok {
            return;
        }
        if self.private {
            panic!("private page {}: {table} row {id}: {field}", self.index);
        }
        panic!("page {}: {table} row {id}: {field}: {}", self.index, detail());
    }

    /// The span's slice, after checking that it lies on char boundaries and that its char offsets are right.
    fn span(&self, chars: &CharIndex, table: &str, id: i32, (start, end, start_char, end_char): (i32, i32, i32, i32)) -> &str {
        let s = self.text.get(start as usize..end as usize);
        self.check(s.is_some(), table, id, "span on char boundaries", || format!("{start}..{end}"));
        let want = (chars.char_of(start as usize), chars.char_of(end as usize));
        self.check((start_char, end_char) == want, table, id, "start_char/end_char", || format!("{:?} {want:?}", (start_char, end_char)));
        s.unwrap_or("")
    }

    fn parent(&self, c: &Components, table: &str, id: i32, got: (Option<&str>, Option<i32>), span: (i32, i32)) {
        let want = innermost(c, span);
        let want = (want.map(|w| w.0), want.map(|w| w.1));
        self.check(got == want, table, id, "parent", || format!("{got:?} {want:?}"));
    }
}

fn check_page(pg: &Page, c: &Components, reg: &Registry, site: &str, n: &mut [usize; 5]) {
    let text = pg.text;
    let chars = CharIndex::new(text);
    for (i, t) in c.templates.iter().enumerate() {
        let ck = |ok: bool, field: &str, detail: &dyn Fn() -> String| pg.check(ok, "template", t.id, field, detail);
        ck(t.id == i as i32, "id", &|| format!("{} {i}", t.id));
        let s = pg.span(&chars, "template", t.id, (t.start, t.end, t.start_char, t.end_char));
        ck(s.starts_with("{{"), "span starts with {{", &|| format!("{s:?}"));
        pg.parent(c, "template", t.id, (t.parent_type.as_deref(), t.parent_id), (t.start, t.end));
        let around = c.templates.iter().filter(|o| o.id != t.id && o.start <= t.start && t.end <= o.end).count();
        ck(t.depth as usize == around, "depth counts the templates around", &|| format!("{} {around}", t.depth));
        ck(c.sections.get(t.section_id as usize).is_some(), "section_id", &|| t.section_id.to_string());
        ck(t.para_id.is_some() == t.para_offset.is_some(), "para_id with para_offset", &|| format!("{:?} {:?}", t.para_id, t.para_offset));
        n[0] += 1;
        for (i0, p) in t.params.iter().enumerate() {
            let whole = text.get(p.start as usize..p.end as usize);
            ck(whole.is_some(), "param span on char boundaries", &|| format!("{}..{}", p.start, p.end));
            let whole = whole.unwrap_or("");
            ck(t.start <= p.start && p.end <= t.end, "param span inside the template", &|| format!("{}..{}", p.start, p.end));
            // The colon parameter of a module or parser function: exactly its trimmed text. Any
            // other parameter's span holds its value.
            let colon_param = i0 == 0 && matches!(t.kind.as_str(), "module" | "parser_function") && t.name_raw.contains(':');
            let holds = if colon_param { whole == p.value_raw } else { whole.contains(&p.value_raw) };
            ck(holds, "param span holds value_raw", &|| format!("{whole:?} {:?}", p.value_raw));
            ck(p.value_raw == p.value_raw.trim(), "param value_raw trimmed", &|| format!("{:?}", p.value_raw));
            ck(!p.value_text.contains("  ") && p.value_text == p.value_text.trim(), "param value_text collapsed", &|| format!("{:?}", p.value_text));
            n[1] += 1;
        }
    }
    let has_body = |r: &RefRow| r.kind != "reuse" && !r.body_raw.is_empty();
    let template_of = |r: &RefRow| c.templates.iter().find(|t| (t.start, t.end) == (r.start, r.end));
    let short = |r: &RefRow| {
        r.kind == "template"
            && template_of(r).and_then(|t| reg.template(&t.name, site)).is_some_and(|e| e.family.as_deref() == Some("short"))
    };
    for (i, r) in c.refs.iter().enumerate() {
        let ck = |ok: bool, field: &str, detail: &dyn Fn() -> String| pg.check(ok, "ref", r.id, field, detail);
        ck(r.id == i as i32, "id", &|| format!("{} {i}", r.id));
        let s = pg.span(&chars, "ref", r.id, (r.start, r.end, r.start_char, r.end_char));
        if r.kind == "template" {
            let role = template_of(r).map(|t| t.role.as_str());
            ck(role == Some("footnote"), "a template ref is a footnote template", &|| format!("{role:?}"));
        } else {
            let tag = s.get(..4).is_some_and(|h| h.eq_ignore_ascii_case("<ref")) && s.ends_with('>');
            ck(tag, "span is a <ref> tag", &|| format!("{s:?}"));
        }
        ck(s.contains(&r.body_raw), "span holds body_raw", &|| format!("{s:?} {:?}", r.body_raw));
        ck(["inline", "named", "reuse", "list_defined", "template"].contains(&r.kind.as_str()), "kind", &|| r.kind.clone());
        ck(r.body_raw == r.body_raw.trim(), "body_raw trimmed", &|| format!("{:?}", r.body_raw));
        ck(!r.body_text.contains("  ") && r.body_text == r.body_text.trim(), "body_text collapsed", &|| format!("{:?}", r.body_text));
        ck(!r.body_raw.is_empty() || r.body_text.is_empty(), "no body_text without body_raw", &|| format!("{:?}", r.body_text));
        pg.parent(c, "ref", r.id, (r.parent_type.as_deref(), r.parent_id), (r.start, r.end));
        ck(c.sections.get(r.section_id as usize).is_some(), "section_id", &|| r.section_id.to_string());
        ck(r.para_id.is_some() == r.para_offset.is_some(), "para_id with para_offset", &|| format!("{:?} {:?}", r.para_id, r.para_offset));
        ck(r.name.as_deref() != Some(""), "name not empty", &String::new);
        let same_key = |o: &RefRow| (&o.group, &o.name) == (&r.group, &r.name);
        match r.def_ref_id {
            Some(d) => {
                let d = &c.refs[d as usize];
                ck(has_body(d) && d.def_ref_id == Some(d.id), "def_ref_id is a definition", &|| d.id.to_string());
                ck(same_key(d), "def_ref_id has the same group and name", &|| d.id.to_string());
                // The first ref with a body for its (group, name).
                let earlier = c.refs[..d.id as usize].iter().any(|o| has_body(o) && same_key(o));
                ck(!earlier, "def_ref_id is the first definition", &|| d.id.to_string());
            }
            None => {
                let missed = r.name.is_some() && c.refs.iter().any(|o| has_body(o) && same_key(o));
                ck(!missed, "def_ref_id missing", &String::new);
            }
        }
        let want_uses = r.name.is_some() && matches!(r.kind.as_str(), "named" | "list_defined");
        ck(r.n_uses.is_some() == want_uses, "n_uses", &|| format!("{:?}", r.n_uses));
        ck(r.resolved == (has_body(r) || r.def_ref_id.is_some()), "resolved", &|| r.resolved.to_string());
        ck(r.fp.len() == 16, "fp", &|| r.fp.clone());
        n[2] += 1;
    }
    for (i, l) in c.ref_lists.iter().enumerate() {
        let ck = |ok: bool, field: &str, detail: &dyn Fn() -> String| pg.check(ok, "ref list", l.id, field, detail);
        ck(l.id == i as i32, "id", &|| format!("{} {i}", l.id));
        let s = pg.span(&chars, "ref list", l.id, (l.start, l.end, l.start_char, l.end_char));
        match (l.kind.as_str(), l.template_id) {
            ("tag", None) => {
                ck(s.get(..11).is_some_and(|h| h.eq_ignore_ascii_case("<references")), "span is a <references> tag", &|| format!("{s:?}"))
            }
            ("template", Some(t)) => {
                let t = &c.templates[t as usize];
                ck((t.start, t.end) == (l.start, l.end) && t.role == "ref_list", "template_id is its ref_list template", &|| t.id.to_string());
            }
            _ => ck(false, "kind with template_id", &|| format!("{} {:?}", l.kind, l.template_id)),
        }
        pg.parent(c, "ref list", l.id, (l.parent_type.as_deref(), l.parent_id), (l.start, l.end));
        ck(c.sections.get(l.section_id as usize).is_some(), "section_id", &|| l.section_id.to_string());
        let mut seen = std::collections::HashSet::new();
        let defs = c
            .refs
            .iter()
            .filter(|r| r.group == l.group && has_body(r) && r.def_ref_id.is_none_or(|d| d == r.id))
            .filter(|r| !short(r) || seen.insert(wikitext_components::fingerprint::norm(&r.body_text)))
            .count();
        ck(l.n_entries as usize == defs, "n_entries", &|| format!("{} {defs}", l.n_entries));
        n[3] += 1;
    }
    for (i, x) in c.citations.iter().enumerate() {
        let ck = |ok: bool, field: &str, detail: &dyn Fn() -> String| pg.check(ok, "citation", x.id, field, detail);
        ck(x.id == i as i32, "id", &|| format!("{} {i}", x.id));
        let s = pg.span(&chars, "citation", x.id, (x.start, x.end, x.start_char, x.end_char));
        ck(c.sections.get(x.section_id as usize).is_some(), "section_id", &|| x.section_id.to_string());
        ck(x.para_id.is_some() == x.para_offset.is_some(), "para_id with para_offset", &|| format!("{:?} {:?}", x.para_id, x.para_offset));
        ck(x.fp.len() == 16, "fp", &|| x.fp.clone());
        ck(x.host.is_none() || x.url.is_some(), "host needs url", &String::new);
        ck(x.registrable_domain.is_none() || x.host.is_some(), "registrable_domain needs host", &String::new);
        ck(x.harv_key.is_none(), "harv_key left to Task 17", &String::new);
        ck(matches!(x.context.as_str(), "in_ref" | "bibliography" | "other"), "context", &|| x.context.clone());
        ck((x.context == "in_ref") == x.ref_id.is_some(), "ref_id with context", &|| format!("{} {:?}", x.context, x.ref_id));
        let outer = x.ref_id.and_then(|r| c.refs.get(r as usize));
        ck(x.ref_id.is_none() || outer.is_some(), "ref_id exists", &|| format!("{:?}", x.ref_id));
        if let Some(r) = outer {
            ck(r.start <= x.start && x.end <= r.end, "span inside the ref", &|| format!("{}..{}", x.start, x.end));
        }
        match x.family.as_str() {
            "plain_text" => {
                // The ref's span, and the ref as parent.
                let r = outer.map(|r| (r.start, r.end, r.id));
                ck(r.is_some_and(|(a, b, _)| (a, b) == (x.start, x.end)), "plain_text has its ref's span", &String::new);
                let got = (x.parent_type.as_deref(), x.parent_id);
                ck(got == (Some("ref"), r.map(|r| r.2)), "plain_text parent is its ref", &|| format!("{got:?}"));
                ck(x.template_id.is_none(), "plain_text has no template", &String::new);
            }
            "bare_link" => {
                ck(s.starts_with('[') || s.contains("://") || s.starts_with("//"), "bare_link span is a link", &|| format!("{s:?}"));
                ck(x.url.is_some() && x.template_id.is_none() && x.params.is_empty(), "bare_link has a url, no template", &String::new);
                pg.parent(c, "citation", x.id, (x.parent_type.as_deref(), x.parent_id), (x.start, x.end));
            }
            _ => {
                let t = x.template_id.and_then(|t| c.templates.get(t as usize));
                ck(t.is_some_and(|t| (t.start, t.end) == (x.start, x.end) && t.role == "citation"), "template row has its template's span", &String::new);
                ck(t.is_some_and(|t| t.params == x.params && Some(&t.name) == x.template_name.as_ref()), "params and name are the template's", &String::new);
                ck(s.starts_with("{{"), "span starts with {{", &|| format!("{s:?}"));
                pg.parent(c, "citation", x.id, (x.parent_type.as_deref(), x.parent_id), (x.start, x.end));
            }
        }
        n[4] += 1;
    }
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
