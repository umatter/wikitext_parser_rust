//! The corpus invariant checker: spans, char offsets, ids, parents, depths and the ref, ref-list and
//! citation rules that hold on every page, whichever path built its rows (the walk or the fallback scan).

use wikitext_components::model::{Components, RefRow};
use wikitext_components::registry::Registry;
use wikitext_components::spans::CharIndex;

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
pub struct Page<'a> {
    pub index: usize,
    pub private: bool,
    pub text: &'a str,
}

impl Page<'_> {
    pub fn check(&self, ok: bool, table: &str, id: i32, field: &str, detail: impl FnOnce() -> String) {
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

pub fn check_page(pg: &Page, c: &Components, reg: &Registry, site: &str, n: &mut [usize; 5]) {
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
        // R50: a short footnote ({{sfn}} and aliases) and a ref holding a short citation are resolved only
        // through a found citation, key or no key; the others keep the body-or-definition meaning.
        let short = s.to_lowercase().starts_with("{{sfn")
            || c.citations.iter().any(|x| x.ref_id == Some(r.id) && x.family == "short");
        let want_resolved = match short {
            true => r.cites_citation_id.is_some(),
            false => has_body(r) || r.def_ref_id.is_some(),
        };
        ck(r.resolved == want_resolved, "resolved", &|| r.resolved.to_string());
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
        ck(x.harv_key.as_deref().is_none_or(|k| k.starts_with("CITEREF") && k.len() > 7), "harv_key shape", &String::new);
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
