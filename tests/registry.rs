mod common;
use common::{reg, ru, run_page};
use std::path::{Path, PathBuf};
use wikitext_components::registry::Registry;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("registry_test_{}_{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A registry dir with the given templates.toml and sections.toml.
fn dir_with(name: &str, templates: &str, sections: &str) -> PathBuf {
    let d = tmp(name);
    std::fs::write(d.join("templates.toml"), templates).unwrap();
    std::fs::write(d.join("sections.toml"), sections).unwrap();
    d
}

fn load_err(name: &str, templates: &str, sections: &str) -> String {
    let d = dir_with(name, templates, sections);
    let e = Registry::load(&d).unwrap_err();
    let _ = std::fs::remove_dir_all(&d);
    format!("{e:#}")
}

#[test]
fn name_variants_resolve() {
    let (s, r) = (ru(), reg());
    for raw in ["Cite_web", " cite web ", "Шаблон:Cite web", "Ш:cite web"] {
        assert_eq!(r.template(&s.template_name(raw), "wikipedia").unwrap().family.as_deref(), Some("web"));
    }
}

#[test]
fn roles() {
    let r = reg();
    assert_eq!(r.template("sfn", "wikipedia").unwrap().role, "footnote");
    assert_eq!(r.template("примечания", "ruwiki").unwrap().role, "ref_list");
    assert!(r.template("wayback", "wikipedia").unwrap().archive_wrapper);
    assert!(!r.template("cite web", "wikipedia").unwrap().archive_wrapper);
    assert_eq!(r.template("недоступная ссылка", "ruwiki").unwrap().role, "link_status");
    assert_eq!(r.template("dead link", "wikipedia").unwrap().role, "link_status");
    assert!(r.template("no such template", "wikipedia").is_none());
    let h = r.template("harvnb", "wikipedia").unwrap();
    assert_eq!((h.role.as_str(), h.family.as_deref()), ("citation", Some("short")));
    assert_eq!(r.template("sfn0", "ruwiki").unwrap().name, "harvnb");
    assert_eq!(r.template("sfn", "wikipedia").unwrap().family.as_deref(), Some("short"));
    assert!(r.template("!", "wikipedia").is_none()); // a magic word, not a template
}

#[test]
fn default_groups_are_set_only_where_the_corpora_show_them() {
    let r = reg();
    for (name, want) in [("efn", Some("К")), ("комментарии", Some("К")), ("sfn", None), ("комментарий", None), ("notelist", None), ("примечания", None), ("reflist", None)] {
        assert_eq!(r.template(name, "wikipedia").unwrap().default_group.as_deref(), want, "{name}");
    }
    let e = load_err("dg_role", "[[template]]\nname = \"a\"\nrole = \"other\"\ndefault_group = \"К\"\n", SECTIONS_OK);
    assert!(e.contains("default_group") && e.contains("\"a\""), "{e}");
    let e = load_err("dg_empty", "[[template]]\nname = \"a\"\nrole = \"footnote\"\ndefault_group = \" \"\n", SECTIONS_OK);
    assert!(e.contains("default_group"), "{e}");
}

#[test]
fn aliases_resolve_to_their_entry() {
    let (s, r) = (ru(), reg());
    let aliases = r.all_aliases();
    assert!(!aliases.is_empty());
    for (name, alias) in aliases {
        assert_eq!(
            r.template(&s.template_name(&alias), "wikipedia").map(|e| e as *const _),
            r.template(&name, "wikipedia").map(|e| e as *const _),
            "{alias}"
        );
        assert!(r.template(&name, "wikipedia").is_some(), "{name}");
    }
}

#[test]
fn section_roles() {
    let r = reg();
    assert_eq!(r.section_role("литература"), "bibliography");
    assert_eq!(r.section_role("история"), "content");
    for (t, role) in [("примечания", "notes"), ("комментарии", "notes"), ("источники", "bibliography"), ("ссылки", "external_links"), ("см. также", "see_also")] {
        assert_eq!(r.section_role(t), role, "{t}");
    }
    let c = run_page(&common::fixture("sections_paragraphs"), "wikipedia").components;
    assert_eq!(c.sections[3].role, "notes");
}

#[test]
fn every_required_citation_entry_has_fields() {
    let r = reg();
    for (name, family) in [
        ("cite web", "web"), ("cite news", "news"), ("cite book", "book"), ("cite journal", "journal"),
        ("cite encyclopedia", "encyclopedia"), ("книга", "book"), ("статья", "journal"),
        ("публикация", "other_template"), ("webarchive", "web"), ("wayback", "web"),
    ] {
        let e = r.template(name, "wikipedia").unwrap_or_else(|| panic!("{name}"));
        assert_eq!(e.role, "citation", "{name}");
        assert_eq!(e.family.as_deref(), Some(family), "{name}");
        assert!(e.fields.contains_key("archive_url"), "{name}");
    }
    let w = r.template("cite web", "wikipedia").unwrap();
    assert_eq!(w.fields["url"], ["url"]);
    assert_eq!(w.fields["access_date_raw"][0], "access-date");
    let k = r.template("книга", "wikipedia").unwrap();
    assert_eq!(k.fields["title"][0], "заглавие");
    assert_eq!(r.template("wayback", "ruwiki").unwrap().fields["archive_date_raw"], ["date"]);
    let sfn = r.template("sfn", "wikipedia").unwrap();
    assert!(sfn.fields.contains_key("harv_names") && sfn.fields.contains_key("harv_ref"));
    for n in ["efn", "комментарий"] {
        assert_eq!(r.template(n, "wikipedia").unwrap().role, "footnote");
    }
    for n in ["примечания", "reflist", "notelist", "список примечаний", "комментарии"] {
        assert_eq!(r.template(n, "wikipedia").unwrap().role, "ref_list", "{n}");
    }
}

#[test]
fn hash_is_sha256_of_both_files() {
    use sha2::{Digest, Sha256};
    let mut bytes = std::fs::read("config/registry/templates.toml").unwrap();
    bytes.extend(std::fs::read("config/registry/sections.toml").unwrap());
    let want: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
    let h = reg().hash();
    assert_eq!(h.len(), 64);
    assert_eq!(h, want);
}

const SECTIONS_OK: &str = "[[section]]\nrole = \"notes\"\ntitles = [\"примечания\"]\n";

#[test]
fn site_specific_entry_wins_and_scopes() {
    let t = r#"
[[template]]
name = "x"
role = "other"

[[template]]
name = "x"
role = "citation"
site = "ruwiki"

[[template]]
name = "only wp"
aliases = ["Only_WP alias"]
role = "navigation"
site = "wikipedia"
"#;
    let d = dir_with("site", t, SECTIONS_OK);
    let r = Registry::load(&d).unwrap();
    let _ = std::fs::remove_dir_all(&d);
    assert_eq!(r.template("x", "ruwiki").unwrap().role, "citation");
    assert_eq!(r.template("x", "wikipedia").unwrap().role, "other");
    assert_eq!(r.template("only wp", "wikipedia").unwrap().role, "navigation");
    assert!(r.template("only wp", "ruwiki").is_none());
    assert!(r.template("only WP alias", "wikipedia").is_some());
}

#[test]
fn names_and_aliases_are_normalised_on_load() {
    let t = "[[template]]\nname = \"Cite_Foo  bar\"\naliases = [\"Ал_Ис\"]\nrole = \"other\"\n";
    let d = dir_with("norm", t, SECTIONS_OK);
    let r = Registry::load(&d).unwrap();
    let _ = std::fs::remove_dir_all(&d);
    assert!(r.template("cite Foo bar", "wikipedia").is_some());
    assert!(r.template("ал Ис", "wikipedia").is_some());
}

#[test]
fn namespace_prefixed_names_and_aliases_are_rejected() {
    let e = load_err("pfx_name", "[[template]]\nname = \"Шаблон:Cite web\"\nrole = \"other\"\n", SECTIONS_OK);
    assert!(e.contains("Шаблон:Cite web"), "{e}");
    let e = load_err("pfx_alias", "[[template]]\nname = \"a\"\naliases = [\"Ш:b\"]\nrole = \"other\"\n", SECTIONS_OK);
    assert!(e.contains("\"a\"") && e.contains("Ш:b"), "{e}");
}

#[test]
fn duplicate_names_are_errors_naming_both_entries() {
    let t = "[[template]]\nname = \"a\"\naliases = [\"shared\"]\nrole = \"other\"\n\n[[template]]\nname = \"b\"\naliases = [\"Shared\"]\nrole = \"other\"\n";
    let e = load_err("dup", t, SECTIONS_OK);
    assert!(e.contains("\"a\"") && e.contains("\"b\"") && e.contains("shared"), "{e}");
    // the same name for different sites is not a duplicate; for overlapping scopes it is
    let t = "[[template]]\nname = \"a\"\nrole = \"other\"\n\n[[template]]\nname = \"a\"\nrole = \"other\"\nsite = \"ruwiki\"\n";
    let d = dir_with("nodup", t, SECTIONS_OK);
    assert!(Registry::load(&d).is_ok());
    let _ = std::fs::remove_dir_all(&d);
    let t = "[[template]]\nname = \"a\"\nrole = \"other\"\nsite = \"ruwiki\"\n\n[[template]]\nname = \"a\"\nrole = \"other\"\nsite = \"ruwiki\"\n";
    assert!(load_err("dup2", t, SECTIONS_OK).contains("\"a\""));
}

#[test]
fn invalid_content_is_rejected() {
    let ok = |extra: &str| format!("[[template]]\nname = \"a\"\nrole = \"other\"\n{extra}");
    assert!(load_err("role", "[[template]]\nname = \"a\"\nrole = \"bogus\"\n", SECTIONS_OK).contains("bogus"));
    assert!(load_err("field", &ok("[template.fields]\nbogus_key = [\"x\"]\n"), SECTIONS_OK).contains("bogus_key"));
    assert!(load_err("unk", &ok("colour = \"red\"\n"), SECTIONS_OK).contains("colour"));
    assert!(load_err("site", &ok("site = \"enwiki\"\n"), SECTIONS_OK).contains("enwiki"));
    assert!(load_err("unk2", "[[template]]\nname = \"a\"\nrole = \"other\"\n", "[[section]]\nrole = \"notes\"\ntitles = []\nextra = 1\n").contains("extra"));
    assert!(load_err("secdup", "", "[[section]]\nrole = \"notes\"\ntitles = [\"Лит\"]\n[[section]]\nrole = \"see_also\"\ntitles = [\"лит\"]\n").contains("лит"));
}

#[test]
fn missing_files_are_errors_with_the_path() {
    let d = tmp("missing");
    let e = format!("{:#}", Registry::load(&d).unwrap_err());
    let _ = std::fs::remove_dir_all(&d);
    assert!(e.contains("templates.toml"), "{e}");
    let e = format!("{:#}", Registry::load(Path::new("no/such/dir")).unwrap_err());
    assert!(e.contains("no/such/dir"), "{e}");
}

#[test]
fn walk_roles_come_from_the_registry() {
    let c = run_page("{{cite web|url=http://x.org|title=T}} {{unknown|a}}", "wikipedia").components;
    assert_eq!(c.templates[0].role, "citation");
    assert_eq!(c.templates[1].role, "other");
}

#[test]
fn in_ref_coverage_counts_templates_inside_refs() {
    // Inside refs: {{нечто}} twice (nested), {{cite web}}, {{sfn}} (a footnote template inside a ref), and what
    // an {{efn}} body holds. Not inside: the {{efn}}s themselves, {{вне}}, and a template in a ref list.
    // Not templates (kind variable, parser function): {{!}}, {{#if:}}.
    let text = "А<ref>{{cite web|url=x}} {{нечто|{{нечто}}}} {{sfn|Б|1990}} {{!}} {{#if:a|b}}</ref>{{efn|{{нечто внутри}}}}{{efn|y}} {{вне}}\n<references>{{cite web|url=y}}</references>";
    let outs = vec![run_page(text, "wikipedia")];
    let cov = reg().in_ref_coverage(&outs, "wikipedia");
    let want: Vec<(String, usize, bool)> = vec![
        ("нечто".into(), 2, false),
        ("cite web".into(), 1, true),
        ("sfn".into(), 1, true),
        ("нечто внутри".into(), 1, false),
    ];
    assert_eq!(cov, want);
    assert!(reg().in_ref_coverage(&[], "wikipedia").is_empty());
}

/// Parses every row on `site` and requires that at most 1% of the template occurrences inside refs have no
/// registry entry. Prints the shares and the most frequent in-ref names (test output only, never committed).
/// The share of in-ref template occurrences without a registry entry, at most 1%. For the private corpus the
/// output holds counts only: the names, which come from Ruwiki markup, go to a file under the target
/// directory (never into git), for curating the registry.
fn assert_coverage(rows: Vec<wikitext_components::corpus::CorpusRow>, site: &str, private: bool) {
    let outputs = common::run_rows(&rows, site);
    assert!(outputs.iter().all(|o| o.page.status == "ok"), "a page did not parse");
    let cov = reg().in_ref_coverage(&outputs, site);
    let total: usize = cov.iter().map(|c| c.1).sum();
    let unclassified: Vec<&(String, usize, bool)> = cov.iter().filter(|c| !c.2).collect();
    let missing: usize = unclassified.iter().map(|c| c.1).sum();
    let share = missing as f64 / total.max(1) as f64;
    eprintln!(
        "{site}: {} versions, {total} in-ref template occurrences of {} names, {missing} unclassified ({:.2}%) of {} names",
        rows.len(),
        cov.len(),
        100.0 * share,
        unclassified.len()
    );
    for (name, n, known) in cov.iter().take(20) {
        let name = if private { "(name withheld)" } else { name.as_str() };
        eprintln!("  top in-ref: {n:>6} {name} {}", if *known { "" } else { "(unclassified)" });
    }
    if private {
        let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("in_ref_coverage_{site}.txt"));
        let lines: String = cov.iter().map(|(name, n, known)| format!("{n}\t{known}\t{name}\n")).collect();
        std::fs::write(&path, lines).unwrap();
        eprintln!("  names: {}", path.display());
    }
    assert!(total > 0, "no templates inside refs");
    if share > 0.01 {
        if !private {
            for (name, n, _) in unclassified.iter().take(30) {
                eprintln!("  unclassified: {n:>6} {name}");
            }
        }
        panic!("{site}: {:.2}% of in-ref template occurrences are unclassified (limit 1%)", 100.0 * share);
    }
}

#[test]
fn in_ref_templates_are_classified_public() {
    assert_coverage(common::public_corpus(), "wikipedia", false);
}

#[test]
fn in_ref_templates_are_classified_private() {
    let Some(rows) = common::private_corpus("in_ref_templates_are_classified_private") else { return };
    assert_coverage(rows, "ruwiki", true);
}
