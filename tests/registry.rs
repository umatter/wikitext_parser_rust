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
    let t = "[[template]]\nname = \"Cite_Foo  bar\"\naliases = [\"Шаблон:Ал Ис\"]\nrole = \"other\"\n";
    let d = dir_with("norm", t, SECTIONS_OK);
    let r = Registry::load(&d).unwrap();
    let _ = std::fs::remove_dir_all(&d);
    assert!(r.template("cite Foo bar", "wikipedia").is_some());
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
