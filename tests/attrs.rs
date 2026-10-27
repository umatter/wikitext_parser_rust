use wikitext_components::attrs::{open_tag, self_closing, tag_attrs};

fn get(v: Vec<(String, String)>, k: &str) -> Option<String> {
    v.into_iter().find(|(n, _)| n == k).map(|(_, v)| v)
}

#[test]
fn ref_name_variants_are_one_name() {
    for t in [r#"<ref name="a">"#, "<ref name=a>", "<ref name = 'a'>", r#"<ref name=" a ">"#, r#"<ref NAME="a"/>"#] {
        assert_eq!(get(tag_attrs(open_tag(t)), "name"), Some("a".into()), "{t}");
    }
}

#[test]
fn group_and_self_closing() {
    let t = r#"<ref group="К" name=b />"#;
    assert_eq!(get(tag_attrs(open_tag(t)), "group"), Some("К".into()));
    assert!(self_closing(open_tag(t)));
    assert!(!self_closing(open_tag(r#"<ref name="a>b">x</ref>"#)));
}

#[test]
fn edge_cases_never_panic() {
    for t in ["", "<", ">", "<ref", "<ref name=\"a", "<ref name='", "<ref=", "ж<ref>", "<ref name=ж/>", "<ref ж=ж\u{a0}x/ >", "<ref \"", "</", "<ref name=&amp"] {
        let o = open_tag(t);
        let a = tag_attrs(o);
        let _ = self_closing(o);
        let _ = a;
    }
    assert_eq!(open_tag("no tag"), "no tag");
    assert_eq!(open_tag("<ref name=\"a"), "<ref name=\"a");
    assert_eq!(open_tag("<ref name=a>text>"), "<ref name=a>");
    let t = "<ref name=a/>";
    assert_eq!(get(tag_attrs(open_tag(t)), "name"), Some("a".into()));
    assert!(self_closing(open_tag(t)));
    assert_eq!(tag_attrs("<ref follow>"), vec![("follow".to_string(), String::new())]);
    assert_eq!(
        tag_attrs("<ref name=a name=b>"),
        vec![("name".to_string(), "a".to_string()), ("name".to_string(), "b".to_string())]
    );
    assert_eq!(get(tag_attrs("<ref name=\"a&amp;\n  b\">"), "name"), Some("a& b".into()));
    assert_eq!(get(tag_attrs("<ref name=\"unterminated>x"), "name"), Some("unterminated>x".into()));
}

#[test]
fn edge_values_are_asserted() {
    let t = "<ref name=ж/>";
    assert_eq!(get(tag_attrs(open_tag(t)), "name"), Some("ж".into()));
    assert!(self_closing(open_tag(t)));
    assert!(!self_closing(open_tag(r#"<ref name="a/">"#)));
    assert!(self_closing(open_tag("<ref/>")));
    let t = "<ref\tname=a\ngroup=b\t>";
    let a = tag_attrs(open_tag(t));
    assert_eq!(get(a.clone(), "name"), Some("a".into()));
    assert_eq!(get(a, "group"), Some("b".into()));
    assert_eq!(get(tag_attrs("<ref name=\"\u{a0}a\u{a0}\u{a0}b \u{a0}\">"), "name"), Some("\u{a0}a\u{a0}\u{a0}b \u{a0}".into()));
    let t = "<ref name=O'Brien>x</ref>";
    assert_eq!(open_tag(t), "<ref name=O'Brien>");
    assert_eq!(get(tag_attrs(open_tag(t)), "name"), Some("O'Brien".into()));
    assert!(!self_closing(open_tag(t)));
    let t = "<ref name=O'Brien group=\"x\">y'z</ref>";
    assert_eq!(open_tag(t), "<ref name=O'Brien group=\"x\">");
    assert_eq!(open_tag("<ref name = 'a>b'>x"), "<ref name = 'a>b'>");
    assert_eq!(get(tag_attrs("<ref name=O'Brien/>"), "name"), Some("O'Brien".into()));
    assert!(self_closing("<ref name=O'Brien/>"));
}
