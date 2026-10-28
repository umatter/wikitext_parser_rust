mod common;

use common::run_page;
use wikitext_components::walk::citeref;

#[test]
fn sfn_resolution() {
    let c = run_page(&common::fixture("sfn"), "wikipedia").components;
    let r: Vec<_> = c.refs.iter().filter(|r| r.kind == "template").map(|r| (r.harv_key.as_deref(), r.cites_citation_id, r.resolved)).collect();
    assert_eq!(r, vec![(Some("CITEREFИванов1990"),Some(0),true),(Some("CITEREFПетровСидоров2001"),Some(1),true),(Some("CITEREFНикто1900"),None,false)]);
    assert_eq!(c.citations.len(), 2); assert!(c.citations.iter().all(|x| x.context == "bibliography" && x.ref_id.is_none()));
    let k: Vec<_> = c.citations.iter().map(|x| x.harv_key.as_deref()).collect();
    assert_eq!(k, vec![Some("CITEREFИванов1990"), Some("CITEREFПетровСидоров2001")]);
}

#[test]
fn citeref_rule() {
    assert_eq!(citeref(&[" Ван  Дейк ", "Бор"], "1990a"), "CITEREFВан_ДейкБор1990a");
    assert_eq!(citeref(&["Иванов"], ""), "CITEREFИванов");
}

fn keys(text: &str) -> Vec<(Option<String>, Option<i32>, bool)> {
    let c = run_page(text, "wikipedia").components;
    c.refs.iter().map(|r| (r.harv_key.clone(), r.cites_citation_id, r.resolved)).collect()
}

#[test]
fn sfn_rules() {
    // Named parameters are ignored; the letter suffix is kept; aliases count.
    let t = "A{{sfn|Иванов|1990a|с=5}} B{{sfnp|Иванов|1990б}}\n* {{cite book |last1=Иванов |year=1990 |date=1990a}}\n* {{книга |автор=Иванов И. И. |год=1990 |ref=Иванов}}\n";
    let v = keys(t);
    assert_eq!(v[0].0.as_deref(), Some("CITEREFИванов1990a"));
    assert_eq!(v[1].0.as_deref(), Some("CITEREFИванов1990б"));
    // ref= gives the key by itself, `harv` means names and year; an empty ref= is absent.
    let t = "A{{sfn|Иванов|1990}}\n* {{книга |автор=Иванов И. И. |год=1990 |ref=harv}}\n* {{книга |автор=Другой |год=1990 |ref=}}\n";
    assert_eq!(keys(t)[0], (Some("CITEREFИванов1990".into()), Some(0), true));
    let c = run_page("* {{книга |автор=Другой |год=1990 |ref=}}\n* {{книга |автор=Автор |ref=Метка}}", "wikipedia").components;
    assert_eq!(c.citations[0].harv_key.as_deref(), Some("CITEREFДругой1990"));
    assert_eq!(c.citations[1].harv_key.as_deref(), Some("CITEREFМетка"));
    // First citation by id wins; a citation without year and ref has no key.
    let c = run_page("A{{sfn|Иванов|1990}}\n* {{cite book |last1=Иванов |year=1990}}\n* {{cite book |last1=Иванов |year=1990}}\n* {{cite book |last1=Иванов}}", "wikipedia").components;
    assert_eq!(c.refs[0].cites_citation_id, Some(0));
    assert_eq!(c.citations[2].harv_key, None);
}

#[test]
fn harvnb_refs() {
    let c = run_page("A<ref>{{harvnb|Иванов|1990|p=5}}</ref> B<ref>{{harvnb|Никто|1900}} {{harvnb|Иванов|1990}}</ref>\n* {{cite book |last1=Иванов |year=1990}}", "wikipedia").components;
    let r: Vec<_> = c.refs.iter().map(|r| (r.harv_key.as_deref(), r.cites_citation_id, r.resolved)).collect();
    assert_eq!(r, vec![(Some("CITEREFИванов1990"), Some(3), true), (Some("CITEREFНикто1900"), None, false)]);
    assert_eq!(c.citations[0].harv_key.as_deref(), Some("CITEREFИванов1990"));
    // Short citations are never targets.
    let c = run_page("A{{sfn|Иванов|1990}}<ref>{{harvnb|Иванов|1990}}</ref>", "wikipedia").components;
    assert!(c.refs.iter().all(|r| r.cites_citation_id.is_none() && !r.resolved));
}

#[test]
#[ignore]
fn sfn_resolution_rate() {
    // Public corpus only. Prints the unresolved calls so each can be classified by hand.
    let (mut total, mut ok) = (0, 0);
    for r in common::public_corpus() {
        let c = run_page(&r.wikitext, "wikipedia").components;
        for x in c.refs.iter().filter(|x| x.kind == "template") {
            let t = &r.wikitext[x.start as usize..x.end as usize];
            if !t.to_lowercase().starts_with("{{sfn") {
                continue;
            }
            total += 1;
            ok += usize::from(x.resolved);
            if !x.resolved {
                eprintln!("UNRESOLVED {} {} {:?} {}", r.page_id, r.version, x.harv_key, t);
            }
        }
    }
    eprintln!("sfn resolved {ok}/{total}");
    assert!(ok * 100 >= total * 90, "{ok}/{total}");
}

#[test]
fn date_as_year_and_year_letters() {
    // Public page 4589444: {{публикация}} with `год = 04.08.2009` and `ref = Кавказский Узел`, cited as
    // {{sfn|Кавказский Узел|04.08.2009}}.
    let t = "A{{sfn|Кавказский Узел|04.08.2009|с=}}\n* {{публикация |заглавие=Т |год = 04.08.2009 |ref = Кавказский Узел}}\n";
    let c = run_page(t, "wikipedia").components;
    assert_eq!(c.citations[0].harv_key.as_deref(), Some("CITEREFКавказский_Узел04.08.2009"));
    assert!(c.refs[0].resolved);
    // The letter of a year parameter is kept; a date never gives one.
    let c = run_page("* {{cite book |last1=Иванов |date=1990a}}\n* {{книга |автор=Петров П. |год=2001б}}\n* {{cite book |last1=Сидоров |date=in 1990 ago}}", "wikipedia").components;
    let k: Vec<_> = c.citations.iter().map(|x| x.harv_key.as_deref()).collect();
    assert_eq!(k, vec![Some("CITEREFИванов1990"), Some("CITEREFПетров2001б"), Some("CITEREFСидоров1990")]);
}

#[test]
fn year_letter_only_from_the_year_field() {
    let c = run_page(
        "* {{книга |автор=А |год=2009г.}}\n* {{cite book |last1=Б |date=10.05.2009г.}}\n* {{cite book |last1=В |date=1990s}}\n* {{cite book |last1=Г |year=1990а}}",
        "wikipedia",
    )
    .components;
    let k: Vec<_> = c.citations.iter().map(|x| x.harv_key.as_deref()).collect();
    assert_eq!(k, vec![Some("CITEREFА2009"), Some("CITEREFБ2009"), Some("CITEREFВ1990"), Some("CITEREFГ1990а")]);
}

#[test]
fn names_form_with_a_date_year_and_a_year_only_sfn() {
    let c = run_page("A{{sfn|Иванов|04.08.2009}} B{{sfn|1990}}\n* {{cite book |last=Иванов |last1=Иванов |year=04.08.2009}}", "wikipedia").components;
    assert_eq!(c.citations[0].harv_key.as_deref(), Some("CITEREFИванов04.08.2009"));
    assert_eq!((c.refs[0].harv_key.as_deref(), c.refs[0].cites_citation_id, c.refs[0].resolved), (Some("CITEREFИванов04.08.2009"), Some(0), true));
    assert_eq!((c.refs[1].harv_key.as_deref(), c.refs[1].cites_citation_id, c.refs[1].resolved), (None, None, false));
}
