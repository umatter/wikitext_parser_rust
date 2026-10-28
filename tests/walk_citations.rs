mod common;

use common::run_page;
use wikitext_components::fingerprint::{fp, norm};
use wikitext_components::model::CitationRow;

fn cites(text: &str) -> Vec<CitationRow> {
    let r = run_page(text, "wikipedia");
    assert_eq!(r.page.status, "ok", "{:?}", r.page.status_detail);
    r.components.citations
}

#[test]
fn web_and_plain() {
    let c = run_page(&common::fixture("refs_basic"), "wikipedia").components;
    assert_eq!(
        c.citations.iter().map(|x| (x.family.as_str(), x.ref_id)).collect::<Vec<_>>(),
        vec![("plain_text", Some(0)), ("web", Some(1))]
    );
    let w = &c.citations[1];
    assert_eq!(
        (w.url.as_deref(), w.host.as_deref(), w.title.as_deref(), w.work.as_deref(), w.year),
        (Some("https://www.kremlin.ru/events/1"), Some("kremlin.ru"), Some("Событие"), Some("Kremlin"), Some(2020))
    );
    assert_eq!(c.citations[0].text.as_deref(), Some("Простой источник."));
    assert_eq!(c.citations[1].registrable_domain.as_deref(), Some("kremlin.ru"));
}

#[test]
fn book_from_list_defined() {
    let c = run_page(&common::fixture("refs_groups_listdefined"), "wikipedia").components;
    let b = c.citations.iter().find(|x| x.family == "book").unwrap();
    assert_eq!(
        (b.authors.clone(), b.title.as_deref(), b.publisher.as_deref(), b.year, b.isbn.as_deref()),
        (vec!["Иванов И. И.".to_string()], Some("Книга"), Some("Наука"), Some(1990), Some("978-5-02-000000-0"))
    );
    let t = c.citations.iter().find(|x| x.family == "bare_link").unwrap();
    assert_eq!(t.host.as_deref(), Some("tass.ru"));
    assert_eq!(t.title.as_deref(), Some("ТАСС"));
}

#[test]
fn archives_and_dead_links() {
    let c = run_page(&common::fixture("archive_deadlink"), "wikipedia").components;
    let v: Vec<_> = c
        .citations
        .iter()
        .map(|x| (x.family.as_str(), x.url.as_deref().unwrap(), x.host.as_deref().unwrap(), x.archive_url.is_some(), x.dead_link))
        .collect();
    assert_eq!(
        v,
        vec![
            ("web", "https://tass.ru/politika/1", "tass.ru", true, false),
            ("news", "https://ria.ru/2020/a.html", "ria.ru", true, false),
            ("bare_link", "https://www.bbc.co.uk/news/1", "bbc.co.uk", true, false),
            ("journal", "http://old.example.org/x", "old.example.org", false, true),
            ("web", "http://gov.ru/a", "gov.ru", true, false)
        ]
    );
    assert_eq!(c.citations[4].archive_url.as_deref(), Some("https://web.archive.org/web/20150101/http://gov.ru/a"));
    assert_eq!(c.citations[1].archive_date_raw.as_deref(), Some("2021-01-01"));
    assert_eq!(c.citations[2].archive_date_raw.as_deref(), Some("2019"));
}

#[test]
fn efn_body_citation() {
    let c = run_page(&common::fixture("efn"), "wikipedia").components;
    assert_eq!(c.refs[0].kind, "template");
    assert_eq!(c.citations.len(), 1);
    assert_eq!((c.citations[0].ref_id, c.citations[0].host.as_deref()), (Some(0), Some("a.ru")));
}

#[test]
fn efn_without_citation_is_plain_text() {
    let c = cites("А{{efn|Просто пояснение}}.");
    assert_eq!(c.len(), 1);
    assert_eq!((c[0].family.as_str(), c[0].text.as_deref(), c[0].ref_id), ("plain_text", Some("Просто пояснение"), Some(0)));
}

#[test]
fn every_ref_with_body_has_a_citation() {
    let pages = common::public_corpus();
    let (mut refs, mut stale) = (0, 0);
    for r in pages {
        let c = run_page(&r.wikitext, "wikipedia").components;
        for rf in c.refs.iter().filter(|r| matches!(r.kind.as_str(), "inline" | "named" | "list_defined") && !r.body_raw.is_empty()) {
            refs += 1;
            if !c.citations.iter().any(|x| x.ref_id == Some(rf.id)) {
                stale += 1;
            }
        }
    }
    assert!(refs > 1000, "{refs}");
    assert_eq!(stale, 0);
}

#[test]
fn sfn_has_no_rows_but_harvnb_has_one() {
    let c = cites("А{{sfn|Иванов|1990|с=5}} Б{{sfnp|Петров|2001}}");
    assert!(c.is_empty(), "{c:?}");
    let c = cites("А<ref>{{harvnb|Иванов|1990}}</ref>");
    assert_eq!(c.len(), 1);
    assert_eq!((c[0].family.as_str(), c[0].template_name.as_deref(), c[0].url.clone()), ("short", Some("harvnb"), None));
    assert_eq!(c[0].params.len(), 2);
    assert_eq!(c[0].harv_key.as_deref(), Some("CITEREFИванов1990"));
}

#[test]
fn bibliography_and_other_contexts() {
    let t = "Лид.\n== Литература ==\n* {{cite book |title=К |last=Иванов |first=И |year=1990}}\n== Прочее ==\n{{cite web |url=http://a.ru/x |title=Т}}\n";
    let c = cites(t);
    assert_eq!(c.iter().map(|x| (x.context.as_str(), x.ref_id, x.family.as_str())).collect::<Vec<_>>(), vec![("bibliography", None, "book"), ("other", None, "web")]);
    assert_eq!(c[0].authors, vec!["Иванов, И".to_string()]);
    assert_eq!((c[0].parent_type.clone(), c[0].parent_id), (None, None));
    let ex = cites("== Ссылки ==\n* [http://a.ru x]\n");
    assert!(ex.is_empty(), "bare links outside refs are no citations");
}

#[test]
fn spans_and_parents() {
    let text = "Т<ref>[http://a.ru/p Метка] {{cite web |url=http://b.ru}}</ref> и<ref>Просто</ref>";
    let c = run_page(text, "wikipedia").components;
    let v: Vec<_> = c.citations.iter().map(|x| (x.family.as_str(), &text[x.start as usize..x.end as usize], x.parent_type.as_deref(), x.parent_id)).collect();
    assert_eq!(
        v,
        vec![
            ("bare_link", "[http://a.ru/p Метка]", Some("ref"), Some(0)),
            ("web", "{{cite web |url=http://b.ru}}", Some("ref"), Some(0)),
            ("plain_text", "<ref>Просто</ref>", Some("ref"), Some(1)),
        ]
    );
    assert_eq!(c.citations.iter().map(|x| x.id).collect::<Vec<_>>(), vec![0, 1, 2]);
    assert!(c.citations.iter().all(|x| x.para_id == Some(0) && x.para_offset.is_some()));
    assert_eq!(c.citations[0].title.as_deref(), Some("Метка"));
}

#[test]
fn field_reading_rules() {
    let c = cites("<ref>{{cite web |url=http://x.ru/a?b=1&amp;c=2 |title= |trans-title=Перевод |last1=Ли |first1=А |first2=Б |author3=Тре |date=12 мая 1999 |accessdate=2020}}</ref>");
    let w = &c[0];
    assert_eq!(w.url.as_deref(), Some("http://x.ru/a?b=1&c=2"));
    assert_eq!(w.title.as_deref(), Some("Перевод"));
    assert_eq!(w.authors, vec!["Ли, А".to_string(), "Тре".to_string()]);
    assert_eq!((w.year, w.date_raw.as_deref(), w.access_date_raw.as_deref()), (Some(1999), Some("12 мая 1999"), Some("2020")));
    // Names are case-sensitive.
    let c = cites("<ref>{{cite web |URL=http://x.ru |title=T}}</ref>");
    assert_eq!(c[0].url, None);
    // The dead-link template marks every row of its body only.
    let c = cites("<ref>{{cite web |url=http://x.ru}} {{dead link}}</ref><ref>{{cite web |url=http://y.ru}}</ref>");
    assert_eq!(c.iter().map(|x| x.dead_link).collect::<Vec<_>>(), vec![true, false]);
}

#[test]
fn archive_wrapper_in_a_parameter_merges_into_the_template() {
    let c = cites("<ref>{{cite web |url=http://x.ru/a |title={{webarchive |url=http://x.ru/a |date=20100101}}}}</ref>");
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].archive_url.as_deref(), Some("https://web.archive.org/web/20100101/http://x.ru/a"));
    assert_eq!(c[0].archive_date_raw.as_deref(), Some("20100101"));
}

#[test]
fn fingerprint_rule() {
    let c = cites("<ref>{{cite web |url=http://x.ru/a |title=Т}}</ref><ref>{{cite book |title=Книга  Х |author=И}}</ref><ref>Просто  текст</ref>");
    assert_eq!(c[0].fp, fp("web\u{1f}http://x.ru/a"));
    assert_eq!(c[1].fp, fp(&format!("book\u{1f}{}", norm("Книга Х И"))));
    assert_eq!(c[2].fp, fp("plain_text\u{1f}просто текст"));
}

#[test]
fn malformed_citations_do_not_panic() {
    let texts = [
        "<ref>{{cite web}}</ref>",
        "<ref>{{cite web |url=}}</ref>",
        "<ref>{{cite web |url=:::// |title=x}}</ref>",
        "<ref>{{cite web |url=[http://a.ru b]}}</ref>",
        "<ref>{{webarchive}}</ref>",
        "<ref>{{webarchive |url=garbage |date=}}</ref>",
        "{{cite web|<ref>{{cite book|<ref>x</ref>}}</ref>}}",
        "<ref><ref>вложенный</ref>[http://a.ru]</ref>",
        "<ref>[http://</ref>",
        "{{dead link}}{{webarchive|url=http://a.ru|date=1}}",
        "<ref>{{sfn|{{cite web|url=http://a.ru}}}}</ref>",
        "{{efn|{{cite web|url=http://a.ru|last=Z}}}}{{cite web|first1=Q}}",
        "<ref>{{cite web |url=//a.ru/x |year=99999}}</ref>",
    ];
    for t in texts {
        let r = run_page(t, "wikipedia");
        assert_eq!(r.page.status, "ok", "{t}");
        for (i, c) in r.components.citations.iter().enumerate() {
            assert_eq!(c.id, i as i32, "{t}");
        }
    }
    let c = cites("<ref>{{cite web}}</ref>");
    assert_eq!((c.len(), c[0].family.as_str(), c[0].url.clone()), (1, "web", None));
    let c = cites("<ref>{{sfn|{{cite web|url=http://a.ru}}}}</ref>");
    assert_eq!(c.iter().map(|x| x.family.as_str()).collect::<Vec<_>>(), vec!["plain_text"], "the arguments of a short footnote are not sources");
}

#[test]
fn public_corpus_counts() {
    let (mut by_family, mut in_ref, mut with_host) = (std::collections::BTreeMap::<String, usize>::new(), 0usize, 0usize);
    for r in common::public_corpus() {
        for c in run_page(&r.wikitext, "wikipedia").components.citations {
            *by_family.entry(c.family.clone()).or_default() += 1;
            if c.context == "in_ref" {
                in_ref += 1;
                with_host += usize::from(c.host.is_some());
            }
        }
    }
    eprintln!("public citations by family: {by_family:?}; in_ref {in_ref}, with host {with_host}");
    assert!(in_ref > 1000);
}

#[test]
fn page_level_bodies_are_paragraphs_not_offsets() {
    // Text between items does not split the body.
    let c = cites("* {{cite web|url=http://b.ru}}. {{dead link}}");
    assert_eq!(c.iter().map(|x| x.dead_link).collect::<Vec<_>>(), vec![true]);
    let c = cites("* {{cite web|url=http://c.ru}} текст {{webarchive|url=https://web.archive.org/web/2012/http://c.ru|date=2012}}");
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].archive_url.as_deref(), Some("https://web.archive.org/web/2012/http://c.ru"));
    // Two list items are two bodies.
    let c = cites("* {{cite web|url=http://a.ru}}\n* {{cite web|url=http://b.ru}} {{dead link}}");
    assert_eq!(c.iter().map(|x| x.dead_link).collect::<Vec<_>>(), vec![false, true]);
    let c = cites("* {{cite web|url=http://a.ru}}\n* {{webarchive|url=http://b.ru|date=2012}}");
    assert_eq!(c.len(), 2);
}

#[test]
fn wrappers_never_attach_another_pages_snapshot() {
    let wrap = "{{webarchive|url=https://web.archive.org/web/2012/http://g.ru|date=2012}}";
    let c = cites(&format!("<ref>{{{{cite web|url=http://f.ru|archive-url=https://web.archive.org/web/2011/http://f.ru}}}} {wrap}</ref>"));
    assert_eq!(c.iter().map(|x| x.url.as_deref().unwrap()).collect::<Vec<_>>(), vec!["http://f.ru", "http://g.ru"]);
    assert_eq!(c[0].archive_url.as_deref(), Some("https://web.archive.org/web/2011/http://f.ru"));
    assert_eq!(c[1].archive_url.as_deref(), Some("https://web.archive.org/web/2012/http://g.ru"));
    // Same original, row already archived: the row keeps its own archive and date.
    let c = cites("<ref>{{cite web|url=http://f.ru|archive-url=https://web.archive.org/web/2011/http://f.ru|archive-date=2011}} {{webarchive|url=https://web.archive.org/web/2012/http://f.ru|date=2012}}</ref>");
    assert_eq!(c.len(), 1);
    assert_eq!((c[0].archive_url.as_deref(), c[0].archive_date_raw.as_deref()), (Some("https://web.archive.org/web/2011/http://f.ru"), Some("2011")));
    // Same original, no archive yet: it merges.
    let c = cites("<ref>{{cite web|url=http://f.ru}} {{webarchive|url=https://web.archive.org/web/2012/http://f.ru|date=2012}}</ref>");
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].archive_date_raw.as_deref(), Some("2012"));
}

#[test]
fn empty_plain_text_has_no_text() {
    let c = cites("<ref>{{some other|x}}</ref>");
    assert_eq!((c[0].family.as_str(), c[0].text.clone()), ("plain_text", None));
    assert_eq!(c[0].fp, fp("plain_text\u{1f}"));
}
