//! Symmetry: identical markup gives identical tables. Publicly, every Wikipedia version parses alike with the
//! two settings files (which tests them against each other without Ruwiki markup); privately, every Ruwiki
//! version byte-identical to its page's fork-day Wikipedia version parses alike with it.

mod common;

use wikitext_components::compare::identical_except_labels;
use wikitext_components::corpus::CorpusRow;

/// Differences between the two settings files that are deliberate (R61): a registry entry with `site=`, or a
/// namespace or prefix only one site has. `(page_id, version, reason)`. None so far: the settings files differ
/// only in their `site` key, and no registry entry is limited to a site.
const SITE_SCOPED: &[(&str, &str, &str)] = &[];

#[test]
fn identical_except_labels_names_the_first_difference_without_its_value() {
    let (a, b) = (common::run_page("== A ==\nx\n", "wikipedia"), common::run_page("== A ==\nx\n", "ruwiki"));
    assert_eq!(identical_except_labels(&a, &b), Ok(()));
    let c = common::run_page("== B ==\nx\n", "wikipedia");
    assert_eq!(identical_except_labels(&a, &c), Err("sections row 1: fp differs".into()));
    let d = common::run_page("== A ==\nx\n\ny\n", "wikipedia");
    assert!(matches!(identical_except_labels(&a, &d), Err(e) if e.starts_with("pages: ") && e.ends_with(" differs")));
    // Labels are not compared; a table's row count is.
    let mut e = a.clone();
    (e.page.title, e.page.timestamp, e.page.page_id) = (Some("Другое".into()), Some("2025".into()), "2".into());
    assert_eq!(identical_except_labels(&a, &e), Ok(()));
    let f = common::run_page("x<ref>y</ref>\n", "wikipedia");
    let mut g = f.clone();
    g.components.refs.clear();
    assert_eq!(identical_except_labels(&f, &g), Err("refs: 1 rows against 0".into()));
}

#[test]
fn wikipedia_markup_parses_alike_with_both_settings_files() {
    assert_eq!(common::settings_for("ruwiki").site(), "ruwiki");
    assert_eq!(common::settings_for("wikipedia").site(), "wikipedia");
    let rows = common::public_corpus();
    let (a, b) = (common::run_rows(&rows, "wikipedia"), common::run_rows(&rows, "ruwiki"));
    let mut bad = Vec::new();
    let mut used = vec![false; SITE_SCOPED.len()];
    for ((r, a), b) in rows.iter().zip(&a).zip(&b) {
        assert_eq!((a.page.site.as_str(), b.page.site.as_str()), ("wikipedia", "ruwiki"));
        let Err(e) = identical_except_labels(a, b) else { continue };
        match SITE_SCOPED.iter().position(|(p, v, _)| (*p, *v) == (r.page_id.as_str(), r.version.as_str())) {
            Some(i) => used[i] = true,
            None => bad.push(format!("page_id {} {}: {e}", r.page_id, r.version)),
        }
    }
    for ((p, v, _), used) in SITE_SCOPED.iter().zip(used) {
        if !used {
            bad.push(format!("page_id {p} {v}: allowed difference no longer occurs"));
        }
    }
    eprintln!("symmetry, public: {} versions, {} allowed site-scoped differences", rows.len(), SITE_SCOPED.len());
    assert!(bad.is_empty(), "{} versions parse differently with the two settings files:\n{}", bad.len(), bad.join("\n"));
}

#[test]
fn ruwiki_versions_equal_to_the_fork_day_parse_alike() {
    let Some(ruwiki) = common::private_corpus("ruwiki_versions_equal_to_the_fork_day_parse_alike") else { return };
    let public = common::public_corpus();
    let pairs: Vec<(&CorpusRow, &CorpusRow)> = ruwiki
        .iter()
        .filter_map(|r| {
            let fork = public.iter().find(|p| p.version == "wiki_forkday" && p.page_id == r.page_id)?;
            (fork.wikitext == r.wikitext).then_some((fork, r))
        })
        .collect();
    // The private corpus is pinned by its hash, so the number of byte-identical pairs is too.
    assert_eq!(pairs.len(), 17, "Ruwiki versions byte-identical to their fork-day version");
    let mut bad = Vec::new();
    for (i, (fork, r)) in pairs.iter().enumerate() {
        let a = &common::run_rows(std::slice::from_ref(*fork), "wikipedia")[0];
        let b = &common::run_rows(std::slice::from_ref(*r), "ruwiki")[0];
        // The message names a table, a row and a field, never a value (compare.rs).
        if let Err(e) = identical_except_labels(a, b) {
            bad.push(format!("private pair {i} (page_id {}, wiki_forkday / ruwiki_2025): {e}", r.page_id));
        }
    }
    eprintln!("symmetry, private: {} Ruwiki versions equal to their fork-day version", pairs.len());
    assert!(bad.is_empty(), "{} identical versions parse differently:\n{}", bad.len(), bad.join("\n"));
}
