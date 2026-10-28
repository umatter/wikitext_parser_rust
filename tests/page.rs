mod common;

use common::{defaults, run_opt, run_with};
use wikitext_components::page::Limits;

fn run(t: Option<&str>) -> wikitext_components::model::PageOutput {
    run_opt(t)
}

#[test]
fn statuses() {
    assert_eq!(run(None).page.status, "null_input");
    assert_eq!(run(Some("  \n")).page.status, "empty");
    let r = run(Some(&common::fixture("redirect_ru")));
    assert_eq!(r.page.status, "redirect");
    assert_eq!(r.page.redirect_target.as_deref(), Some("Москва"));
    let r = run(Some(&common::fixture("redirect_en")));
    assert_eq!(r.page.redirect_target.as_deref(), Some("Moscow"));
    assert_eq!(run(Some(&common::fixture("refs_basic"))).page.status, "ok");
}

#[test]
fn budget_failure_with_and_without_fallback() {
    let t = common::fixture("refs_basic");
    let a = run_with(&t, Limits { parse_budget: 10, fallback: false, ..defaults() });
    assert_eq!(a.page.status, "budget");
    assert!(a.page.status_detail.unwrap().starts_with("budget: "));
    let b = run_with(&t, Limits { parse_budget: 10, fallback: true, ..defaults() });
    assert_eq!(b.page.status, "fallback");
    assert!(b.page.status_detail.unwrap().starts_with("budget: "));
}

#[test]
fn counts_and_sizes() {
    let r = run(Some("Привет"));
    assert_eq!(r.page.n_bytes, 12);
    assert_eq!(r.page.n_chars, 6);
    assert!(r.page.parse_steps > 0);
}

#[test]
fn warnings_are_counted_and_sorted() {
    let r = run(Some(&common::fixture("malformed")));
    let p = &r.page;
    assert_eq!(p.n_warnings as usize, p.warning_counts.iter().map(|c| c.n as usize).sum::<usize>());
    assert!(p.warnings.len() <= 100);
    let kinds: Vec<_> = p.warning_counts.iter().map(|c| c.kind.clone()).collect();
    let mut sorted = kinds.clone();
    sorted.sort();
    assert_eq!(kinds, sorted);
}
