mod common;

use parse_wiki_text_2::Configuration;
use wikitext_components::frontend::{self, FrontendError};

fn cfg() -> Configuration {
    Configuration::default()
}

#[test]
fn steps_are_deterministic() {
    let t = common::fixture("refs_basic");
    let a = frontend::parse(&t, &cfg(), 0).unwrap().steps;
    let b = frontend::parse(&t, &cfg(), 0).unwrap().steps;
    assert_eq!(a, b);
    assert!(a > 0);
}

#[test]
fn budget_exceeded_is_reported_with_steps() {
    let t = common::fixture("refs_basic");
    match frontend::parse(&t, &cfg(), 10) {
        Err(FrontendError::Budget { steps }) => assert!(steps > 10),
        _ => panic!("expected a budget error"),
    }
}

#[test]
fn budget_equal_to_need_succeeds() {
    let t = common::fixture("refs_basic");
    let need = frontend::parse(&t, &cfg(), 0).unwrap().steps;
    assert!(frontend::parse(&t, &cfg(), need).is_ok());
}
