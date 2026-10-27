//! Per-page processing: parse within a budget, catch panics, assign exactly one status.
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Instant;

use parse_wiki_text_2::{Node, Output};

use crate::frontend::{self, FrontendError};
use crate::model::{Components, Key, PageOutput, PageRow, TimingRow, WarningCount, WarningRow};
use crate::registry::Registry;
use crate::settings::Settings;

/// Placeholder until Task 10 calibrates it.
pub const DEFAULT_PARSE_BUDGET: u64 = 2_000_000_000;
pub const DEFAULT_WALK_BUDGET: u64 = 50_000_000;
pub const DEFAULT_DEPTH_CAP: usize = 400;
const MAX_WARNINGS: usize = 100;

#[derive(Clone, Debug)]
pub struct Limits {
    pub parse_budget: u64,
    pub walk_budget: u64,
    pub depth_cap: usize,
    pub fallback: bool,
}

pub struct PageInput {
    pub key: Key,
    pub title: Option<String>,
    pub timestamp: Option<String>,
    pub text: Option<String>,
}

/// A failure that gives a page a non-`ok` status: `budget`, `depth_limit` or `panic`.
#[derive(Debug, Clone, PartialEq)]
struct Failure {
    status: &'static str,
    message: String,
}

/// Why the walk stopped early (the walk of Task 13 returns this).
#[derive(Debug, Clone, PartialEq)]
// Constructed by the walk of Task 13; until then only the tests build them.
#[allow(dead_code)]
enum WalkError {
    Budget { steps: u64 },
    Depth { cap: usize },
    Panic(String),
}

impl From<WalkError> for Failure {
    fn from(e: WalkError) -> Failure {
        match e {
            WalkError::Budget { steps } => Failure {
                status: "budget",
                message: format!("walk exceeded its budget (used {steps})"),
            },
            WalkError::Depth { cap } => Failure {
                status: "depth_limit",
                message: format!("nesting deeper than {cap}"),
            },
            WalkError::Panic(m) => Failure { status: "panic", message: m },
        }
    }
}

/// Run `f`, turning a panic into a `panic` failure that carries the panic message.
fn guarded<T>(f: impl FnOnce() -> T) -> Result<T, Failure> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|p| {
        let message = p
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".to_string());
        Failure { status: "panic", message }
    })
}

/// Stub until Task 13: the structured walk.
fn walk(
    _text: &str,
    _output: &Output<'_>,
    _settings: &Settings,
    _registry: &Registry,
    _limits: &Limits,
) -> Result<Components, WalkError> {
    Ok(Components::default())
}

/// Stub until Task 18: the fallback scanner.
fn scan(_text: &str, _settings: &Settings, _registry: &Registry) -> Components {
    Components::default()
}

fn warning_kind(m: &parse_wiki_text_2::WarningMessage) -> String {
    let d = format!("{m:?}");
    d.split(|c: char| !c.is_alphanumeric()).next().unwrap_or("").to_string()
}

fn base_row(input: &PageInput, status: &str) -> PageRow {
    let k = &input.key;
    let (n_bytes, n_chars) = input
        .text
        .as_deref()
        .map(|t| (t.len() as i32, t.chars().count() as i32))
        .unwrap_or((0, 0));
    PageRow {
        source_file: k.source_file.clone(),
        row: k.row,
        page_id: k.page_id.clone(),
        site: k.site.clone(),
        version: k.version.clone(),
        column: k.column.clone(),
        title: input.title.clone(),
        timestamp: input.timestamp.clone(),
        status: status.to_string(),
        status_detail: None,
        parse_steps: 0,
        n_bytes,
        n_chars,
        n_warnings: 0,
        warning_counts: vec![],
        warnings: vec![],
        redirect_target: None,
        n_sections: 0,
        n_paragraphs: 0,
        n_templates: 0,
        n_refs: 0,
        n_ref_lists: 0,
        n_citations: 0,
    }
}

fn timing_row(key: &Key, parse_ms: f64, walk_ms: f64) -> TimingRow {
    TimingRow {
        source_file: key.source_file.clone(),
        row: key.row,
        page_id: key.page_id.clone(),
        site: key.site.clone(),
        version: key.version.clone(),
        column: key.column.clone(),
        parse_ms,
        walk_ms,
    }
}

fn fill_counts(page: &mut PageRow, c: &Components) {
    page.n_sections = c.sections.len() as i32;
    page.n_paragraphs = c.paragraphs.len() as i32;
    page.n_templates = c.templates.len() as i32;
    page.n_refs = c.refs.len() as i32;
    page.n_ref_lists = c.ref_lists.len() as i32;
    page.n_citations = c.citations.len() as i32;
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

type WalkFn<'f> =
    &'f dyn Fn(&str, &Output<'_>, &Settings, &Registry, &Limits) -> Result<Components, WalkError>;
type ScanFn<'f> = &'f dyn Fn(&str, &Settings, &Registry) -> Components;

pub fn process_page(
    input: &PageInput,
    settings: &Settings,
    registry: &Registry,
    limits: &Limits,
) -> PageOutput {
    process_with(input, settings, registry, limits, &walk, &scan)
}

/// `process_page` with the walk and the fallback scanner as parameters, so tests can inject failures.
fn process_with(
    input: &PageInput,
    settings: &Settings,
    registry: &Registry,
    limits: &Limits,
    walk: WalkFn<'_>,
    scan: ScanFn<'_>,
) -> PageOutput {
    let finish = |page: PageRow, parse_ms: f64, walk_ms: f64, components: Components| {
        let mut page = page;
        fill_counts(&mut page, &components);
        PageOutput { timing: timing_row(&input.key, parse_ms, walk_ms), page, components }
    };

    let Some(text) = input.text.as_deref() else {
        return finish(base_row(input, "null_input"), 0.0, 0.0, Components::default());
    };
    if text.trim().is_empty() {
        return finish(base_row(input, "empty"), 0.0, 0.0, Components::default());
    }

    // A failure either ends the page with its status or, with the fallback on, hands it to the scanner.
    let fail = |mut page: PageRow, f: Failure, parse_ms: f64, walk_ms: f64| {
        if limits.fallback {
            let detail = format!("{}: {}", f.status, f.message);
            match guarded(|| scan(text, settings, registry)) {
                Ok(components) => {
                    page.status = "fallback".to_string();
                    page.status_detail = Some(detail);
                    finish(page, parse_ms, walk_ms, components)
                }
                Err(sf) => {
                    page.status = "panic".to_string();
                    page.status_detail =
                        Some(format!("panic: fallback scan panicked: {} (after {detail})", sf.message));
                    finish(page, parse_ms, walk_ms, Components::default())
                }
            }
        } else {
            page.status = f.status.to_string();
            page.status_detail = Some(format!("{}: {}", f.status, f.message));
            finish(page, parse_ms, walk_ms, Components::default())
        }
    };

    let mut page = base_row(input, "ok");
    let t0 = Instant::now();
    let parsed = guarded(|| frontend::parse(text, settings.configuration(), limits.parse_budget));
    let parse_ms = ms(t0);
    let parsed = match parsed {
        Ok(Ok(p)) => p,
        Ok(Err(FrontendError::Budget { steps })) => {
            page.parse_steps = steps as i64;
            let f = Failure {
                status: "budget",
                message: format!("parse exceeded {} steps (used {steps})", limits.parse_budget),
            };
            return fail(page, f, parse_ms, 0.0);
        }
        Err(f) => return fail(page, f, parse_ms, 0.0),
    };
    page.parse_steps = parsed.steps as i64;

    let output = &parsed.output;
    page.n_warnings = output.warnings.len() as i32;
    page.warnings = output
        .warnings
        .iter()
        .take(MAX_WARNINGS)
        .map(|w| WarningRow {
            kind: warning_kind(&w.message),
            start: w.start as i32,
            end: w.end as i32,
        })
        .collect();
    let mut counts = std::collections::BTreeMap::<String, i32>::new();
    for w in &output.warnings {
        *counts.entry(warning_kind(&w.message)).or_insert(0) += 1;
    }
    page.warning_counts = counts.into_iter().map(|(kind, n)| WarningCount { kind, n }).collect();

    if let Some(Node::Redirect { target, .. }) = output.nodes.first() {
        page.status = "redirect".to_string();
        page.redirect_target = Some(target.to_string());
        return finish(page, parse_ms, 0.0, Components::default());
    }

    let t1 = Instant::now();
    let walked = guarded(|| walk(text, output, settings, registry, limits));
    let walk_ms = ms(t1);
    match walked {
        Ok(Ok(components)) => finish(page, parse_ms, walk_ms, components),
        Ok(Err(e)) => fail(page, e.into(), parse_ms, walk_ms),
        Err(f) => fail(page, f, parse_ms, walk_ms),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guarded_turns_a_panic_into_a_failure() {
        let f = guarded(|| -> () { panic!("x") }).unwrap_err();
        assert_eq!(f.status, "panic");
        assert_eq!(f.message, "x");
    }

    #[test]
    fn walk_failures_map_to_statuses() {
        let f: Failure = WalkError::Depth { cap: 400 }.into();
        assert_eq!(f.status, "depth_limit");
        let f: Failure = WalkError::Budget { steps: 9 }.into();
        assert_eq!(f.status, "budget");
        let f: Failure = WalkError::Panic("p".into()).into();
        assert_eq!((f.status, f.message.as_str()), ("panic", "p"));
    }

    use crate::model::Key;

    fn run(walk: WalkFn<'_>, scan: ScanFn<'_>, fallback: bool) -> PageOutput {
        let settings = Settings::load(std::path::Path::new("config/sites/ru.wikipedia.toml")).unwrap();
        let input = PageInput {
            key: Key {
                source_file: "t".into(),
                row: 0,
                page_id: "1".into(),
                site: "wikipedia".into(),
                version: "v".into(),
                column: "text".into(),
            },
            title: None,
            timestamp: None,
            text: Some("Привет '''мир'''".into()),
        };
        let limits = Limits {
            parse_budget: DEFAULT_PARSE_BUDGET,
            walk_budget: DEFAULT_WALK_BUDGET,
            depth_cap: DEFAULT_DEPTH_CAP,
            fallback,
        };
        process_with(&input, &settings, &Registry::default(), &limits, walk, scan)
    }

    fn ok_scan(_: &str, _: &Settings, _: &Registry) -> Components {
        Components::default()
    }

    #[test]
    fn walk_failures_give_status_or_fallback() {
        let cases: [(WalkError, &str); 3] = [
            (WalkError::Budget { steps: 9 }, "budget"),
            (WalkError::Depth { cap: 400 }, "depth_limit"),
            (WalkError::Panic("boom".into()), "panic"),
        ];
        for (err, status) in cases {
            let walk = |_: &str, _: &Output<'_>, _: &Settings, _: &Registry, _: &Limits| Err(err.clone());
            let a = run(&walk, &ok_scan, false);
            assert_eq!(a.page.status, status);
            assert!(a.page.status_detail.unwrap().starts_with(&format!("{status}: ")));
            let b = run(&walk, &ok_scan, true);
            assert_eq!(b.page.status, "fallback");
            assert!(b.page.status_detail.unwrap().starts_with(&format!("{status}: ")));
        }
    }

    #[test]
    fn walk_panic_is_caught() {
        let walk = |_: &str, _: &Output<'_>, _: &Settings, _: &Registry, _: &Limits| -> Result<Components, WalkError> {
            panic!("walk blew up")
        };
        let a = run(&walk, &ok_scan, false);
        assert_eq!(a.page.status, "panic");
        assert_eq!(a.page.status_detail.as_deref(), Some("panic: walk blew up"));
        let b = run(&walk, &ok_scan, true);
        assert_eq!(b.page.status, "fallback");
        assert_eq!(b.page.status_detail.as_deref(), Some("panic: walk blew up"));
    }

    #[test]
    fn scan_panic_gives_panic_row_naming_both() {
        let walk = |_: &str, _: &Output<'_>, _: &Settings, _: &Registry, _: &Limits| {
            Err(WalkError::Depth { cap: 400 })
        };
        let scan = |_: &str, _: &Settings, _: &Registry| -> Components { panic!("scan blew up") };
        let r = run(&walk, &scan, true);
        assert_eq!(r.page.status, "panic");
        let d = r.page.status_detail.unwrap();
        assert!(d.starts_with("panic: "), "{d}");
        assert!(d.contains("scan blew up") && d.contains("depth_limit"), "{d}");
        assert_eq!(r.components, Components::default());
    }
}
