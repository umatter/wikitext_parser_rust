//! Measures the parser's counted steps and wall time per page version (Task 10).
//!
//! Usage: measure <parquet> --label key=site:version ... [--budget N] [--cases slow,largest]
//!                [--config-dir DIR] [--text-column NAME]
//!
//! Prints one TSV line per page version to stdout: `page_id version n_chars steps parse_ms status`.
//! `status` is `ok` or `budget`. With `--budget 0` the parse has no limit, so rows whose `case`
//! column says `slow` are skipped (they may never end). `--cases` keeps only rows whose `case`
//! is in the list. The `case` column is read from the file itself when it has one.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use wikitext_components::corpus;
use wikitext_components::frontend::{self, FrontendError};
use wikitext_components::input::{self, Label};
use wikitext_components::page::DEFAULT_PARSE_BUDGET;
use wikitext_components::settings::Settings;

struct Item {
    page_id: String,
    version: String,
    site: String,
    text: String,
    case: Option<String>,
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut path: Option<PathBuf> = None;
    let mut labels: Vec<Label> = Vec::new();
    let mut budget = DEFAULT_PARSE_BUDGET;
    let mut cases: Option<Vec<String>> = None;
    let mut config_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config");
    let mut text_column: Option<String> = None;
    while let Some(a) = args.next() {
        let mut val = |name: &str| args.next().ok_or_else(|| anyhow!("{name} needs a value"));
        match a.as_str() {
            "--label" => labels.push(input::parse_label(&val("--label")?)?),
            "--budget" => budget = val("--budget")?.parse().context("--budget")?,
            "--cases" => cases = Some(val("--cases")?.split(',').map(str::to_string).collect()),
            "--config-dir" => config_dir = PathBuf::from(val("--config-dir")?),
            "--text-column" => text_column = Some(val("--text-column")?),
            s if s.starts_with("--") => bail!("unknown option {s}"),
            _ => path = Some(PathBuf::from(a)),
        }
    }
    let path = path.ok_or_else(|| anyhow!("usage: measure <parquet> --label ... [--budget N] [--cases a,b]"))?;

    let mut settings: BTreeMap<String, Settings> = BTreeMap::new();
    for l in &labels {
        if !settings.contains_key(&l.site) {
            let f = config_dir.join("sites").join(if l.site == "wikipedia" {
                "ru.wikipedia.toml".to_string()
            } else {
                format!("{}.toml", l.site)
            });
            settings.insert(l.site.clone(), Settings::load(&f)?);
        }
    }

    let has_case = parquet_has_case(&path)?;
    let mut items: Vec<Item> = Vec::new();
    if has_case {
        for r in corpus::load(&path)? {
            let l = labels
                .iter()
                .find(|l| l.key == r.version)
                .ok_or_else(|| anyhow!("no --label for '{}'", r.version))?;
            items.push(Item { page_id: r.page_id, version: r.version, site: l.site.clone(), text: r.wikitext, case: r.case });
        }
    } else {
        let mut file = input::open(&path, None, text_column.as_deref(), &labels)?;
        for batch in file.batches(256) {
            for p in batch? {
                if let Some(text) = p.text {
                    let version = if p.key.column.is_empty() { p.key.version.clone() } else { p.key.column.clone() };
                    items.push(Item { page_id: p.key.page_id, version, site: p.key.site, text, case: None });
                }
            }
        }
    }

    for it in &items {
        let case = it.case.as_deref();
        if budget == 0 && case == Some("slow") {
            continue;
        }
        if let Some(keep) = &cases {
            if !case.is_some_and(|c| keep.iter().any(|k| k == c)) {
                continue;
            }
        }
        let config = settings[&it.site].configuration();
        let t = Instant::now();
        let res = frontend::parse(&it.text, config, budget);
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let (steps, status) = match &res {
            Ok(p) => (p.steps, "ok"),
            Err(FrontendError::Budget { steps }) => (*steps, "budget"),
        };
        drop(res);
        println!("{}\t{}\t{}\t{}\t{:.3}\t{}", it.page_id, it.version, it.text.chars().count(), steps, ms, status);
    }
    Ok(())
}

fn parquet_has_case(path: &std::path::Path) -> Result<bool> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let f = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let b = ParquetRecordBatchReaderBuilder::try_new(f)?;
    Ok(b.schema().field_with_name("case").is_ok())
}
