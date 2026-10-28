//! The `parse_components` driver: streams each input in batches, processes the pages of a batch in
//! parallel, and writes them in input order, then document order, to one part file per input and
//! table. Also the manifest, `--resume` and the watchdog.
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use serde::Serialize;
use serde_json::{json, Value};

use crate::input::{self, Label, Layout};
use crate::model::{Key, PageOutput, SCHEMA_VERSION};
use crate::output::{self, PartWriter, TABLES};
use crate::page::{self, Limits, PageInput};
use crate::registry::Registry;
use crate::settings::Settings;

/// Input rows read per batch.
pub const BATCH_ROWS: usize = 256;
/// Wall-clock seconds one page may run before the watchdog quarantines it and aborts (D8).
pub const WATCHDOG_SECS: u64 = 600;
/// Upstream commit of the vendored `parse-wiki-text-2` (vendor/parse-wiki-text-2/VENDORED.md).
pub const VENDORED_UPSTREAM: &str = "bc6bde5";
// `VENDORED_PATCHES` and `VENDORED_SRC_SHA256`, computed by build.rs.
include!(concat!(env!("OUT_DIR"), "/vendored.rs"));
const QUARANTINE: &str = "quarantine.list";
/// Worker stack size: room for deep pages (D8, step 2).
const WORKER_STACK: usize = 32 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Parquet,
    Json,
}

#[derive(Clone, Debug)]
pub struct RunArgs {
    pub inputs: Vec<PathBuf>,
    pub out: PathBuf,
    pub labels: Vec<Label>,
    pub layout: Option<Layout>,
    pub text_column: Option<String>,
    pub format: Format,
    pub tables: Option<Vec<String>>,
    /// Worker threads; 0 means one per core.
    pub jobs: usize,
    pub limits: Limits,
    pub resume: bool,
    /// Holds `sites/*.toml` and `registry/{templates,sections}.toml`; see `default_config_dir`.
    pub config_dir: PathBuf,
}

/// `config/` of this source tree, fixed at build time.
pub fn default_config_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config")
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct InputEntry {
    pub path: String,
    /// Input rows (1 for a `.wiki` file).
    pub rows: usize,
    /// Page versions: rows times texts per row.
    pub pages: usize,
    /// Skipped by `--resume`: its outputs were finished by an earlier run.
    pub resumed: bool,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct OutputEntry {
    /// Relative to `--out` (the `--out` path itself for a `.wiki` input with `--format json`).
    pub path: String,
    pub rows: usize,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct FailedInput {
    pub path: String,
    pub message: String,
}

/// site -> version -> status -> page versions.
pub type StatusCounts = BTreeMap<String, BTreeMap<String, BTreeMap<String, u64>>>;

/// `<out>/manifest.json`.
#[derive(Serialize, Clone, Debug)]
pub struct Manifest {
    pub schema_version: String,
    pub crate_version: String,
    pub git_commit: String,
    pub vendored_upstream: String,
    pub vendored_patches: Vec<String>,
    pub vendored_src_sha256: String,
    pub settings_hashes: BTreeMap<String, String>,
    /// sha256 of the registry's two files; see `Registry::hash`.
    pub registry_hash: Option<String>,
    pub args: Value,
    pub inputs: Vec<InputEntry>,
    pub outputs: Vec<OutputEntry>,
    pub failed_inputs: Vec<FailedInput>,
    pub status_counts: StatusCounts,
    pub pages_per_second: f64,
    pub seconds_over_budget: f64,
}

/// Every `sites/*.toml` under `config_dir`, by its `site` key. A duplicate key is an error.
pub fn load_sites(config_dir: &Path) -> Result<BTreeMap<String, Settings>> {
    let dir = config_dir.join("sites");
    let mut files: Vec<PathBuf> = fs::read_dir(&dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<_>>()?;
    files.retain(|p| p.extension().is_some_and(|x| x == "toml") && p.is_file());
    files.sort();
    let mut sites: BTreeMap<String, Settings> = BTreeMap::new();
    let mut from: BTreeMap<String, PathBuf> = BTreeMap::new();
    for f in files {
        let s = Settings::load(&f)?;
        let site = s.site().to_string();
        if let Some(first) = from.get(&site) {
            bail!("duplicate site '{site}' in {} and {}", first.display(), f.display());
        }
        from.insert(site.clone(), f);
        sites.insert(site, s);
    }
    Ok(sites)
}

fn label_string(l: &Label) -> String {
    if l.key.is_empty() {
        format!("{}:{}", l.site, l.version)
    } else {
        format!("{}={}:{}", l.key, l.site, l.version)
    }
}

fn args_json(args: &RunArgs, jobs: usize, tables: &[&str]) -> Value {
    json!({
        "inputs": args.inputs.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
        "out": args.out.display().to_string(),
        "labels": args.labels.iter().map(label_string).collect::<Vec<_>>(),
        "layout": args.layout.map(|l| format!("{l:?}").to_lowercase()),
        "text_column": args.text_column,
        "format": format!("{:?}", args.format).to_lowercase(),
        "tables": tables,
        "jobs": jobs,
        "budget": args.limits.parse_budget,
        "walk_budget": args.limits.walk_budget,
        "depth_cap": args.limits.depth_cap,
        "fallback": args.limits.fallback,
        "resume": args.resume,
        "config_dir": args.config_dir.display().to_string(),
    })
}

/// The tables to write, in `TABLES` order; `pages` always.
fn selected_tables(args: &RunArgs) -> Result<Vec<&'static str>> {
    let Some(wanted) = &args.tables else { return Ok(TABLES.to_vec()) };
    if args.format == Format::Json {
        bail!("--tables applies to --format parquet only");
    }
    for t in wanted {
        if !TABLES.contains(&t.as_str()) {
            bail!("unknown table '{t}' (known: {})", TABLES.join(", "));
        }
    }
    Ok(TABLES.iter().copied().filter(|t| *t == "pages" || wanted.iter().any(|w| w == t)).collect())
}

fn is_wiki(p: &Path) -> bool {
    p.extension().is_some_and(|x| x == "wiki")
}

/// Directories become their `*.parquet` files, sorted by file name.
fn expand_inputs(inputs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut out = vec![];
    for p in inputs {
        if p.is_dir() {
            let mut files: Vec<PathBuf> = fs::read_dir(p)
                .with_context(|| format!("reading {}", p.display()))?
                .map(|e| e.map(|e| e.path()))
                .collect::<std::io::Result<_>>()?;
            files.retain(|f| f.is_file() && f.extension().is_some_and(|x| x == "parquet"));
            files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
            if files.is_empty() {
                bail!("no .parquet files in {}", p.display());
            }
            out.extend(files);
        } else if p.is_file() {
            out.push(p.clone());
        } else {
            bail!("input not found: {}", p.display());
        }
    }
    if out.is_empty() {
        bail!("no input files");
    }
    Ok(out)
}

fn stem(p: &Path) -> String {
    p.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default()
}

/// The watchdog's limit; `test-hooks` builds read `WIKITEXT_WATCHDOG_SECS`.
fn watchdog_limit() -> Duration {
    #[cfg(feature = "test-hooks")]
    if let Some(s) = std::env::var("WIKITEXT_WATCHDOG_SECS").ok().and_then(|v| v.parse().ok()) {
        return Duration::from_secs(s);
    }
    Duration::from_secs(WATCHDOG_SECS)
}

fn quarantine_line(k: &Key) -> String {
    format!("{}\t{}\t{}", k.source_file, k.row, k.column)
}

/// Lines of `quarantine.list` (`source_file\trow\tcolumn`); none when the file is absent.
fn read_quarantine(path: &Path) -> Result<HashSet<String>> {
    let text = match fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let mut set = HashSet::new();
    for (i, line) in text.lines().enumerate().filter(|(_, l)| !l.is_empty()) {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 3 || parts[1].parse::<i32>().is_err() {
            bail!("{}:{}: expected source_file<TAB>row<TAB>column", path.display(), i + 1);
        }
        set.insert(line.to_string());
    }
    Ok(set)
}

struct Running {
    line: String,
    started: Instant,
}

#[derive(Default)]
struct Watched {
    next: AtomicU64,
    running: Mutex<BTreeMap<u64, Running>>,
}

/// A supervisor thread that polls the running pages every second. A page over the limit is
/// appended to `quarantine.list` and the process aborts: the budgets bound all the work, so this
/// is a bug, and `--resume` then gives the page `status = "hang"`.
struct Watchdog {
    watched: Arc<Watched>,
    stop: Option<mpsc::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

/// Marks a page as running until dropped.
struct Watch<'a> {
    watched: &'a Watched,
    id: u64,
}

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        self.watched.running.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.id);
    }
}

impl Watchdog {
    fn start(quarantine: PathBuf, limit: Duration) -> Watchdog {
        let watched = Arc::new(Watched::default());
        let (stop, rx) = mpsc::channel::<()>();
        let w = watched.clone();
        let thread = std::thread::spawn(move || {
            while let Err(RecvTimeoutError::Timeout) = rx.recv_timeout(Duration::from_secs(1)) {
                let over: Vec<String> = {
                    let running = w.running.lock().unwrap_or_else(|e| e.into_inner());
                    running.values().filter(|r| r.started.elapsed() > limit).map(|r| r.line.clone()).collect()
                };
                if !over.is_empty() {
                    quarantine_and_abort(&quarantine, &over, limit);
                }
            }
        });
        Watchdog { watched, stop: Some(stop), thread: Some(thread) }
    }

    fn watch(&self, key: &Key) -> Watch<'_> {
        let id = self.watched.next.fetch_add(1, Ordering::Relaxed);
        let r = Running { line: quarantine_line(key), started: Instant::now() };
        self.watched.running.lock().unwrap_or_else(|e| e.into_inner()).insert(id, r);
        Watch { watched: &self.watched, id }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn quarantine_and_abort(path: &Path, lines: &[String], limit: Duration) -> ! {
    let written = fs::OpenOptions::new().create(true).append(true).open(path).and_then(|mut f| {
        for l in lines {
            writeln!(f, "{l}")?;
        }
        f.sync_all()
    });
    for l in lines {
        eprintln!(
            "watchdog: page {} ran more than {} s; quarantined in {}; aborting (rerun with --resume)",
            l.replace('\t', " "),
            limit.as_secs(),
            path.display()
        );
    }
    if let Err(e) = written {
        eprintln!("watchdog: could not write {}: {e}", path.display());
    }
    std::process::abort()
}

/// What one page needs, shared by the worker threads.
struct Engine<'a> {
    sites: &'a BTreeMap<String, Settings>,
    registry: &'a Registry,
    limits: &'a Limits,
    quarantined: &'a HashSet<String>,
    watchdog: Option<&'a Watchdog>,
}

impl Engine<'_> {
    /// The page's output and its wall time in seconds.
    fn page(&self, p: &PageInput) -> (PageOutput, f64) {
        let settings = self.sites.get(&p.key.site).expect("label sites are checked before any page");
        let t = Instant::now();
        let out = if !self.quarantined.is_empty() && self.quarantined.contains(&quarantine_line(&p.key)) {
            page::hang_page(p, settings, self.registry, self.limits)
        } else {
            let _watch = self.watchdog.map(|w| w.watch(&p.key));
            #[cfg(feature = "test-hooks")]
            if std::env::var("WIKITEXT_TEST_SLEEP_PAGE").ok().as_deref() == Some(p.key.page_id.as_str()) {
                std::thread::sleep(watchdog_limit() * 2 + Duration::from_secs(5));
            }
            page::process_page(p, settings, self.registry, self.limits)
        };
        (out, t.elapsed().as_secs_f64())
    }
}

#[derive(Default)]
struct Tally {
    pages: usize,
    counts: StatusCounts,
    over_budget: f64,
}

impl Tally {
    fn count(&mut self, site: &str, version: &str, status: &str, n: u64) {
        let versions = self.counts.entry(site.to_string()).or_default();
        *versions.entry(version.to_string()).or_default().entry(status.to_string()).or_default() += n;
    }

    fn add(&mut self, out: &PageOutput, secs: f64) {
        let p = &out.page;
        self.pages += 1;
        self.count(&p.site, &p.version, &p.status, 1);
        let detail_budget = p.status_detail.as_deref().is_some_and(|d| d.starts_with("budget:"));
        let budget = p.status == "budget" || (p.status == "fallback" && detail_budget);
        if budget {
            self.over_budget += secs;
        }
    }

    fn merge(&mut self, other: Tally) {
        self.pages += other.pages;
        self.over_budget += other.over_budget;
        for (site, versions) in &other.counts {
            for (version, statuses) in versions {
                for (status, n) in statuses {
                    self.count(site, version, status, *n);
                }
            }
        }
    }
}

/// Where one input's rows go.
enum Sink {
    /// One writer per selected table, `pages` among them.
    Parquet(Vec<PartWriter>),
    Jsonl { tmp: PathBuf, path: PathBuf, file: BufWriter<File>, rows: usize },
}

impl Sink {
    fn create(out: &Path, format: Format, tables: &[&str], stem: &str) -> Result<Sink> {
        Ok(match format {
            Format::Parquet => {
                Sink::Parquet(tables.iter().map(|t| PartWriter::create(out, t, stem)).collect::<Result<_>>()?)
            }
            Format::Json => {
                let path = out.join(format!("{stem}.jsonl"));
                let tmp = out.join(format!("{stem}.jsonl.tmp"));
                let file = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
                Sink::Jsonl { tmp, path, file: BufWriter::new(file), rows: 0 }
            }
        })
    }

    /// Appends a batch. The tables' writers run in parallel (R74): each part file depends only on its own
    /// writer and the batches in order, so the output does not depend on the threads.
    fn append(&mut self, pages: &[PageOutput]) -> Result<()> {
        match self {
            Sink::Parquet(writers) => writers.par_iter_mut().try_for_each(|w| w.append(pages)),
            Sink::Jsonl { file, rows, .. } => {
                for p in pages {
                    serde_json::to_writer(&mut *file, &output::page_value(p))?;
                    file.write_all(b"\n")?;
                }
                *rows += pages.len();
                Ok(())
            }
        }
    }

    /// Finishes every part, `pages` last (chosen by name, whatever the table order), so a finished
    /// `pages` part marks the input as done. Returns the outputs, relative to `out`, in the order
    /// their files were renamed into place.
    fn finish(self, stem: &str) -> Result<Vec<OutputEntry>> {
        match self {
            Sink::Parquet(writers) => {
                let (pages, rest): (Vec<_>, Vec<_>) =
                    writers.into_iter().partition(|w| w.table() == "pages");
                if pages.len() != 1 {
                    bail!("expected one pages part, found {}", pages.len());
                }
                let mut outputs = vec![];
                for w in rest.into_iter().chain(pages) {
                    let path = format!("{}/{stem}.parquet", w.table());
                    let rows = w.finish()?;
                    outputs.push(OutputEntry { path, rows });
                }
                Ok(outputs)
            }
            Sink::Jsonl { tmp, path, file, rows } => {
                let file = file.into_inner().map_err(|e| e.into_error())?;
                file.sync_all()?;
                fs::rename(&tmp, &path).with_context(|| format!("renaming {} into place", tmp.display()))?;
                Ok(vec![OutputEntry { path: format!("{stem}.jsonl"), rows }])
            }
        }
    }
}

/// Run-wide state shared by every input.
struct Ctx<'a> {
    args: &'a RunArgs,
    tables: &'a [&'static str],
    parquet_labels: &'a [Label],
    wiki_label: Option<&'a Label>,
    engine: Engine<'a>,
    pool: &'a rayon::ThreadPool,
}

/// One input processed to the end. On an error the sink is dropped unfinished: its parts stay `.tmp`.
fn process_input(ctx: &Ctx, path: &Path, stem: &str) -> Result<(InputEntry, Vec<OutputEntry>, Tally)> {
    let args = ctx.args;
    let mut source = None;
    let mut wiki = None;
    if is_wiki(path) {
        let label = ctx.wiki_label.ok_or_else(|| anyhow!("a .wiki input needs --label <site>:<version>"))?;
        wiki = Some(input::wiki_file(path, label)?);
    } else {
        source = Some(input::open(path, args.layout, args.text_column.as_deref(), ctx.parquet_labels)?);
    }
    let batches: Box<dyn Iterator<Item = Result<Vec<PageInput>>> + '_> = match (&mut source, wiki) {
        (Some(f), _) => Box::new(f.batches(BATCH_ROWS)),
        (None, Some(p)) => Box::new(std::iter::once(Ok(vec![p]))),
        (None, None) => unreachable!(),
    };

    let mut sink = Sink::create(&args.out, args.format, ctx.tables, stem)?;
    let mut tally = Tally::default();
    let mut rows = 0usize;
    for batch in batches {
        let batch = batch?;
        if let Some(last) = batch.last() {
            rows = last.key.row as usize + 1;
        }
        let done: Vec<(PageOutput, f64)> =
            ctx.pool.install(|| batch.par_iter().map(|p| ctx.engine.page(p)).collect());
        for (out, secs) in &done {
            tally.add(out, *secs);
        }
        let outputs: Vec<PageOutput> = done.into_iter().map(|(o, _)| o).collect();
        ctx.pool.install(|| sink.append(&outputs))?;
    }
    let outputs = sink.finish(stem)?;
    let entry =
        InputEntry { path: path.display().to_string(), rows, pages: tally.pages, resumed: false };
    Ok((entry, outputs, tally))
}

/// Rows in a Parquet file, from its footer.
fn parquet_rows(path: &Path) -> Result<usize> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let b = ParquetRecordBatchReaderBuilder::try_new(f)
        .with_context(|| format!("reading {}", path.display()))?;
    Ok(b.metadata().file_metadata().num_rows() as usize)
}

fn string_column(a: &dyn Array) -> Result<Vec<String>> {
    let c = cast(a, &DataType::Utf8)?;
    let c = c.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow!("not a string column"))?;
    Ok(c.iter().map(|v| v.unwrap_or_default().to_string()).collect())
}

/// An input `--resume` skips: its outputs and status counts are read back from what the earlier
/// run finished, so the manifest still describes the whole output directory.
fn resumed_input(ctx: &Ctx, path: &Path, stem: &str) -> Result<(InputEntry, Vec<OutputEntry>, Tally)> {
    let out = &ctx.args.out;
    let mut tally = Tally::default();
    let mut outputs = vec![];
    match ctx.args.format {
        Format::Parquet => {
            // The order a run renames them in: `pages` last.
            let tables = ctx.tables.iter().filter(|t| **t != "pages").chain(std::iter::once(&"pages"));
            for t in tables {
                let p = out.join(t).join(format!("{stem}.parquet"));
                if p.exists() {
                    let rows = parquet_rows(&p)?;
                    outputs.push(OutputEntry { path: format!("{t}/{stem}.parquet"), rows });
                }
            }
            let pages = out.join("pages").join(format!("{stem}.parquet"));
            let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&pages)?)?;
            let names = ["site", "version", "status"];
            let idx: Vec<usize> = names
                .iter()
                .map(|n| b.schema().index_of(n))
                .collect::<std::result::Result<_, _>>()?;
            let mask = ProjectionMask::roots(b.parquet_schema(), idx);
            for batch in b.with_projection(mask).build()? {
                let batch = batch?;
                let [site, version, status] =
                    [0, 1, 2].map(|i| string_column(batch.column(i)));
                for ((site, version), status) in site?.iter().zip(&version?).zip(&status?) {
                    tally.pages += 1;
                    tally.count(site, version, status, 1);
                }
            }
        }
        Format::Json => {
            let p = out.join(format!("{stem}.jsonl"));
            let text = fs::read_to_string(&p).with_context(|| format!("reading {}", p.display()))?;
            for line in text.lines() {
                let v: Value = serde_json::from_str(line)?;
                let s = |k: &str| v["page"][k].as_str().unwrap_or_default().to_string();
                tally.pages += 1;
                tally.count(&s("site"), &s("version"), &s("status"), 1);
            }
            outputs.push(OutputEntry { path: format!("{stem}.jsonl"), rows: tally.pages });
        }
    }
    let rows = if is_wiki(path) { 1 } else { parquet_rows(path)? };
    let entry = InputEntry { path: path.display().to_string(), rows, pages: tally.pages, resumed: true };
    Ok((entry, outputs, tally))
}

fn write_atomic(path: &Path, text: &str) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut f = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        f.write_all(text.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path).with_context(|| format!("renaming {} into place", tmp.display()))?;
    Ok(())
}

/// Runs the whole job. Configuration errors (labels, sites, registry, tables, inputs) fail before any output.
/// A failure inside one input is recorded under `failed_inputs`, its parts stay `.tmp`, and the run
/// goes on with the next input; the caller exits non-zero when `failed_inputs` is not empty.
pub fn run(args: &RunArgs) -> Result<Manifest> {
    let tables = selected_tables(args)?;
    let sites = load_sites(&args.config_dir)?;
    let registry = Registry::load(&args.config_dir.join("registry"))?;
    let mut settings_hashes = BTreeMap::new();
    for l in &args.labels {
        let s = sites
            .get(&l.site)
            .ok_or_else(|| anyhow!("unknown site '{}' (no config/sites file)", l.site))?;
        settings_hashes.insert(l.site.clone(), s.hash());
    }
    let inputs = expand_inputs(&args.inputs)?;
    let mut stems = BTreeMap::new();
    for p in &inputs {
        if let Some(first) = stems.insert(stem(p), p) {
            bail!("inputs {} and {} share the stem '{}'", first.display(), p.display(), stem(p));
        }
    }
    let wiki_labels: Vec<&Label> = args.labels.iter().filter(|l| l.key.is_empty()).collect();
    let parquet_labels: Vec<Label> = args.labels.iter().filter(|l| !l.key.is_empty()).cloned().collect();
    let any_wiki = inputs.iter().any(|p| is_wiki(p));
    match (any_wiki, wiki_labels.len()) {
        (true, 1) | (false, 0) => {}
        (true, _) => bail!("a .wiki input needs exactly one --label <site>:<version> (without a key)"),
        (false, _) => {
            bail!("--label without a key ('{}') applies to .wiki inputs only", label_string(wiki_labels[0]))
        }
    }
    let wiki_json = any_wiki && args.format == Format::Json;
    if wiki_json && inputs.len() != 1 {
        bail!("--format json with a .wiki input takes exactly one input; --out is the JSON file");
    }

    let jobs = match args.jobs {
        0 => std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
        n => n,
    };
    let pool = rayon::ThreadPoolBuilder::new().num_threads(jobs).stack_size(WORKER_STACK).build()?;
    let mut manifest = Manifest {
        schema_version: SCHEMA_VERSION.to_string(),
        crate_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: env!("GIT_COMMIT").to_string(),
        vendored_upstream: VENDORED_UPSTREAM.to_string(),
        vendored_patches: VENDORED_PATCHES.iter().map(|p| p.to_string()).collect(),
        vendored_src_sha256: VENDORED_SRC_SHA256.to_string(),
        settings_hashes,
        registry_hash: Some(registry.hash()),
        args: args_json(args, jobs, &tables),
        inputs: vec![],
        outputs: vec![],
        failed_inputs: vec![],
        status_counts: BTreeMap::new(),
        pages_per_second: 0.0,
        seconds_over_budget: 0.0,
    };
    let started = Instant::now();
    let no_quarantine = HashSet::new();

    if wiki_json {
        let engine = Engine {
            sites: &sites,
            registry: &registry,
            limits: &args.limits,
            quarantined: &no_quarantine,
            watchdog: None,
        };
        let page = input::wiki_file(&inputs[0], wiki_labels[0])?;
        let (out, secs) = pool.install(|| engine.page(&page));
        write_atomic(&args.out, &(output::page_json(&out) + "\n"))?;
        let mut tally = Tally::default();
        tally.add(&out, secs);
        let path = inputs[0].display().to_string();
        manifest.inputs.push(InputEntry { path, rows: 1, pages: 1, resumed: false });
        manifest.outputs.push(OutputEntry { path: args.out.display().to_string(), rows: 1 });
        finish_manifest(&mut manifest, tally, started);
        return Ok(manifest);
    }

    fs::create_dir_all(&args.out).with_context(|| format!("creating {}", args.out.display()))?;
    let quarantine = args.out.join(QUARANTINE);
    let quarantined = if args.resume { read_quarantine(&quarantine)? } else { HashSet::new() };
    let watchdog = Watchdog::start(quarantine, watchdog_limit());
    let ctx = Ctx {
        args,
        tables: &tables,
        parquet_labels: &parquet_labels,
        wiki_label: wiki_labels.first().copied(),
        engine: Engine {
            sites: &sites,
            registry: &registry,
            limits: &args.limits,
            quarantined: &quarantined,
            watchdog: Some(&watchdog),
        },
        pool: &pool,
    };

    let mut total = Tally::default();
    for path in &inputs {
        let stem = stem(path);
        let marker = match args.format {
            Format::Parquet => args.out.join("pages").join(format!("{stem}.parquet")),
            Format::Json => args.out.join(format!("{stem}.jsonl")),
        };
        let result = if args.resume && marker.exists() {
            resumed_input(&ctx, path, &stem)
        } else {
            process_input(&ctx, path, &stem)
        };
        match result {
            Ok((entry, outputs, tally)) => {
                manifest.inputs.push(entry);
                manifest.outputs.extend(outputs);
                total.merge(tally);
            }
            Err(e) => {
                eprintln!("error: {}: {e:#}", path.display());
                let failed = FailedInput { path: path.display().to_string(), message: format!("{e:#}") };
                manifest.failed_inputs.push(failed);
            }
        }
    }
    drop(watchdog);
    finish_manifest(&mut manifest, total, started);
    let text = serde_json::to_string_pretty(&manifest)? + "\n";
    write_atomic(&args.out.join("manifest.json"), &text)?;
    Ok(manifest)
}

/// Throughput counts the pages parsed in this run; resumed inputs add to the status counts only.
fn finish_manifest(m: &mut Manifest, tally: Tally, started: Instant) {
    let resumed: usize = m.inputs.iter().filter(|i| i.resumed).map(|i| i.pages).sum();
    let parsed = tally.pages - resumed;
    let secs = started.elapsed().as_secs_f64();
    m.pages_per_second = if secs > 0.0 { parsed as f64 / secs } else { 0.0 };
    m.seconds_over_budget = tally.over_budget;
    m.status_counts = tally.counts;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pages_part_is_renamed_last_whatever_the_table_order() {
        let orders: [&[&str]; 3] =
            [&["pages", "refs", "sections"], &["refs", "pages", "sections"], &["refs", "sections", "pages"]];
        for (i, tables) in orders.iter().enumerate() {
            let d = std::env::temp_dir().join(format!("wtc_sink_{}_{i}", std::process::id()));
            let _ = fs::remove_dir_all(&d);
            let sink = Sink::create(&d, Format::Parquet, tables, "s").unwrap();
            // `finish` returns the outputs in the order their parts were renamed.
            let renamed: Vec<String> = sink.finish("s").unwrap().into_iter().map(|o| o.path).collect();
            assert_eq!(renamed.len(), 3, "{tables:?}");
            assert_eq!(renamed.last().map(String::as_str), Some("pages/s.parquet"), "{tables:?}");
            assert!(renamed.iter().all(|p| d.join(p).exists()), "{tables:?}");
            let _ = fs::remove_dir_all(&d);
        }
    }
}
