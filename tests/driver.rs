//! The `parse_components` driver: CLI, part writers, manifest, resume, failures.
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use sha2::Digest;
use wikitext_components::model::*;
use wikitext_components::output::{self, PartWriter};
use wikitext_components::settings::Settings;

const BIN: &str = env!("CARGO_BIN_EXE_parse_components");
const CORPUS: &str = "tests/corpus/wikipedia_v1.parquet";
const STEM: &str = "wikipedia_v1";
const JSON_KEYS: [&str; 7] = ["page", "sections", "paragraphs", "templates", "refs", "ref_lists", "citations"];
const LABELS: [&str; 3] = [
    "wiki_forkday=wikipedia:forkday",
    "wiki_2025=wikipedia:2025-01-01",
    "deleted_wiki_2025=wikipedia:2025-01-01",
];

/// A fresh, empty scratch directory under the system temp dir.
fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("wtc_driver_{}_{name}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

fn s(p: &Path) -> String {
    p.display().to_string()
}

fn cli(args: &[String]) -> Output {
    Command::new(BIN).args(args).env_remove("WIKITEXT_TEST_SLEEP_PAGE").output().expect("spawn")
}

fn ok(o: Output) -> Output {
    assert!(o.status.success(), "parse_components failed: {}", String::from_utf8_lossy(&o.stderr));
    o
}

fn args(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

fn corpus_args(out: &Path, extra: &[&str]) -> Vec<String> {
    let mut a = args(&["--input", CORPUS, "--out", &s(out)]);
    for l in LABELS {
        a.extend(args(&["--label", l]));
    }
    a.extend(args(extra));
    a
}

fn corpus_run(out: &Path, extra: &[&str]) -> Output {
    ok(cli(&corpus_args(out, extra)))
}

fn part(out: &Path, table: &str, stem: &str) -> PathBuf {
    out.join(table).join(format!("{stem}.parquet"))
}

fn read_all(path: &Path) -> (Arc<Schema>, Vec<RecordBatch>) {
    let b = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let schema = b.schema().clone();
    (schema, b.build().unwrap().map(|x| x.unwrap()).collect())
}

fn strings(a: &ArrayRef) -> Vec<String> {
    let c = cast(a, &DataType::Utf8).unwrap();
    let c = c.as_any().downcast_ref::<StringArray>().unwrap();
    c.iter().map(|x| x.unwrap().to_string()).collect()
}

/// One column of a part file as strings (dictionary columns decoded).
fn column(path: &Path, name: &str) -> Vec<String> {
    let (_, batches) = read_all(path);
    batches.iter().flat_map(|b| strings(b.column(b.schema().index_of(name).unwrap()))).collect()
}

fn manifest(out: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(out.join("manifest.json")).unwrap()).unwrap()
}

fn dirs(out: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(out)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

fn write_parquet(path: &Path, cols: Vec<(&str, ArrayRef)>) {
    let fields: Vec<Field> =
        cols.iter().map(|(n, a)| Field::new(*n, a.data_type().clone(), true)).collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), cols.into_iter().map(|(_, a)| a).collect()).unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn texts(path: &Path, ids: &[&str], texts: &[&str]) {
    write_parquet(path, vec![
        ("page_id", Arc::new(StringArray::from(ids.to_vec())) as ArrayRef),
        ("text", Arc::new(StringArray::from(texts.to_vec()))),
    ]);
}

#[test]
fn output_is_identical_for_one_and_four_jobs() {
    let a = tmp("jobs1");
    let b = tmp("jobs4");
    corpus_run(&a, &["--jobs", "1"]);
    corpus_run(&b, &["--jobs", "4"]);
    for t in output::TABLES {
        let (x, y) = (part(&a, t, STEM), part(&b, t, STEM));
        assert!(x.exists() && y.exists(), "{t} part file missing");
        if t == "timings" {
            continue;
        }
        assert!(fs::read(&x).unwrap() == fs::read(&y).unwrap(), "{t} differs between --jobs 1 and 4");
    }
    let _ = fs::remove_dir_all(&a);
    let _ = fs::remove_dir_all(&b);
}

#[test]
fn every_input_row_yields_one_pages_row_per_text() {
    let out = tmp("rows");
    corpus_run(&out, &[]);
    let pages = part(&out, "pages", STEM);
    let rows: Vec<i32> = column(&pages, "row").iter().map(|r| r.parse().unwrap()).collect();
    assert_eq!(rows.len(), 180);
    assert_eq!(rows, (0..180).collect::<Vec<i32>>(), "pages rows in input order");
    assert_eq!(column(&part(&out, "timings", STEM), "row").len(), 180);
    let statuses = column(&pages, "status");
    assert!(statuses.iter().all(|s| !s.is_empty()));
    let _ = fs::remove_dir_all(&out);
}

#[test]
fn resume_skips_completed_inputs() {
    let out = tmp("resume");
    corpus_run(&out, &[]);
    let pages = part(&out, "pages", STEM);
    let sections = part(&out, "sections", STEM);
    let before = fs::metadata(&pages).unwrap().modified().unwrap();
    fs::remove_file(&sections).unwrap();
    corpus_run(&out, &["--resume"]);
    assert_eq!(fs::metadata(&pages).unwrap().modified().unwrap(), before, "pages rewritten");
    assert!(!sections.exists(), "sections recreated on resume");
    let m = manifest(&out);
    assert_eq!(m["inputs"][0]["resumed"], true);
    assert_eq!(m["inputs"][0]["rows"], 180);
    let total: u64 = m["status_counts"]["wikipedia"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|v| v.as_object().unwrap().values())
        .map(|n| n.as_u64().unwrap())
        .sum();
    assert_eq!(total, 180, "status counts of a resumed input come from its pages file");
    let _ = fs::remove_dir_all(&out);
}

#[test]
fn wiki_file_to_json() {
    let d = tmp("json");
    let out = d.join("x.json");
    ok(cli(&args(&[
        "--input", "tests/fixtures/refs_basic.wiki", "--label", "wikipedia:adhoc", "--format", "json",
        "--out", &s(&out),
    ])));
    let v: Value = serde_json::from_str(&fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(v["page"]["status"], "ok");
    assert_eq!(v["page"]["site"], "wikipedia");
    assert_eq!(v["page"]["version"], "adhoc");
    assert_eq!(v["page"]["page_id"], "refs_basic");
    let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(keys, JSON_KEYS, "keys in the documented order");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn parquet_input_to_jsonl() {
    let d = tmp("jsonl");
    let input = d.join("three.parquet");
    texts(&input, &["1", "2", "3"], &["a", "", "== H ==\nb"]);
    let out = d.join("out");
    ok(cli(&args(&["--input", &s(&input), "--label", "text=wikipedia:v", "--format", "json", "--out", &s(&out)])));
    let lines: Vec<Value> = fs::read_to_string(out.join("three.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let st: Vec<&str> = lines.iter().map(|v| v["page"]["status"].as_str().unwrap()).collect();
    assert_eq!(st, ["ok", "empty", "ok"]);
    assert!(lines.iter().all(|v| v.get("timing").is_none()));
    assert_eq!(manifest(&out)["outputs"][0]["rows"], 3);
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn manifest_records_hashes_and_counts() {
    let out = tmp("manifest");
    corpus_run(&out, &["--jobs", "4"]);
    let m = manifest(&out);
    let ru = Settings::load(Path::new("config/sites/ru.wikipedia.toml")).unwrap();
    assert_eq!(m["settings_hashes"]["wikipedia"], ru.hash());
    assert!(m["status_counts"]["wikipedia"]["forkday"]["ok"].as_u64().unwrap() > 0);
    assert_eq!(m["schema_version"], SCHEMA_VERSION);
    assert_eq!(m["crate_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(m["vendored_upstream"], "bc6bde5");
    assert!(!m["git_commit"].as_str().unwrap().is_empty());
    let mut reg_bytes = fs::read("config/registry/templates.toml").unwrap();
    reg_bytes.extend(fs::read("config/registry/sections.toml").unwrap());
    let want: String = sha2::Sha256::digest(&reg_bytes).iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(want.len(), 64);
    assert_eq!(m["registry_hash"], want);
    assert_eq!(m["args"]["jobs"], 4);
    assert_eq!(m["inputs"][0]["path"], CORPUS);
    assert_eq!(m["inputs"][0]["rows"], 180);
    let outputs = m["outputs"].as_array().unwrap();
    assert_eq!(outputs.len(), output::TABLES.len());
    let pages = outputs.iter().find(|o| o["path"] == format!("pages/{STEM}.parquet")).unwrap();
    assert_eq!(pages["rows"], 180);
    assert!(m["failed_inputs"].as_array().unwrap().is_empty());
    assert!(m["pages_per_second"].as_f64().unwrap() > 0.0);
    assert_eq!(outputs.last().unwrap()["path"], format!("pages/{STEM}.parquet"), "outputs in rename order, pages last");
    // R19: the vendored parser's patches and a hash of its source, for builds without git.
    let patches: Vec<&str> = m["vendored_patches"].as_array().unwrap().iter().map(|p| p.as_str().unwrap()).collect();
    assert!(!patches.is_empty());
    assert!(patches.iter().any(|p| p.to_lowercase().contains("budget")), "{patches:?}");
    assert!(patches.iter().all(|p| !p.is_empty() && !p.ends_with('.')), "{patches:?}");
    assert_eq!(m["vendored_src_sha256"], vendored_src_sha256());
    assert!(m["seconds_over_budget"].as_f64().unwrap() >= 0.0);
    let _ = fs::remove_dir_all(&out);
}

/// sha256 over vendor/parse-wiki-text-2/src/: each file in sorted relative-path order, its path
/// (relative to that directory, `/`-separated) then its contents.
fn vendored_src_sha256() -> String {
    use sha2::{Digest, Sha256};
    fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, PathBuf)>) {
        for e in fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, base, out);
            } else {
                let rel = p.strip_prefix(base).unwrap().iter().map(|c| c.to_str().unwrap()).collect::<Vec<_>>().join("/");
                out.push((rel, p));
            }
        }
    }
    let base = Path::new("vendor/parse-wiki-text-2/src");
    let mut files = vec![];
    walk(base, base, &mut files);
    files.sort();
    let mut h = Sha256::new();
    for (rel, p) in files {
        h.update(rel.as_bytes());
        h.update(fs::read(p).unwrap());
    }
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn tables_subset_always_writes_pages() {
    let d = tmp("subset");
    let out = d.join("out");
    ok(cli(&args(&[
        "--input", "tests/fixtures/refs_basic.wiki", "--label", "wikipedia:adhoc", "--tables", "refs",
        "--out", &s(&out),
    ])));
    assert_eq!(dirs(&out), ["pages", "refs"]);
    assert!(part(&out, "pages", "refs_basic").exists());
    assert!(part(&out, "refs", "refs_basic").exists());
    let e = cli(&args(&[
        "--input", "tests/fixtures/refs_basic.wiki", "--label", "wikipedia:adhoc", "--tables", "nope",
        "--out", &s(&d.join("out2")),
    ]));
    assert!(!e.status.success());
    assert!(String::from_utf8_lossy(&e.stderr).contains("unknown table 'nope'"));
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn duplicate_label_key_fails_before_any_output() {
    let d = tmp("duplicate_label");
    let out = d.join("out");
    let o = cli(&args(&[
        "--input", CORPUS, "--out", &s(&out), "--label", LABELS[0], "--label", LABELS[1], "--label", LABELS[2],
        "--label", "wiki_2025=ruwiki:2025-01-01",
    ]));
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("--label 'wiki_2025' is given twice"), "{err}");
    assert!(!out.exists(), "output written before the label check");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn unknown_site_fails_before_any_output() {
    let d = tmp("unknown_site");
    let out = d.join("out");
    let o = cli(&args(&[
        "--input", CORPUS, "--out", &s(&out), "--label", "wiki_forkday=nosuch:forkday",
        "--label", LABELS[1], "--label", LABELS[2],
    ]));
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("unknown site 'nosuch' (no config/sites file)"), "{err}");
    assert!(!out.exists(), "output written before the label check");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn missing_or_invalid_registry_fails_before_any_output() {
    let d = tmp("bad_registry");
    let cfg = d.join("config");
    fs::create_dir_all(cfg.join("sites")).unwrap();
    fs::copy("config/sites/ru.wikipedia.toml", cfg.join("sites").join("a.toml")).unwrap();
    let out = d.join("out");
    let run = || {
        cli(&args(&[
            "--input", "tests/fixtures/refs_basic.wiki", "--label", "wikipedia:adhoc", "--out", &s(&out),
            "--config-dir", &s(&cfg),
        ]))
    };
    let o = run();
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("registry") && err.contains("templates.toml"), "{err}");
    assert!(!out.exists());
    fs::create_dir_all(cfg.join("registry")).unwrap();
    fs::write(cfg.join("registry").join("templates.toml"), "[[template]]\nname = \"a\"\nrole = \"bogus\"\n").unwrap();
    fs::write(cfg.join("registry").join("sections.toml"), "").unwrap();
    let o = run();
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("bogus") && err.contains("templates.toml"), "{err}");
    assert!(!out.exists());
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn duplicate_site_key_is_an_error() {
    let d = tmp("dup_site");
    let sites = d.join("config").join("sites");
    fs::create_dir_all(&sites).unwrap();
    fs::copy("config/sites/ru.wikipedia.toml", sites.join("a.toml")).unwrap();
    fs::copy("config/sites/ru.wikipedia.toml", sites.join("b.toml")).unwrap();
    let out = d.join("out");
    let o = cli(&args(&[
        "--input", "tests/fixtures/refs_basic.wiki", "--label", "wikipedia:adhoc", "--out", &s(&out),
        "--config-dir", &s(&d.join("config")),
    ]));
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("duplicate site 'wikipedia'"), "{err}");
    assert!(!out.exists());
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn failed_input_is_recorded_left_as_tmp_and_the_next_runs() {
    let d = tmp("failed");
    let dir = d.join("in");
    fs::create_dir_all(&dir).unwrap();
    // a_bad: a value's length prefix made absurd, so reading fails after open succeeds.
    let bad = dir.join("a_bad.parquet");
    texts(&bad, &["1", "2"], &["QQQQQQQQQQ", "RRRRRRRRRR"]);
    let mut bytes = fs::read(&bad).unwrap();
    let pos = bytes.windows(10).position(|w| w == b"QQQQQQQQQQ").unwrap();
    for b in &mut bytes[pos - 4..pos] {
        *b = 0xFF;
    }
    fs::write(&bad, bytes).unwrap();
    texts(&dir.join("b_good.parquet"), &["7", "8"], &["x", "y"]);
    fs::write(dir.join("c_ignored.wiki"), "not read from a directory").unwrap();

    let out = d.join("out");
    let o = cli(&args(&["--input", &s(&dir), "--label", "text=wikipedia:v", "--out", &s(&out)]));
    assert!(!o.status.success(), "a failed input must give a non-zero exit");
    assert!(String::from_utf8_lossy(&o.stderr).contains("a_bad.parquet"));
    assert!(out.join("pages").join("a_bad.parquet.tmp").exists());
    assert!(!part(&out, "pages", "a_bad").exists());
    assert_eq!(column(&part(&out, "pages", "b_good"), "page_id"), ["7", "8"]);
    assert!(!part(&out, "pages", "c_ignored").exists());
    let m = manifest(&out);
    let failed = m["failed_inputs"].as_array().unwrap();
    assert_eq!(failed.len(), 1);
    assert!(failed[0]["path"].as_str().unwrap().ends_with("a_bad.parquet"));
    assert!(!failed[0]["message"].as_str().unwrap().is_empty());
    let inputs: Vec<&str> = m["inputs"].as_array().unwrap().iter().map(|i| i["path"].as_str().unwrap()).collect();
    assert_eq!(inputs.len(), 1);
    assert!(inputs[0].ends_with("b_good.parquet"));
    let _ = fs::remove_dir_all(&d);
}

// ---- R11: every table's batches and part files carry exactly `output::schema(table)`.

fn key(row: i32) -> Key {
    Key {
        source_file: "sample".into(),
        row,
        page_id: format!("p{row}"),
        site: "wikipedia".into(),
        version: "v".into(),
        column: "wikitext".into(),
    }
}

fn param(i: i32) -> ParamRow {
    ParamRow { name: format!("n{i}"), positional: i == 0, value_raw: "[[x]]".into(), value_text: "x".into(), start: i, end: i + 1 }
}

macro_rules! common {
    ($id:expr) => {
        (
            $id,
            10 * $id,
            10 * $id + 5,
            10 * $id,
            10 * $id + 5,
            0,
            Some(1),
            Some(2),
            Some("templates".to_string()),
            Some(0),
            $id % 2 == 1,
            format!("{:016x}", $id),
        )
    };
}

fn section(id: i32) -> SectionRow {
    let (id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp) = common!(id);
    SectionRow {
        id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp,
        level: 2, title_text: "История".into(), title_raw: "История".into(), title_norm: "история".into(),
        path: vec!["история".into()], parent_section_id: 0, heading_start: Some(0), heading_end: None,
        subtree_end: 99, role: "content".into(),
    }
}

fn sample(row: i32, n: i32) -> PageOutput {
    let k = key(row);
    let ids = 0..n;
    let page = PageRow {
        source_file: k.source_file.clone(), row, page_id: k.page_id.clone(), site: k.site.clone(),
        version: k.version.clone(), column: k.column.clone(), title: Some("T".into()), timestamp: None,
        status: "ok".into(), status_detail: None, parse_steps: 7, n_bytes: 3, n_chars: 3, n_warnings: 1,
        warning_counts: vec![WarningCount { kind: "Unclosed".into(), n: 1 }],
        warnings: vec![WarningRow { kind: "Unclosed".into(), start: 0, end: 1 }], redirect_target: None,
        n_sections: n, n_paragraphs: n, n_templates: n, n_refs: n, n_ref_lists: n, n_citations: n,
    };
    let timing = TimingRow {
        source_file: k.source_file.clone(), row, page_id: k.page_id.clone(), site: k.site.clone(),
        version: k.version.clone(), column: k.column.clone(), parse_ms: 1.5, walk_ms: 0.25,
    };
    let components = Components {
        sections: ids.clone().map(section).collect(),
        paragraphs: ids.clone().map(|i| {
            let (id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp) = common!(i);
            ParagraphRow {
                id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp,
                kind: "prose".into(), list_id: None, item_depth: Some(1), text: "Текст".into(), n_chars: 5,
            }
        }).collect(),
        templates: ids.clone().map(|i| {
            let (id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp) = common!(i);
            TemplateRow {
                id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp,
                name_raw: "Cite web".into(), name: "cite web".into(), kind: "template".into(), role: "citation".into(),
                looks_like_infobox: false, depth: 0, rendered_len: 0, params: vec![param(0), param(1)],
            }
        }).collect(),
        refs: ids.clone().map(|i| {
            let (id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp) = common!(i);
            RefRow {
                id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp,
                kind: "tag".into(), name: Some("a".into()), group: "".into(), body_raw: "b".into(), body_text: "b".into(),
                def_ref_id: None, n_uses: Some(2), resolved: true, harv_key: None, cites_citation_id: Some(0),
            }
        }).collect(),
        ref_lists: ids.clone().map(|i| {
            let (id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp) = common!(i);
            RefListRow {
                id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp,
                group: "".into(), kind: "tag".into(), template_id: None, n_entries: 3,
            }
        }).collect(),
        citations: ids.map(|i| {
            let (id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp) = common!(i);
            CitationRow {
                id, start, end, start_char, end_char, section_id, para_id, para_offset, parent_type, parent_id, fallback, fp,
                context: "ref".into(), ref_id: Some(0), family: "web".into(), template_id: Some(0),
                template_name: Some("cite web".into()), params: vec![param(0)], url: Some("https://example.org/a".into()),
                archive_url: None, archive_date_raw: None, host: Some("example.org".into()),
                registrable_domain: Some("example.org".into()), dead_link: false, title: Some("A".into()),
                authors: vec!["X".into(), "Y".into()], date_raw: None, year: Some(2020), publisher: None, work: None,
                language: Some("ru".into()), isbn: None, doi: None, access_date_raw: None, harv_key: None, text: None,
            }
        }).collect(),
    };
    PageOutput { page, timing, components }
}

fn rows_of_table(table: &str, pages: &[PageOutput]) -> usize {
    pages
        .iter()
        .map(|p| {
            let c = &p.components;
            match table {
                "pages" | "timings" => 1,
                "sections" => c.sections.len(),
                "paragraphs" => c.paragraphs.len(),
                "templates" => c.templates.len(),
                "refs" => c.refs.len(),
                "ref_lists" => c.ref_lists.len(),
                "citations" => c.citations.len(),
                t => panic!("{t}"),
            }
        })
        .sum()
}

/// Row values read back from a part file equal the rows written.
fn assert_round_trip(table: &str, batch: &RecordBatch, pages: &[PageOutput]) {
    use serde_arrow::from_record_batch;
    let own = batch.project(&(6..batch.num_columns()).collect::<Vec<_>>()).unwrap();
    let flat = |f: fn(&Components) -> Vec<Value>| -> Vec<Value> { pages.iter().flat_map(|p| f(&p.components)).collect() };
    fn vals<T: serde::Serialize>(v: &[T]) -> Vec<Value> {
        v.iter().map(|x| serde_json::to_value(x).unwrap()).collect()
    }
    let (got, want): (Vec<Value>, Vec<Value>) = match table {
        "pages" => (vals(&from_record_batch::<Vec<PageRow>>(batch).unwrap()), vals(&pages.iter().map(|p| p.page.clone()).collect::<Vec<_>>())),
        "timings" => (vals(&from_record_batch::<Vec<TimingRow>>(batch).unwrap()), vals(&pages.iter().map(|p| p.timing.clone()).collect::<Vec<_>>())),
        "sections" => (vals(&from_record_batch::<Vec<SectionRow>>(&own).unwrap()), flat(|c| vals(&c.sections))),
        "paragraphs" => (vals(&from_record_batch::<Vec<ParagraphRow>>(&own).unwrap()), flat(|c| vals(&c.paragraphs))),
        "templates" => (vals(&from_record_batch::<Vec<TemplateRow>>(&own).unwrap()), flat(|c| vals(&c.templates))),
        "refs" => (vals(&from_record_batch::<Vec<RefRow>>(&own).unwrap()), flat(|c| vals(&c.refs))),
        "ref_lists" => (vals(&from_record_batch::<Vec<RefListRow>>(&own).unwrap()), flat(|c| vals(&c.ref_lists))),
        "citations" => (vals(&from_record_batch::<Vec<CitationRow>>(&own).unwrap()), flat(|c| vals(&c.citations))),
        t => panic!("{t}"),
    };
    assert_eq!(got, want, "{table}: values read back differ");
}

#[test]
fn every_table_batch_and_part_file_carries_its_schema() {
    let d = tmp("schemas");
    // Page 0 has two rows per component table, page 1 none, page 2 one.
    let pages = vec![sample(0, 2), sample(1, 0), sample(2, 1)];
    for t in output::TABLES {
        let batch = output::record_batch(t, &pages).unwrap();
        assert_eq!(batch.schema(), output::schema(t), "{t}: batch schema");
        assert_eq!(batch.num_rows(), rows_of_table(t, &pages), "{t}");

        let mut w = PartWriter::create(&d, t, "sample").unwrap();
        w.append(&pages[..1]).unwrap();
        w.append(&pages[1..]).unwrap();
        assert!(d.join(t).join("sample.parquet.tmp").exists());
        assert_eq!(w.finish().unwrap(), rows_of_table(t, &pages));
        let path = part(&d, t, "sample");
        assert!(path.exists() && !d.join(t).join("sample.parquet.tmp").exists(), "{t}: not renamed");

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        assert_eq!(reader.schema(), &output::schema(t), "{t}: schema read back");
        let kv = reader.metadata().file_metadata().key_value_metadata().unwrap();
        let v = kv.iter().find(|k| k.key == "wikitext_parser.schema_version").and_then(|k| k.value.clone());
        assert_eq!(v.as_deref(), Some(SCHEMA_VERSION), "{t}: key-value metadata");
        let batches: Vec<RecordBatch> = reader.build().unwrap().map(|b| b.unwrap()).collect();
        let all = arrow::compute::concat_batches(&output::schema(t), &batches).unwrap();
        assert_round_trip(t, &all, &pages);
        if !matches!(t, "pages" | "timings") {
            // Keys repeat once per row of their page, in page order.
            let rows = all.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(rows.values().to_vec(), vec![0, 0, 2], "{t}: key rows");
            assert_eq!(strings(all.column(2)), ["p0", "p0", "p2"]);
            assert_eq!(strings(all.column(0)), ["sample"; 3]);
        }
    }
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn page_json_has_the_documented_shape() {
    let p = sample(0, 1);
    let text = output::page_json(&p);
    assert!(text.starts_with("{\n  \"page\": {"), "page is the first key, pretty-printed");
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["page"]["page_id"], "p0");
    assert_eq!(v["sections"][0]["title_text"], "История");
    assert_eq!(v["citations"][0]["authors"][1], "Y");
    assert!(v.get("timing").is_none());
    let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(keys, JSON_KEYS);
    // Row fields in column order: the table's schema without the six keys (pages keeps them).
    let fields = |t: &str, skip: usize| -> Vec<String> {
        output::schema(t).fields().iter().skip(skip).map(|f| f.name().clone()).collect()
    };
    let keys_of = |v: &Value| -> Vec<String> { v.as_object().unwrap().keys().cloned().collect() };
    assert_eq!(keys_of(&v["page"]), fields("pages", 0));
    for t in &JSON_KEYS[1..] {
        assert_eq!(keys_of(&v[*t][0]), fields(t, 6), "{t}: keys in column order");
    }
    assert_eq!(keys_of(&v["templates"][0]["params"][0]), ["name", "positional", "value_raw", "value_text", "start", "end"]);
}
