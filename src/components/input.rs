//! Input layouts: any supported Parquet file, or a `.wiki` file, becomes a stream of `PageInput`s,
//! each with an explicit site and version label.
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;

use crate::model::Key;
use crate::page::PageInput;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    Pair,
    Single,
    Long,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Label {
    pub key: String,
    pub site: String,
    pub version: String,
}

/// `key=site:version`, or `site:version` (empty key) for a `.wiki` file.
pub fn parse_label(s: &str) -> Result<Label> {
    let (key, rest) = match s.split_once('=') {
        Some((k, r)) => (k, r),
        None => ("", s),
    };
    match rest.split_once(':') {
        Some((site, version)) if !site.is_empty() && !version.is_empty() => {
            if s.contains('=') && key.is_empty() {
                bail!("bad --label '{s}': empty key");
            }
            Ok(Label { key: key.to_string(), site: site.to_string(), version: version.to_string() })
        }
        _ => bail!("bad --label '{s}': expected key=site:version"),
    }
}

pub fn detect_layout(schema: &Schema) -> Result<Layout> {
    let has = |n: &str| schema.field_with_name(n).is_ok();
    if has("version") && has("wikitext") {
        Ok(Layout::Long)
    } else if has("official_text") && has("clone_text") {
        Ok(Layout::Pair)
    } else if detect_text_column(schema).is_some() {
        Ok(Layout::Single)
    } else {
        bail!("cannot detect the layout: no text column found")
    }
}

// Copied from src/parse_single.rs (the legacy file stays untouched).
fn detect_text_column(schema: &Schema) -> Option<String> {
    for candidate in ["text", "content", "official_text", "clone_text"] {
        if schema.field_with_name(candidate).is_ok() {
            return Some(candidate.to_string());
        }
    }
    schema
        .fields()
        .iter()
        .find(|f| f.name().to_lowercase().contains("text"))
        .map(|f| f.name().clone())
}

fn detect_pageid_column(schema: &Schema) -> Option<String> {
    ["page_id", "pageid"].into_iter().find(|c| schema.field_with_name(c).is_ok()).map(String::from)
}

fn detect_title_column(schema: &Schema) -> Option<String> {
    ["page_title", "title"].into_iter().find(|c| schema.field_with_name(c).is_ok()).map(String::from)
}

/// One text column to emit: where its title and timestamp live, and its label.
#[derive(Clone, Debug)]
struct TextSpec {
    column: String,
    title: Option<String>,
    timestamp: Option<String>,
    site: String,
    version: String,
}

pub struct InputFile {
    path: std::path::PathBuf,
    stem: String,
    layout: Layout,
    page_id: String,
    /// Pair and single: the columns to emit. Long: one entry holding the shared columns.
    specs: Vec<TextSpec>,
    /// Long only: version value -> (site, version).
    versions: HashMap<String, (String, String)>,
    /// Long only: the `version` column.
    version_column: String,
}

/// Fails when two labels share a key: which of them a text takes would be a guess. Labels without a key
/// (`.wiki` inputs) are checked by the driver, which takes exactly one.
pub fn check_unique_keys(labels: &[Label]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for l in labels.iter().filter(|l| !l.key.is_empty()) {
        if !seen.insert(l.key.as_str()) {
            bail!("--label '{}' is given twice; give each key one label", l.key);
        }
    }
    Ok(())
}

fn find_label<'a>(labels: &'a [Label], key: &str) -> Result<&'a Label> {
    labels.iter().find(|l| l.key == key).ok_or_else(|| anyhow!("no --label for '{key}'"))
}

fn check_unmatched(labels: &[Label], known: &BTreeSet<String>) -> Result<()> {
    for l in labels {
        if !known.contains(&l.key) {
            bail!("--label '{}' matches no column or version", l.key);
        }
    }
    Ok(())
}

fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8)
}

fn require_string(schema: &Schema, name: &str) -> Result<()> {
    let f = schema.field_with_name(name).map_err(|_| anyhow!("column '{name}' not found"))?;
    if !is_string(f.data_type()) {
        bail!("column '{name}' has unsupported type {}", f.data_type());
    }
    Ok(())
}

fn require_timestamp(schema: &Schema, name: &str) -> Result<()> {
    let f = schema.field_with_name(name).map_err(|_| anyhow!("column '{name}' not found"))?;
    match f.data_type() {
        DataType::Timestamp(..) => Ok(()),
        t if is_string(t) => Ok(()),
        t => bail!("column '{name}' has unsupported type {t}"),
    }
}

fn require_pageid(schema: &Schema, name: &str) -> Result<()> {
    let f = schema.field_with_name(name).map_err(|_| anyhow!("column '{name}' not found"))?;
    match f.data_type() {
        t if is_string(t) || t.is_integer() => Ok(()),
        t => bail!("column '{name}' has unsupported type {t}"),
    }
}

fn opt_if_present(schema: &Schema, name: &str) -> Option<String> {
    schema.field_with_name(name).ok().map(|_| name.to_string())
}

/// The optional title and timestamp columns of one text column, type-checked.
fn aux_columns(
    schema: &Schema,
    title: Option<String>,
    timestamp: Option<String>,
) -> Result<(Option<String>, Option<String>)> {
    if let Some(t) = &title {
        require_string(schema, t)?;
    }
    if let Some(t) = &timestamp {
        require_timestamp(schema, t)?;
    }
    Ok((title, timestamp))
}

pub fn open(
    path: &Path,
    layout: Option<Layout>,
    text_column: Option<&str>,
    labels: &[Label],
) -> Result<InputFile> {
    check_unique_keys(labels)?;
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading {}", path.display()))?;
    let schema = builder.schema().clone();
    let layout = match layout {
        Some(l) => l,
        None => detect_layout(&schema)?,
    };
    let stem = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    let page_id = detect_pageid_column(&schema).ok_or_else(|| anyhow!("no page_id column found"))?;
    require_pageid(&schema, &page_id)?;

    let mut input = InputFile {
        path: path.to_path_buf(),
        stem,
        layout,
        page_id,
        specs: vec![],
        versions: HashMap::new(),
        version_column: "version".to_string(),
    };

    match layout {
        Layout::Pair => {
            for (col, title, ts) in [
                ("official_text", "page_title", "official_timestamp"),
                ("clone_text", "clone_page_title", "clone_timestamp"),
            ] {
                require_string(&schema, col)?;
                let (title, ts) = aux_columns(
                    &schema,
                    opt_if_present(&schema, title),
                    opt_if_present(&schema, ts),
                )?;
                let l = find_label(labels, col)?;
                input.specs.push(TextSpec {
                    column: col.to_string(),
                    title,
                    timestamp: ts,
                    site: l.site.clone(),
                    version: l.version.clone(),
                });
            }
            let known: BTreeSet<String> =
                ["official_text", "clone_text"].into_iter().map(String::from).collect();
            check_unmatched(labels, &known)?;
        }
        Layout::Single => {
            let col = match text_column {
                Some(c) => c.to_string(),
                None => detect_text_column(&schema).ok_or_else(|| anyhow!("no text column found"))?,
            };
            if schema.field_with_name(&col).is_err() {
                bail!("text column '{col}' not found in schema");
            }
            require_string(&schema, &col)?;
            let (title, ts) = aux_columns(
                &schema,
                detect_title_column(&schema),
                opt_if_present(&schema, "timestamp"),
            )?;
            let l = find_label(labels, &col)?;
            input.specs.push(TextSpec {
                column: col.clone(),
                title,
                timestamp: ts,
                site: l.site.clone(),
                version: l.version.clone(),
            });
            check_unmatched(labels, &BTreeSet::from([col]))?;
        }
        Layout::Long => {
            let col = text_column.unwrap_or("wikitext").to_string();
            if schema.field_with_name(&col).is_err() {
                bail!("text column '{col}' not found in schema");
            }
            require_string(&schema, &col)?;
            require_string(&schema, "version")?;
            let (title, ts) = aux_columns(
                &schema,
                detect_title_column(&schema),
                opt_if_present(&schema, "timestamp"),
            )?;
            input.specs.push(TextSpec {
                column: col,
                title,
                timestamp: ts,
                site: String::new(),
                version: String::new(),
            });
            // Validate up front: read the `version` column alone.
            let idx = schema.index_of("version")?;
            let mask = ProjectionMask::roots(builder.parquet_schema(), [idx]);
            let reader = builder.with_projection(mask).with_batch_size(65536).build()?;
            let mut distinct = BTreeSet::new();
            let mut row = 0usize;
            for batch in reader {
                let batch = batch?;
                for v in strings(batch.column(0), "version")? {
                    match v {
                        Some(v) => {
                            distinct.insert(v);
                        }
                        None => bail!("null value in version column at row {row}"),
                    }
                    row += 1;
                }
            }
            for v in &distinct {
                let l = find_label(labels, v)?;
                input.versions.insert(v.clone(), (l.site.clone(), l.version.clone()));
            }
            check_unmatched(labels, &distinct)?;
        }
    }
    Ok(input)
}

/// A string-like column as owned values.
fn strings(col: &ArrayRef, name: &str) -> Result<Vec<Option<String>>> {
    match col.data_type() {
        DataType::Utf8 => {
            let a = col.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(a.iter().map(|v| v.map(String::from)).collect())
        }
        DataType::LargeUtf8 => {
            let a = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(a.iter().map(|v| v.map(String::from)).collect())
        }
        t => bail!("column '{name}' has unsupported type {t}"),
    }
}

fn page_ids(col: &ArrayRef, name: &str) -> Result<Vec<Option<String>>> {
    let t = col.data_type();
    if is_string(t) {
        strings(col, name)
    } else if t.is_integer() {
        let s = cast(col, &DataType::Utf8)?;
        strings(&s, name)
    } else {
        bail!("column '{name}' has unsupported type {t}")
    }
}

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// RFC 3339 UTC, whole seconds; sub-second digits only when non-zero.
fn rfc3339(secs: i64, nanos: u32) -> String {
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let s = secs.rem_euclid(86_400);
    let mut out =
        format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60);
    if nanos != 0 {
        let frac = format!("{nanos:09}");
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

fn timestamps(col: &ArrayRef, name: &str) -> Result<Vec<Option<String>>> {
    macro_rules! ts {
        ($ty:ty, $per:expr, $nano:expr) => {{
            let a = col.as_any().downcast_ref::<$ty>().unwrap();
            Ok(a.iter()
                .map(|v| v.map(|v| rfc3339(v.div_euclid($per), (v.rem_euclid($per) * $nano) as u32)))
                .collect())
        }};
    }
    match col.data_type() {
        t if is_string(t) => strings(col, name),
        DataType::Timestamp(TimeUnit::Second, _) => ts!(TimestampSecondArray, 1, 0),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            ts!(TimestampMillisecondArray, 1_000, 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            ts!(TimestampMicrosecondArray, 1_000_000, 1_000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            ts!(TimestampNanosecondArray, 1_000_000_000, 1)
        }
        t => bail!("column '{name}' has unsupported type {t}"),
    }
}

fn column<'b>(batch: &'b arrow::record_batch::RecordBatch, name: &str) -> Result<&'b ArrayRef> {
    Ok(batch.column(batch.schema().index_of(name)?))
}

impl InputFile {
    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// Streams the file in batches of `rows` input rows. A reader or conversion error is yielded
    /// as one `Err` item, after which the iterator ends; the caller must treat it as fatal for
    /// this file. Nothing is swallowed.
    pub fn batches(&mut self, rows: usize) -> impl Iterator<Item = Result<Vec<PageInput>>> + '_ {
        let reader = File::open(&self.path)
            .map_err(anyhow::Error::from)
            .and_then(|f| Ok(ParquetRecordBatchReaderBuilder::try_new(f)?))
            .and_then(|b| Ok(b.with_batch_size(rows.max(1)).build()?));
        let (reader, failed) = match reader {
            Ok(r) => (Some(r), None),
            Err(e) => (None, Some(e)),
        };
        BatchIter { input: self, reader, failed, next_row: 0 }
    }

    fn convert(&self, batch: &arrow::record_batch::RecordBatch, first_row: i32) -> Result<Vec<PageInput>> {
        let n = batch.num_rows();
        let ids = page_ids(column(batch, &self.page_id)?, &self.page_id)?;
        let versions = if self.layout == Layout::Long {
            Some(strings(column(batch, &self.version_column)?, &self.version_column)?)
        } else {
            None
        };
        struct Cols {
            text: Vec<Option<String>>,
            title: Option<Vec<Option<String>>>,
            ts: Option<Vec<Option<String>>>,
        }
        let mut cols = vec![];
        for s in &self.specs {
            cols.push(Cols {
                text: strings(column(batch, &s.column)?, &s.column)?,
                title: s.title.as_ref().map(|t| strings(column(batch, t)?, t)).transpose()?,
                ts: s.timestamp.as_ref().map(|t| timestamps(column(batch, t)?, t)).transpose()?,
            });
        }
        let mut out = Vec::with_capacity(n * self.specs.len());
        for i in 0..n {
            let row = first_row + i as i32;
            // A null page_id flows on as ""; `row` keeps the page apart.
            let page_id = ids[i].clone().unwrap_or_default();
            for (s, c) in self.specs.iter().zip(&mut cols) {
                let (site, version) = match &versions {
                    Some(v) => {
                        let v = v[i].as_deref().ok_or_else(|| anyhow!("null version at row {row}"))?;
                        self.versions.get(v).cloned().ok_or_else(|| anyhow!("no --label for '{v}'"))?
                    }
                    None => (s.site.clone(), s.version.clone()),
                };
                out.push(PageInput {
                    key: Key {
                        source_file: self.stem.clone(),
                        row,
                        page_id: page_id.clone(),
                        site,
                        version,
                        column: s.column.clone(),
                    },
                    title: c.title.as_ref().and_then(|t| t[i].clone()),
                    timestamp: c.ts.as_ref().and_then(|t| t[i].clone()),
                    text: std::mem::take(&mut c.text[i]),
                });
            }
        }
        Ok(out)
    }
}

struct BatchIter<'a> {
    input: &'a InputFile,
    reader: Option<ParquetRecordBatchReader>,
    failed: Option<anyhow::Error>,
    next_row: i32,
}

impl Iterator for BatchIter<'_> {
    type Item = Result<Vec<PageInput>>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.failed.take() {
            return Some(Err(e));
        }
        let batch = match self.reader.as_mut()?.next()? {
            Ok(b) => b,
            Err(e) => {
                self.reader = None;
                return Some(Err(e.into()));
            }
        };
        let first = self.next_row;
        self.next_row += batch.num_rows() as i32;
        let r = self.input.convert(&batch, first);
        if r.is_err() {
            self.reader = None;
        }
        Some(r)
    }
}

/// A single `.wiki` file: `page_id` and `source_file` are the stem.
pub fn wiki_file(path: &Path, label: &Label) -> Result<PageInput> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let stem = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    Ok(PageInput {
        key: Key {
            source_file: stem.clone(),
            row: 0,
            page_id: stem,
            site: label.site.clone(),
            version: label.version.clone(),
            column: "wikitext".to_string(),
        },
        title: None,
        timestamp: None,
        text: Some(text),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_formats() {
        assert_eq!(rfc3339(1_735_689_600, 0), "2025-01-01T00:00:00Z");
        assert_eq!(rfc3339(0, 500_000_000), "1970-01-01T00:00:00.5Z");
        assert_eq!(rfc3339(-1, 0), "1969-12-31T23:59:59Z");
        assert_eq!(rfc3339(1_735_689_600 + 59 * 86_400, 0), "2025-03-01T00:00:00Z");
    }

    #[test]
    fn labels_parse() {
        let l = parse_label("wiki_2025=wikipedia:2025-01-01").unwrap();
        assert_eq!((l.key.as_str(), l.site.as_str(), l.version.as_str()), ("wiki_2025", "wikipedia", "2025-01-01"));
        assert_eq!(parse_label("ruwiki:2025").unwrap().key, "");
        assert!(parse_label("nonsense").is_err());
        assert!(parse_label("k=site:").is_err());
    }
}
