//! Test-corpus access: reads the private v1 file or the public Wikipedia-only file,
//! and writes the public file.

use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub const PRIVATE_V1_SHA256: &str =
    "9800a5a3a11feee64af7bf2ebcaf44d281ea59619b82f81af1a67835ef263d06";

/// (corpus version, site, version label)
pub const LABELS: [(&str, &str, &str); 4] = [
    ("wiki_forkday", "wikipedia", "forkday"),
    ("wiki_2025", "wikipedia", "2025-01-01"),
    ("deleted_wiki_2025", "wikipedia", "2025-01-01"),
    ("ruwiki_2025", "ruwiki", "2025-01-01"),
];

#[derive(Debug, Clone, PartialEq)]
pub struct CorpusRow {
    pub page_id: String,
    pub page_title: String,
    pub version: String,
    pub timestamp: String,
    pub wikitext: String,
    pub n_chars: i64,
    pub case: Option<String>,
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

fn strings(batch: &RecordBatch, name: &str) -> Result<StringArray> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| anyhow!("missing column {name}"))?;
    let cast = cast(col, &DataType::Utf8)?;
    Ok(cast
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("column {name} is not text"))?
        .clone())
}

fn case_from_why(why: &str) -> Option<String> {
    if why == "largest" {
        Some("largest".into())
    } else if why.contains("slow") {
        Some("slow".into())
    } else {
        None
    }
}

/// Reads the v1 file (column `why`) or the public file (column `case`).
pub fn load(path: &Path) -> Result<Vec<CorpusRow>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch?;
        let page_id = strings(&batch, "page_id")?;
        let title = strings(&batch, "page_title")?;
        let version = strings(&batch, "version")?;
        let ts = strings(&batch, "timestamp")?;
        let text = strings(&batch, "wikitext")?;
        let n = cast(
            batch
                .column_by_name("n_chars")
                .ok_or_else(|| anyhow!("missing column n_chars"))?,
            &DataType::Int64,
        )?;
        let n = n.as_any().downcast_ref::<Int64Array>().unwrap();
        let (label, is_why) = match batch.column_by_name("case") {
            Some(_) => (strings(&batch, "case")?, false),
            None => (strings(&batch, "why")?, true),
        };
        for i in 0..batch.num_rows() {
            let case = if label.is_null(i) {
                None
            } else if is_why {
                case_from_why(label.value(i))
            } else {
                Some(label.value(i).to_string())
            };
            rows.push(CorpusRow {
                page_id: page_id.value(i).to_string(),
                page_title: title.value(i).to_string(),
                version: version.value(i).to_string(),
                timestamp: ts.value(i).to_string(),
                wikitext: text.value(i).to_string(),
                n_chars: n.value(i),
                case,
            });
        }
    }
    Ok(rows)
}

/// Writes the non-ruwiki rows, in input order, as one zstd row group.
pub fn write_public(rows: &[CorpusRow], path: &Path) -> Result<()> {
    let rows: Vec<&CorpusRow> = rows
        .iter()
        .filter(|r| !r.version.starts_with("ruwiki"))
        .collect();
    let text = |f: fn(&CorpusRow) -> &str| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| f(r))))
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("page_id", DataType::Utf8, false),
        Field::new("page_title", DataType::Utf8, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("wikitext", DataType::Utf8, false),
        Field::new("n_chars", DataType::Int64, false),
        Field::new("case", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        text(|r| &r.page_id),
        text(|r| &r.page_title),
        text(|r| &r.version),
        text(|r| &r.timestamp),
        text(|r| &r.wikitext),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.n_chars))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.case.as_deref()))),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_size(usize::MAX / 2)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Legacy regression support: inputs for the frozen binaries and the long format
// their outputs are collected in.
// ---------------------------------------------------------------------------

fn utf8_batch(names: &[&str], cols: Vec<Vec<&str>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = cols
        .into_iter()
        .map(|c| Arc::new(StringArray::from_iter_values(c)) as ArrayRef)
        .collect();
    RecordBatch::try_new(schema, arrays).expect("equal-length string columns")
}

/// Input for `parse_single`: `page_id, page_title, text, timestamp`.
pub fn single_fetch(rows: &[&CorpusRow]) -> RecordBatch {
    utf8_batch(
        &["page_id", "page_title", "text", "timestamp"],
        vec![
            rows.iter().map(|r| r.page_id.as_str()).collect(),
            rows.iter().map(|r| r.page_title.as_str()).collect(),
            rows.iter().map(|r| r.wikitext.as_str()).collect(),
            rows.iter().map(|r| r.timestamp.as_str()).collect(),
        ],
    )
}

/// Input for `wikitext_parser_rust`: the two-column comparison format.
pub fn pair_fetch(pairs: &[(&CorpusRow, &CorpusRow)]) -> RecordBatch {
    utf8_batch(
        &[
            "page_id",
            "page_title",
            "official_text",
            "official_timestamp",
            "clone_page_title",
            "clone_text",
            "clone_timestamp",
        ],
        vec![
            pairs.iter().map(|(o, _)| o.page_id.as_str()).collect(),
            pairs.iter().map(|(o, _)| o.page_title.as_str()).collect(),
            pairs.iter().map(|(o, _)| o.wikitext.as_str()).collect(),
            pairs.iter().map(|(o, _)| o.timestamp.as_str()).collect(),
            pairs.iter().map(|(_, c)| c.page_title.as_str()).collect(),
            pairs.iter().map(|(_, c)| c.wikitext.as_str()).collect(),
            pairs.iter().map(|(_, c)| c.timestamp.as_str()).collect(),
        ],
    )
}

/// Pages that have both versions, sorted by `page_id`, without any page where
/// either version is a `slow` case.
pub fn legacy_pairs<'a>(
    rows: &'a [CorpusRow],
    official: &str,
    clone: &str,
) -> Vec<(&'a CorpusRow, &'a CorpusRow)> {
    let find = |v: &str, id: &str| rows.iter().find(|r| r.version == v && r.page_id == id);
    let mut ids: Vec<&str> = rows
        .iter()
        .filter(|r| r.version == official)
        .map(|r| r.page_id.as_str())
        .collect();
    ids.sort();
    ids.dedup();
    ids.into_iter()
        .filter_map(|id| Some((find(official, id)?, find(clone, id)?)))
        .filter(|(o, c)| o.case.as_deref() != Some("slow") && c.case.as_deref() != Some("slow"))
        .collect()
}

/// One legacy output: `binary` in {parse_single, wikitext_parser_rust, export_parsed},
/// `stage` in {dirty, clean, file}.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacyOut {
    pub page_id: String,
    pub version: String,
    pub binary: String,
    pub stage: String,
    pub text: String,
}

pub fn write_batch(batch: &RecordBatch, path: &Path) -> Result<()> {
    let mut writer = ArrowWriter::try_new(File::create(path)?, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

/// Writes `single.parquet`, `pair.parquet` and their `.index` sidecars
/// (`page_id\tversion` and `page_id\tofficial_version\tclone_version`) into `dir`.
pub fn write_legacy_inputs(
    dir: &Path,
    singles: &[&CorpusRow],
    pairs: &[(&CorpusRow, &CorpusRow)],
) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    write_batch(&single_fetch(singles), &dir.join("single.parquet"))?;
    write_batch(&pair_fetch(pairs), &dir.join("pair.parquet"))?;
    let mut s = String::new();
    for r in singles {
        s.push_str(&format!("{}\t{}\n", r.page_id, r.version));
    }
    std::fs::write(dir.join("single.index"), s)?;
    let mut p = String::new();
    for (o, c) in pairs {
        p.push_str(&format!("{}\t{}\t{}\n", o.page_id, o.version, c.version));
    }
    std::fs::write(dir.join("pair.index"), p)?;
    Ok(())
}

/// The inputs for the public corpus: every non-slow version through `parse_single`,
/// `wiki_forkday` (official) against `wiki_2025` (clone) through `wikitext_parser_rust`.
pub fn public_legacy_selection(
    rows: &[CorpusRow],
) -> (Vec<&CorpusRow>, Vec<(&CorpusRow, &CorpusRow)>) {
    let singles = rows
        .iter()
        .filter(|r| r.case.as_deref() != Some("slow"))
        .collect();
    (singles, legacy_pairs(rows, "wiki_forkday", "wiki_2025"))
}

/// The inputs for the private corpus: non-slow ruwiki versions through `parse_single`,
/// `wiki_2025` (official, from `public`) against `ruwiki_2025` (clone) as pairs.
pub fn private_legacy_selection<'a>(
    public: &'a [CorpusRow],
    ruwiki: &'a [CorpusRow],
) -> (Vec<&'a CorpusRow>, Vec<(&'a CorpusRow, &'a CorpusRow)>) {
    let singles = ruwiki
        .iter()
        .filter(|r| r.case.as_deref() != Some("slow"))
        .collect();
    let mut pairs = Vec::new();
    for c in ruwiki.iter().filter(|r| r.case.as_deref() != Some("slow")) {
        if let Some(o) = public
            .iter()
            .find(|r| r.version == "wiki_2025" && r.page_id == c.page_id)
        {
            if o.case.as_deref() != Some("slow") {
                pairs.push((o, c));
            }
        }
    }
    pairs.sort_by(|a, b| a.0.page_id.cmp(&b.0.page_id));
    (singles, pairs)
}

fn read_columns(path: &Path, names: &[&str]) -> Result<Vec<Vec<String>>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out = vec![Vec::new(); names.len()];
    for batch in reader {
        let batch = batch?;
        for (k, name) in names.iter().enumerate() {
            let col = strings(&batch, name)?;
            for i in 0..col.len() {
                if col.is_null(i) {
                    return Err(anyhow!("null in column {name} of {}", path.display()));
                }
                out[k].push(col.value(i).to_string());
            }
        }
    }
    Ok(out)
}

fn index(path: &Path) -> Result<Vec<Vec<String>>> {
    Ok(std::fs::read_to_string(path)?
        .lines()
        .map(|l| l.split('\t').map(String::from).collect())
        .collect())
}

/// Reads what the legacy binaries wrote under `dir` (see `write_legacy_inputs`):
/// `single.dirty.parquet`, `single.clean.parquet`, `pair.dirty.parquet`,
/// `pair.clean.parquet`, `export/official/<id>_official.txt`, `export/clone/<id>_clone.txt`.
/// Sorted by (page_id, version, binary, stage).
pub fn collect_legacy(dir: &Path) -> Result<Vec<LegacyOut>> {
    let mut out = Vec::new();
    let mut push = |page_id: &str, version: &str, binary: &str, stage: &str, text: String| {
        out.push(LegacyOut {
            page_id: page_id.into(),
            version: version.into(),
            binary: binary.into(),
            stage: stage.into(),
            text,
        })
    };
    let single_idx = index(&dir.join("single.index"))?;
    for stage in ["dirty", "clean"] {
        let cols = read_columns(
            &dir.join(format!("single.{stage}.parquet")),
            &["page_id", "text_parsed"],
        )?;
        if cols[0].len() != single_idx.len() {
            return Err(anyhow!("single.{stage}: row count differs from index"));
        }
        for (i, ix) in single_idx.iter().enumerate() {
            if cols[0][i] != ix[0] {
                return Err(anyhow!("single.{stage}: row {i} page_id differs from index"));
            }
            push(&ix[0], &ix[1], "parse_single", stage, cols[1][i].clone());
        }
    }
    let pair_idx = index(&dir.join("pair.index"))?;
    for stage in ["dirty", "clean"] {
        let cols = read_columns(
            &dir.join(format!("pair.{stage}.parquet")),
            &["page_id", "official_text_paragraphs", "clone_text_paragraphs"],
        )?;
        if cols[0].len() != pair_idx.len() {
            return Err(anyhow!("pair.{stage}: row count differs from index"));
        }
        for (i, ix) in pair_idx.iter().enumerate() {
            if cols[0][i] != ix[0] {
                return Err(anyhow!("pair.{stage}: row {i} page_id differs from index"));
            }
            push(&ix[0], &ix[1], "wikitext_parser_rust", stage, cols[1][i].clone());
            push(&ix[0], &ix[2], "wikitext_parser_rust", stage, cols[2][i].clone());
        }
    }
    for ix in &pair_idx {
        let o = std::fs::read_to_string(dir.join(format!("export/official/{}_official.txt", ix[0])))?;
        let c = std::fs::read_to_string(dir.join(format!("export/clone/{}_clone.txt", ix[0])))?;
        push(&ix[0], &ix[1], "export_parsed", "file", o);
        push(&ix[0], &ix[2], "export_parsed", "file", c);
    }
    sort_legacy(&mut out);
    Ok(out)
}

pub fn sort_legacy(out: &mut [LegacyOut]) {
    out.sort_by(|a, b| {
        (&a.page_id, &a.version, &a.binary, &a.stage)
            .cmp(&(&b.page_id, &b.version, &b.binary, &b.stage))
    });
}

pub fn write_legacy_long(rows: &[LegacyOut], path: &Path) -> Result<()> {
    let batch = utf8_batch(
        &["page_id", "version", "binary", "stage", "text"],
        vec![
            rows.iter().map(|r| r.page_id.as_str()).collect(),
            rows.iter().map(|r| r.version.as_str()).collect(),
            rows.iter().map(|r| r.binary.as_str()).collect(),
            rows.iter().map(|r| r.stage.as_str()).collect(),
            rows.iter().map(|r| r.text.as_str()).collect(),
        ],
    );
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

pub fn read_legacy_long(path: &Path) -> Result<Vec<LegacyOut>> {
    let c = read_columns(path, &["page_id", "version", "binary", "stage", "text"])?;
    Ok((0..c[0].len())
        .map(|i| LegacyOut {
            page_id: c[0][i].clone(),
            version: c[1][i].clone(),
            binary: c[2][i].clone(),
            stage: c[3][i].clone(),
            text: c[4][i].clone(),
        })
        .collect())
}

/// One line per output, sorted: `page_id\tversion\tbinary\tstage\t<sha256 of text>`.
pub fn hash_lines(rows: &[LegacyOut]) -> String {
    let mut lines: Vec<String> = rows
        .iter()
        .map(|r| {
            let h: String = Sha256::digest(r.text.as_bytes())
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            format!("{}\t{}\t{}\t{}\t{}\n", r.page_id, r.version, r.binary, r.stage, h)
        })
        .collect();
    lines.sort();
    lines.concat()
}
