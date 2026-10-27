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
