//! The eight output tables: their Arrow schemas, the part-file writer and the JSON form of a page.
use crate::model::*;
use anyhow::{Context, Result};
use arrow::array::{ArrayRef, Int32Builder, RecordBatch, StringBuilder, StringDictionaryBuilder};
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const TABLES: [&str; 8] = [
    "pages", "timings", "sections", "paragraphs", "templates", "refs", "ref_lists", "citations",
];

const META_KEY: &str = "wikitext_parser.schema_version";
const DICTIONARY_KEYS: [&str; 4] = ["source_file", "site", "version", "column"];

fn fields_of<T: for<'de> serde::Deserialize<'de>>() -> Vec<FieldRef> {
    Vec::<FieldRef>::from_type::<T>(TracingOptions::default()
            .allow_null_fields(false)
            .strings_as_large_utf8(false)
            .sequence_as_large_list(false))
        .unwrap_or_else(|e| panic!("schema for {}: {e}", std::any::type_name::<T>()))
}

fn stamp(f: &Field) -> Field {
    let mut meta: HashMap<String, String> = f.metadata().clone();
    meta.insert(META_KEY.into(), SCHEMA_VERSION.into());
    let dt = match f.data_type() {
        DataType::List(c) => DataType::List(Arc::new(stamp(c))),
        DataType::Struct(fs) => DataType::Struct(fs.iter().map(|c| Arc::new(stamp(c))).collect()),
        other => other.clone(),
    };
    Field::new(f.name(), dt, f.is_nullable()).with_metadata(meta)
}

fn key_fields() -> Vec<FieldRef> {
    fields_of::<Key>()
        .into_iter()
        .map(|f| {
            if DICTIONARY_KEYS.contains(&f.name().as_str()) {
                let dt = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
                Arc::new(Field::new(f.name(), dt, false))
            } else {
                f
            }
        })
        .collect()
}

/// Schema of one output table. Panics on an unknown table name.
pub fn schema(table: &str) -> SchemaRef {
    let own: Vec<FieldRef> = match table {
        "pages" => fields_of::<PageRow>(),
        "timings" => fields_of::<TimingRow>(),
        "sections" => fields_of::<SectionRow>(),
        "paragraphs" => fields_of::<ParagraphRow>(),
        "templates" => fields_of::<TemplateRow>(),
        "refs" => fields_of::<RefRow>(),
        "ref_lists" => fields_of::<RefListRow>(),
        "citations" => fields_of::<CitationRow>(),
        other => panic!("unknown table {other}"),
    };
    let component = !matches!(table, "pages" | "timings");
    let mut all: Vec<FieldRef> = Vec::new();
    if component {
        all.extend(key_fields());
        all.extend(own);
    } else {
        // Key fields are embedded in the row; swap in the dictionary types.
        let keys = key_fields();
        all.extend(own.into_iter().map(|f| {
            keys.iter().find(|k| k.name() == f.name() && matches!(k.data_type(), DataType::Dictionary(..))).cloned().unwrap_or(f)
        }));
    }
    let fields: Vec<Field> = all.iter().map(|f| stamp(f)).collect();
    Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(META_KEY.to_string(), SCHEMA_VERSION.to_string())]),
    ))
}

/// The six key columns of a component table: each page's key repeated once per row it has.
fn key_columns(pages: &[PageOutput], rows: impl Fn(&Components) -> usize) -> Vec<ArrayRef> {
    let mut source_file = StringDictionaryBuilder::<Int32Type>::new();
    let mut row = Int32Builder::new();
    let mut page_id = StringBuilder::new();
    let mut site = StringDictionaryBuilder::<Int32Type>::new();
    let mut version = StringDictionaryBuilder::<Int32Type>::new();
    let mut column = StringDictionaryBuilder::<Int32Type>::new();
    for p in pages {
        let k = &p.page;
        for _ in 0..rows(&p.components) {
            source_file.append_value(&k.source_file);
            row.append_value(k.row);
            page_id.append_value(&k.page_id);
            site.append_value(&k.site);
            version.append_value(&k.version);
            column.append_value(&k.column);
        }
    }
    vec![
        Arc::new(source_file.finish()),
        Arc::new(row.finish()),
        Arc::new(page_id.finish()),
        Arc::new(site.finish()),
        Arc::new(version.finish()),
        Arc::new(column.finish()),
    ]
}

/// Columns of a component table: the keys, then the rows serialised against the schema's own fields.
fn component<T: Serialize>(
    table: &str,
    fields: &[FieldRef],
    pages: &[PageOutput],
    rows: impl Fn(&Components) -> &[T],
) -> Result<Vec<ArrayRef>> {
    let mut columns = key_columns(pages, |c| rows(c).len());
    let items: Vec<&T> = pages.iter().flat_map(|p| rows(&p.components)).collect();
    columns.extend(serde_arrow::to_arrow(&fields[6..], &items).with_context(|| format!("{table} rows"))?);
    Ok(columns)
}

/// One record batch of `table` holding the rows of `pages`, in page order and then document order.
/// Its schema is exactly `schema(table)`.
pub fn record_batch(table: &str, pages: &[PageOutput]) -> Result<RecordBatch> {
    if !TABLES.contains(&table) {
        anyhow::bail!("unknown table '{table}'");
    }
    let schema = schema(table);
    let fields = schema.fields();
    let columns = match table {
        "pages" => {
            let rows: Vec<&PageRow> = pages.iter().map(|p| &p.page).collect();
            serde_arrow::to_arrow(fields, &rows).context("pages rows")?
        }
        "timings" => {
            let rows: Vec<&TimingRow> = pages.iter().map(|p| &p.timing).collect();
            serde_arrow::to_arrow(fields, &rows).context("timings rows")?
        }
        "sections" => component(table, fields, pages, |c| &c.sections)?,
        "paragraphs" => component(table, fields, pages, |c| &c.paragraphs)?,
        "templates" => component(table, fields, pages, |c| &c.templates)?,
        "refs" => component(table, fields, pages, |c| &c.refs)?,
        "ref_lists" => component(table, fields, pages, |c| &c.ref_lists)?,
        "citations" => component(table, fields, pages, |c| &c.citations)?,
        _ => unreachable!("checked against TABLES"),
    };
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Flush a row group once its encoded size passes this, so memory stays bounded by the batches
/// in flight rather than by the file. Depends on the data only, so output stays deterministic.
const ROW_GROUP_BYTES: usize = 64 << 20;

/// Writes one table's part file for one input: `<out>/<table>/<stem>.parquet.tmp` while writing,
/// renamed to `<stem>.parquet` by `finish`. The driver finishes `pages` last (by name), so a finished
/// `pages` part means every other part of that input is finished too.
pub struct PartWriter {
    table: String,
    tmp: PathBuf,
    path: PathBuf,
    writer: ArrowWriter<File>,
    rows: usize,
}

impl PartWriter {
    pub fn create(out: &Path, table: &str, stem: &str) -> Result<PartWriter> {
        let dir = out.join(table);
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let path = dir.join(format!("{stem}.parquet"));
        let tmp = dir.join(format!("{stem}.parquet.tmp"));
        let file = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_key_value_metadata(Some(vec![KeyValue::new(
                META_KEY.to_string(),
                SCHEMA_VERSION.to_string(),
            )]))
            .build();
        let writer = ArrowWriter::try_new(file, schema(table), Some(props))?;
        Ok(PartWriter { table: table.to_string(), tmp, path, writer, rows: 0 })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn append(&mut self, pages: &[PageOutput]) -> Result<()> {
        let batch = record_batch(&self.table, pages)?;
        self.rows += batch.num_rows();
        self.writer.write(&batch)?;
        if self.writer.in_progress_size() > ROW_GROUP_BYTES {
            self.writer.flush()?;
        }
        Ok(())
    }

    /// Writes the footer, syncs, renames the `.tmp` file into place and returns the row count.
    pub fn finish(self) -> Result<usize> {
        let file = self.writer.into_inner().with_context(|| format!("closing {}", self.tmp.display()))?;
        file.sync_all()?;
        std::fs::rename(&self.tmp, &self.path)
            .with_context(|| format!("renaming {} into place", self.tmp.display()))?;
        Ok(self.rows)
    }
}

fn value<T: Serialize>(x: &T) -> Value {
    serde_json::to_value(x).expect("row structs serialise to JSON")
}

/// A page version as JSON: its `pages` row and its rows of each component table, without timings.
pub fn page_value(p: &PageOutput) -> Value {
    let c = &p.components;
    json!({
        "page": value(&p.page),
        "sections": value(&c.sections),
        "paragraphs": value(&c.paragraphs),
        "templates": value(&c.templates),
        "refs": value(&c.refs),
        "ref_lists": value(&c.ref_lists),
        "citations": value(&c.citations),
    })
}

/// `page_value`, pretty-printed: the document `--format json` writes for a `.wiki` input.
pub fn page_json(p: &PageOutput) -> String {
    serde_json::to_string_pretty(&page_value(p)).expect("a JSON value prints")
}
