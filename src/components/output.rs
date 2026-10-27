//! Arrow schemas of the eight output tables (schemas only; writers come later).
use crate::model::*;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use std::collections::HashMap;
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
