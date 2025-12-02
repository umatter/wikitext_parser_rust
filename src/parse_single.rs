//! Parse wikitext from a single-column parquet file
//!
//! This binary handles parsing wikitext from parquet files with a flexible schema,
//! supporting single text columns (e.g., for deleted/added page analysis).
//!
//! Input schemas supported:
//! - page_id, page_title, text, timestamp (Wikipedia format)
//! - pageid, title, content, timestamp (Ruwiki format)
//!
//! Output: Same columns with text/content replaced by parsed plaintext

mod parser;

use anyhow::Result;
use clap::Parser as ClapParser;
use std::fs::File;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

#[derive(ClapParser, Debug)]
#[command(author, version, about = "Parse wikitext from single-column parquet files", long_about = None)]
struct Args {
    /// Input parquet file path
    #[arg(short, long)]
    input: String,

    /// Output parquet file path
    #[arg(short, long)]
    output: String,

    /// Name of the text column to parse (auto-detected if not specified)
    #[arg(long)]
    text_column: Option<String>,

    /// Skip lists (remove all bullet/numbered lists from output)
    #[arg(long, default_value_t = false)]
    skip_lists: bool,

    /// Timeout in seconds for parsing each article (0 = no timeout)
    #[arg(long, default_value_t = 30)]
    timeout: u64,
}

/// Parse wikitext with a timeout to handle problematic articles
fn parse_wikitext_with_timeout(wikitext: &str, skip_lists: bool, timeout_secs: u64) -> String {
    let wikitext = wikitext.to_string();
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let result = parser::parse_wikitext(&wikitext, skip_lists);
        let _ = tx.send(result);
    });

    match rx.recv_timeout(Duration::from_secs(timeout_secs)) {
        Ok(result) => result,
        Err(_) => {
            eprintln!("WARNING: Article parsing timed out after {} seconds", timeout_secs);
            format!("[Article skipped: parsing timeout after {} seconds]", timeout_secs)
        }
    }
}

/// Detect the text column name from schema
fn detect_text_column(schema: &Schema) -> Option<String> {
    // Priority order: text, content, official_text, clone_text
    let candidates = ["text", "content", "official_text", "clone_text"];

    for candidate in candidates {
        if schema.field_with_name(candidate).is_ok() {
            return Some(candidate.to_string());
        }
    }

    // Fall back to any column with "text" in name
    for field in schema.fields() {
        if field.name().to_lowercase().contains("text") {
            return Some(field.name().clone());
        }
    }

    None
}

/// Detect the page ID column name from schema
fn detect_pageid_column(schema: &Schema) -> Option<String> {
    let candidates = ["page_id", "pageid"];
    for candidate in candidates {
        if schema.field_with_name(candidate).is_ok() {
            return Some(candidate.to_string());
        }
    }
    None
}

/// Detect the title column name from schema
fn detect_title_column(schema: &Schema) -> Option<String> {
    let candidates = ["page_title", "title"];
    for candidate in candidates {
        if schema.field_with_name(candidate).is_ok() {
            return Some(candidate.to_string());
        }
    }
    None
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Reading input file: {}", args.input);

    // Read input parquet file
    let file = File::open(&args.input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let mut reader = builder.build()?;

    // Detect or validate text column
    let text_column = match &args.text_column {
        Some(col) => {
            if schema.field_with_name(col).is_err() {
                anyhow::bail!("Specified text column '{}' not found in schema", col);
            }
            col.clone()
        }
        None => {
            detect_text_column(&schema)
                .ok_or_else(|| anyhow::anyhow!("Could not auto-detect text column. Use --text-column to specify."))?
        }
    };

    let pageid_column = detect_pageid_column(&schema);
    let title_column = detect_title_column(&schema);

    println!("Using text column: {}", text_column);
    if let Some(ref col) = pageid_column {
        println!("Using page ID column: {}", col);
    }
    if let Some(ref col) = title_column {
        println!("Using title column: {}", col);
    }

    // Collect all record batches
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }

    if batches.is_empty() {
        println!("No data found in input file");
        return Ok(());
    }

    // Build output schema - keep all columns, just rename text column to add _parsed suffix
    let output_text_column = format!("{}_parsed", text_column);
    let output_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if f.name() == &text_column {
                Field::new(&output_text_column, DataType::Utf8, true)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    let output_schema = Arc::new(Schema::new(output_fields));

    // Process batches
    let processed_batches: Vec<RecordBatch> = batches
        .iter()
        .map(|batch| {
            process_single_column_batch(
                batch,
                &text_column,
                &output_text_column,
                pageid_column.as_deref(),
                title_column.as_deref(),
                args.skip_lists,
                args.timeout,
                &output_schema,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // Write output parquet file
    println!("Writing output file: {}", args.output);
    let output_file = File::create(&args.output)?;

    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(output_file, output_schema, Some(props))?;

    for batch in processed_batches {
        writer.write(&batch)?;
    }

    writer.close()?;
    println!("Processing complete!");

    Ok(())
}

fn process_single_column_batch(
    batch: &RecordBatch,
    text_column: &str,
    output_text_column: &str,
    pageid_column: Option<&str>,
    title_column: Option<&str>,
    skip_lists: bool,
    timeout: u64,
    output_schema: &Arc<Schema>,
) -> Result<RecordBatch> {
    // Get the text column
    let text_array = batch
        .column_by_name(text_column)
        .ok_or_else(|| anyhow::anyhow!("Text column '{}' not found", text_column))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("Text column is not a StringArray"))?;

    // Get optional page ID and title for logging
    let pageid_array = pageid_column.and_then(|col| {
        batch.column_by_name(col)?.as_any().downcast_ref::<StringArray>()
    });
    let title_array = title_column.and_then(|col| {
        batch.column_by_name(col)?.as_any().downcast_ref::<StringArray>()
    });

    eprintln!("Processing batch with {} rows", text_array.len());

    // Parse wikitext
    let parsed_texts: Vec<Option<String>> = (0..text_array.len())
        .map(|i| {
            let pid = pageid_array
                .map(|arr| if arr.is_null(i) { "unknown".to_string() } else { arr.value(i).to_string() })
                .unwrap_or_else(|| format!("row_{}", i));
            let title = title_array
                .map(|arr| if arr.is_null(i) { "untitled".to_string() } else { arr.value(i).to_string() })
                .unwrap_or_else(|| "untitled".to_string());

            eprintln!("  [{}] Processing page_id={} title={}", i + 1, pid, title);

            if text_array.is_null(i) {
                None
            } else {
                let result = if timeout == 0 {
                    parser::parse_wikitext(text_array.value(i), skip_lists)
                } else {
                    parse_wikitext_with_timeout(text_array.value(i), skip_lists, timeout)
                };
                eprintln!("  [{}] Done processing page_id={}", i + 1, pid);
                Some(result)
            }
        })
        .collect();

    let parsed_text_array: ArrayRef = Arc::new(StringArray::from(parsed_texts));

    // Build output columns - replace text column with parsed version
    let output_columns: Vec<ArrayRef> = output_schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == output_text_column {
                Arc::clone(&parsed_text_array)
            } else {
                // Find the corresponding column in the input batch
                let original_name = if field.name() == output_text_column {
                    text_column
                } else {
                    field.name()
                };
                Arc::clone(batch.column_by_name(original_name).unwrap())
            }
        })
        .collect();

    let output_batch = RecordBatch::try_new(Arc::clone(output_schema), output_columns)?;

    Ok(output_batch)
}
