mod parser;

use anyhow::Result;
use clap::Parser as ClapParser;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

#[derive(ClapParser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input parquet file path
    #[arg(short, long)]
    input: String,

    /// Output parquet file path
    #[arg(short, long)]
    output: String,

    /// Skip lists (remove all bullet/numbered lists from output)
    #[arg(long, default_value_t = false)]
    skip_lists: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Reading input file: {}", args.input);

    // Read input parquet file
    let file = File::open(&args.input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;

    // Collect all record batches
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }

    if batches.is_empty() {
        println!("No data found in input file");
        return Ok(());
    }

    // Process batches
    let processed_batches: Vec<RecordBatch> = batches
        .iter()
        .map(|batch| process_batch(batch, args.skip_lists))
        .collect::<Result<Vec<_>>>()?;

    // Write output parquet file
    println!("Writing output file: {}", args.output);
    let output_file = File::create(&args.output)?;
    let schema = processed_batches[0].schema();

    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    for batch in processed_batches {
        writer.write(&batch)?;
    }

    writer.close()?;
    println!("Processing complete!");

    Ok(())
}

fn process_batch(batch: &RecordBatch, skip_lists: bool) -> Result<RecordBatch> {
    let _schema = batch.schema();

    // Extract columns
    let page_id = batch.column_by_name("page_id")
        .ok_or_else(|| anyhow::anyhow!("page_id column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("page_id is not a StringArray"))?;
    let page_title = batch.column_by_name("page_title")
        .ok_or_else(|| anyhow::anyhow!("page_title column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("page_title is not a StringArray"))?;
    let official_text = batch.column_by_name("official_text")
        .ok_or_else(|| anyhow::anyhow!("official_text column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("official_text is not a StringArray"))?;
    let official_timestamp = batch.column_by_name("official_timestamp")
        .ok_or_else(|| anyhow::anyhow!("official_timestamp column not found"))?;
    let clone_page_title = batch.column_by_name("clone_page_title")
        .ok_or_else(|| anyhow::anyhow!("clone_page_title column not found"))?;
    let clone_text = batch.column_by_name("clone_text")
        .ok_or_else(|| anyhow::anyhow!("clone_text column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("clone_text is not a StringArray"))?;
    let clone_timestamp = batch.column_by_name("clone_timestamp")
        .ok_or_else(|| anyhow::anyhow!("clone_timestamp column not found"))?;

    eprintln!("Processing batch with {} rows", official_text.len());

    // Parse wikitext for both official and clone texts
    let official_paragraphs: Vec<Option<String>> = (0..official_text.len())
        .map(|i| {
            let pid = if page_id.is_null(i) { "unknown".to_string() } else { page_id.value(i).to_string() };
            let title = if page_title.is_null(i) { "untitled".to_string() } else { page_title.value(i).to_string() };
            eprintln!("  [{}] Processing official text for page_id={} title={}", i+1, pid, title);

            if official_text.is_null(i) {
                None
            } else {
                let result = parser::parse_wikitext(official_text.value(i), skip_lists);
                eprintln!("  [{}] Done processing official text for page_id={}", i+1, pid);
                Some(result)
            }
        })
        .collect();

    let clone_paragraphs: Vec<Option<String>> = (0..clone_text.len())
        .map(|i| {
            let pid = if page_id.is_null(i) { "unknown".to_string() } else { page_id.value(i).to_string() };
            let title = if page_title.is_null(i) { "untitled".to_string() } else { page_title.value(i).to_string() };
            eprintln!("  [{}] Processing clone text for page_id={} title={}", i+1, pid, title);

            if clone_text.is_null(i) {
                None
            } else {
                let result = parser::parse_wikitext(clone_text.value(i), skip_lists);
                eprintln!("  [{}] Done processing clone text for page_id={}", i+1, pid);
                Some(result)
            }
        })
        .collect();

    // Create new arrays
    let official_text_paragraphs: ArrayRef = Arc::new(StringArray::from(official_paragraphs));
    let clone_text_paragraphs: ArrayRef = Arc::new(StringArray::from(clone_paragraphs));

    // Build output schema with renamed columns
    let output_schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("page_id", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new("page_title", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new("official_text_paragraphs", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new("official_timestamp", official_timestamp.data_type().clone(), true),
        arrow::datatypes::Field::new("clone_page_title", clone_page_title.data_type().clone(), true),
        arrow::datatypes::Field::new("clone_text_paragraphs", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new("clone_timestamp", clone_timestamp.data_type().clone(), true),
    ]));

    let output_batch = RecordBatch::try_new(
        output_schema,
        vec![
            Arc::new(page_id.clone()) as ArrayRef,
            Arc::new(page_title.clone()) as ArrayRef,
            official_text_paragraphs,
            Arc::clone(official_timestamp),
            Arc::clone(clone_page_title),
            clone_text_paragraphs,
            Arc::clone(clone_timestamp),
        ],
    )?;

    Ok(output_batch)
}
