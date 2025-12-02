use anyhow::Result;
use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use clap::Parser as ClapParser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use regex::Regex;
use std::fs::File;
use std::sync::Arc;

#[derive(ClapParser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input parquet file path (dirty)
    #[arg(short, long)]
    input: String,

    /// Output parquet file path (clean)
    #[arg(short, long)]
    output: String,
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

    println!("Cleaning {} batches...", batches.len());

    // Process batches
    let cleaned_batches: Vec<RecordBatch> = batches
        .iter()
        .enumerate()
        .map(|(i, batch)| {
            println!("  Cleaning batch {}/{}", i + 1, batches.len());
            clean_batch(batch)
        })
        .collect::<Result<Vec<_>>>()?;

    // Write output parquet file
    println!("Writing output file: {}", args.output);
    let output_file = File::create(&args.output)?;
    let schema = cleaned_batches[0].schema();

    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    for batch in cleaned_batches {
        writer.write(&batch)?;
    }

    writer.close()?;
    println!("Cleaning complete!");

    Ok(())
}

fn clean_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();

    // Get column indices
    let official_idx = schema
        .index_of("official_text_paragraphs")
        .map_err(|_| anyhow::anyhow!("official_text_paragraphs column not found"))?;
    let clone_idx = schema
        .index_of("clone_text_paragraphs")
        .map_err(|_| anyhow::anyhow!("clone_text_paragraphs column not found"))?;

    // Extract the text columns
    let official_text = batch
        .column(official_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("official_text_paragraphs is not a StringArray"))?;

    let clone_text = batch
        .column(clone_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("clone_text_paragraphs is not a StringArray"))?;

    // Clean the text columns
    let cleaned_official = clean_text_array(official_text)?;
    let cleaned_clone = clean_text_array(clone_text)?;

    // Build new column vector
    let mut new_columns: Vec<ArrayRef> = Vec::new();
    for (i, _field) in schema.fields().iter().enumerate() {
        if i == official_idx {
            new_columns.push(cleaned_official.clone());
        } else if i == clone_idx {
            new_columns.push(cleaned_clone.clone());
        } else {
            new_columns.push(Arc::clone(batch.column(i)));
        }
    }

    Ok(RecordBatch::try_new(schema, new_columns)?)
}

fn clean_text_array(array: &StringArray) -> Result<ArrayRef> {
    // Process each string in the array
    let cleaned: Vec<Option<String>> = (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                None
            } else {
                Some(clean_text(array.value(i)))
            }
        })
        .collect();

    Ok(Arc::new(StringArray::from(cleaned)))
}

fn clean_text(text: &str) -> String {
    let mut result = text.to_string();

    // Step 1: Remove templates iteratively (handles nested templates)
    let simple_template_re = Regex::new(r"\{\{[^{}]*\}\}").unwrap();
    let max_iterations = 10;
    let mut prev_len = result.len();

    for _ in 0..max_iterations {
        result = simple_template_re.replace_all(&result, "").to_string();
        if result.len() == prev_len {
            break;
        }
        prev_len = result.len();
    }

    // Step 2: Handle remaining complex templates with bounded quantifier
    let complex_template_re = Regex::new(r"\{\{[^}]{0,500}\}\}").unwrap();
    result = complex_template_re.replace_all(&result, "").to_string();

    // Step 3: Clean up orphaned braces
    let orphan_braces_re = Regex::new(r"[\{\}]").unwrap();
    result = orphan_braces_re.replace_all(&result, "").to_string();

    // Step 4: Remove image fragments
    result = remove_image_fragments(&result);

    // Step 5: Clean up multiple consecutive newlines
    let multi_newline_re = Regex::new(r"\n{3,}").unwrap();
    result = multi_newline_re.replace_all(&result, "\n\n").to_string();

    result
}

fn remove_image_fragments(text: &str) -> String {
    let mut result = text.to_string();

    // Remove [[Файл:...]] and [[File:...]] markup
    let file_re = Regex::new(r"\[\[(?:Файл|File):[^\]]{0,500}\]\]").unwrap();
    result = file_re.replace_all(&result, "").to_string();

    // Remove image size/position parameters
    let image_params_re =
        Regex::new(r"(?m)^\d+px\|(?:мини|thumb|миниатюра|left|right|center|слева|справа|центр)\|.{0,200}$")
            .unwrap();
    let lines: Vec<String> = result
        .lines()
        .filter(|line| !image_params_re.is_match(line.trim()))
        .map(|s| s.to_string())
        .collect();
    result = lines.join("\n");

    // Remove standalone image parameter fragments
    let fragment_patterns = vec![
        r"(?m)^\s*\d+px\|мини\|(?:слева|справа|центр)?.{0,200}$",
        r"(?m)^\s*альт=.{0,100}\|мини\|.{0,200}$",
        r"(?m)^\s*\d+px\|мини$",
    ];

    for pattern in fragment_patterns {
        let re = Regex::new(pattern).unwrap();
        result = re.replace_all(&result, "").to_string();
    }

    result
}
