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
    let schema = builder.schema().clone();
    let mut reader = builder.build()?;

    // Find columns ending with _parsed or _paragraphs (text columns to clean)
    let text_columns: Vec<(usize, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            let name = field.name();
            name.ends_with("_parsed") || name.ends_with("_paragraphs")
        })
        .map(|(i, field)| (i, field.name().clone()))
        .collect();

    if text_columns.is_empty() {
        println!("Warning: No text columns found (columns ending with _parsed or _paragraphs)");
        println!("Available columns: {:?}", schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        return Ok(());
    }

    println!("Found {} text column(s) to clean: {:?}",
             text_columns.len(),
             text_columns.iter().map(|(_, name)| name.as_str()).collect::<Vec<_>>());

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
            clean_batch(batch, &text_columns)
        })
        .collect::<Result<Vec<_>>>()?;

    // Write output parquet file
    println!("Writing output file: {}", args.output);
    let output_file = File::create(&args.output)?;
    let out_schema = cleaned_batches[0].schema();

    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(output_file, out_schema, Some(props))?;

    for batch in cleaned_batches {
        writer.write(&batch)?;
    }

    writer.close()?;
    println!("Cleaning complete!");

    Ok(())
}

fn clean_batch(batch: &RecordBatch, text_columns: &[(usize, String)]) -> Result<RecordBatch> {
    let schema = batch.schema();

    // Build new column vector
    let mut new_columns: Vec<ArrayRef> = Vec::new();

    for (i, _field) in schema.fields().iter().enumerate() {
        // Check if this column is a text column to clean
        let is_text_column = text_columns.iter().any(|(idx, _)| *idx == i);

        if is_text_column {
            // Clean this text column
            let text_array = batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Column {} is not a StringArray", i))?;

            let cleaned = clean_text_array(text_array)?;
            new_columns.push(cleaned);
        } else {
            // Keep other columns as-is
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
