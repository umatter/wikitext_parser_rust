use anyhow::Result;
use arrow::array::{Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::{self, File};
use std::path::Path;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <parsed_parquet> [output_dir_official] [output_dir_clone]", args[0]);
        eprintln!();
        eprintln!("Exports parsed text to individual files:");
        eprintln!("  <output_dir_official>/<pageid>_official.txt - Parsed official text");
        eprintln!("  <output_dir_clone>/<pageid>_clone.txt       - Parsed clone text");
        eprintln!();
        eprintln!("If only one output dir is provided, both types go there.");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  {} input.parquet data/export", args[0]);
        eprintln!("  {} input.parquet data/official data/clone", args[0]);
        std::process::exit(1);
    }

    let parsed_file = &args[1];
    let output_dir_official = if args.len() > 2 {
        args[2].clone()
    } else {
        "data/parsed_export".to_string()
    };
    let output_dir_clone = if args.len() > 3 {
        args[3].clone()
    } else {
        output_dir_official.clone()
    };

    println!("=================================================");
    println!("Parsed Text Export Utility");
    println!("=================================================");
    println!();
    println!("Input (parsed):         {}", parsed_file);
    println!("Output dir (official):  {}", output_dir_official);
    println!("Output dir (clone):     {}", output_dir_clone);
    println!();

    // Create output directories
    let output_path_official = Path::new(&output_dir_official);
    let output_path_clone = Path::new(&output_dir_clone);
    fs::create_dir_all(&output_path_official)?;
    fs::create_dir_all(&output_path_clone)?;

    println!("Created directories");
    println!();

    // Read parsed parquet file
    println!("Reading parsed file...");
    let file = File::open(parsed_file)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;

    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }

    if batches.is_empty() {
        println!("Error: No data found in parquet file");
        return Ok(());
    }

    println!();
    println!("Processing articles...");
    println!();

    let mut total_files = 0;

    // Process each row
    for batch in batches.iter() {
        let num_rows = batch.num_rows();

        // Extract columns
        let page_id = batch
            .column_by_name("page_id")
            .ok_or_else(|| anyhow::anyhow!("page_id column not found"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("page_id is not a StringArray"))?;

        let page_title = batch
            .column_by_name("page_title")
            .ok_or_else(|| anyhow::anyhow!("page_title column not found"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("page_title is not a StringArray"))?;

        let official_paragraphs = batch
            .column_by_name("official_text_paragraphs")
            .ok_or_else(|| anyhow::anyhow!("official_text_paragraphs column not found"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("official_text_paragraphs is not a StringArray"))?;

        let clone_paragraphs = batch
            .column_by_name("clone_text_paragraphs")
            .ok_or_else(|| anyhow::anyhow!("clone_text_paragraphs column not found"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("clone_text_paragraphs is not a StringArray"))?;

        for row_idx in 0..num_rows {
            if page_id.is_null(row_idx) {
                continue;
            }

            let page_id_val = page_id.value(row_idx);
            let page_title_val = if page_title.is_null(row_idx) {
                "untitled"
            } else {
                page_title.value(row_idx)
            };

            // Create header with metadata
            let header = format!(
                "Page ID: {}\nTitle: {}\n{}\n\n",
                page_id_val,
                page_title_val,
                "=".repeat(60)
            );

            // Write official text file
            let official_filename = format!("{}_official.txt", page_id_val);
            let official_filepath = output_path_official.join(&official_filename);
            if official_filepath.exists() {
                // Skip if already exists
            } else if !official_paragraphs.is_null(row_idx) {
                let content = format!("{}{}", header, official_paragraphs.value(row_idx));
                fs::write(&official_filepath, content)?;
                total_files += 1;
            }

            // Write clone text file
            let clone_filename = format!("{}_clone.txt", page_id_val);
            let clone_filepath = output_path_clone.join(&clone_filename);
            if clone_filepath.exists() {
                // Skip if already exists
            } else if !clone_paragraphs.is_null(row_idx) {
                let content = format!("{}{}", header, clone_paragraphs.value(row_idx));
                fs::write(&clone_filepath, content)?;
                total_files += 1;
            }

            if !official_filepath.exists() || !clone_filepath.exists() {
                println!("  ✓ Exported: {} - {}", page_id_val, page_title_val);
            }
        }
    }

    println!();
    println!("=================================================");
    println!("✓ Export complete!");
    println!("=================================================");
    println!();
    println!("Total files created: {}", total_files);
    println!("Output directories:");
    println!("  Official: {}", output_dir_official);
    println!("  Clone:    {}", output_dir_clone);
    println!();

    Ok(())
}
