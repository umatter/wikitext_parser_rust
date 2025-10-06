# Wikitext Parser for Russian Wikipedia

A high-performance Rust parser for extracting clean text from Russian Wikipedia articles in wikitext format. Removes all markup (templates, infoboxes, tables, references, links, etc.) and extracts only the main article text.

## Features

- Built on [`parse_wiki_text`](https://docs.rs/parse_wiki_text/latest/parse_wiki_text/) for efficient MediaWiki parsing
- Extracts plain paragraph text while removing all wikitext markup
- Processes Parquet files with multiple text columns
- Handles both official Russian Wikipedia and Ruwiki fork versions
- **Optional list removal** via `--skip-lists` flag
- **Parallel processing** scripts for large datasets
- **Smart skipping** of problematic articles (large tables with complex nested structures)
- **Flexible export** to individual text files with separate directories
- Fast, memory-efficient processing with detailed progress logging

## Quick Start

### Complete Workflow (3 Steps)

```bash
# 1. Install and build
./install.sh

# 2. Run the parser
./run.sh

# 3. Verify output quality
./verify_output.sh
```

### Detailed Steps

#### 1. Install and Build

```bash
chmod +x install.sh
./install.sh
```

This will:
- Install Rust toolchain (if not already installed)
- Build the project in release mode

#### 2. Run the Parser

```bash
chmod +x run.sh
./run.sh
```

By default, this processes `data/sample_wikitext.parquet` and outputs to `data/output.parquet`.

**Custom input/output:**
```bash
./run.sh path/to/input.parquet path/to/output.parquet
```

#### 3. Verify Output

```bash
chmod +x verify_parsing.sh
./verify_parsing.sh
```

This runs automated quality checks to ensure proper parsing.

## Manual Usage

If you prefer to run cargo commands directly:

```bash
# Build
cargo build --release

# Run (standard mode)
cargo run --release --bin wikitext_parser_rust -- --input data/sample_wikitext.parquet --output data/output.parquet

# Run with list removal (removes all bullet/numbered lists)
cargo run --release --bin wikitext_parser_rust -- --input data/sample_wikitext.parquet --output data/output.parquet --skip-lists
```

### Processing Large Datasets

For production datasets with multiple parquet files:

```bash
# Process all files in parallel (default: 4 parallel jobs)
./parse_parallel.sh data/input_dir data/output_dir

# Custom parallel jobs (e.g., 8 jobs)
./parse_parallel.sh data/input_dir data/output_dir 8
```

The parallel script:
- Automatically skips already-processed files (resume support)
- Shows progress for each file
- Uses cargo in release mode for optimal performance

## Input Format

The parser expects a Parquet file with these columns:
- `page_id`: Wikipedia page identifier
- `page_title`: Article title (official Wikipedia)
- `official_text`: Wikitext from official Russian Wikipedia
- `official_timestamp`: Timestamp of official version
- `clone_page_title`: Article title from Ruwiki fork
- `clone_text`: Wikitext from Ruwiki fork
- `clone_timestamp`: Timestamp of Ruwiki version

## Output Format

The output Parquet file contains:
- `page_id`: Original page identifier
- `page_title`: Original article title
- `official_text_paragraphs`: **Parsed plain text** (replaces `official_text`)
- `official_timestamp`: Original timestamp
- `clone_page_title`: Original Ruwiki title
- `clone_text_paragraphs`: **Parsed plain text** (replaces `clone_text`)
- `clone_timestamp`: Original timestamp

## What Gets Removed

The parser removes all wikitext markup:
- Templates (e.g., `{{Фильм|...}}`, `{{Infobox|...}}`)
- Infoboxes
- Tables
- References and citations (`<ref>...</ref>`)
- Categories
- Images
- Link markup (keeps only display text)
- Section headings
- All other MediaWiki syntax

## What Gets Extracted

Only the main article text:
- Plain paragraph text
- Text from formatted elements (bold, italic)
- Display text from links (without markup)

## Example Output

### Input (Wikitext)
```
{{нет ссылок|дата=2012-09-05}}
{{Фильм
|РусНаз = Ветер «Надежды»
|Изображение = Постер фильма «Ветер «Надежды»» (СССР, 1977).jpg
|Режиссёр = [[Говорухин, Станислав Сергеевич|Станислав Говорухин]]
...
}}

«Ветер „Надежды"» — советский фильм режиссёра
[[Говорухин, Станислав Сергеевич|С. Говорухина]]...
```

### Output (Parsed Plain Text)
```
«Ветер „Надежды"» — советский фильм режиссёра С. Говорухина,
снятый на киностудии им. М. Горького в 1977 году.

Сюжет

Фильм повествует о курсантах морских училищ, направляющихся в
Австралию на учебном паруснике «Надежда»...
```

**Result:** Clean, readable text with all markup removed (36% size reduction).

## Requirements

- Rust 1.70 or later (automatically installed by `install.sh`)
- Linux/macOS/WSL

## Project Structure

```
wikitext_parser_rust/
├── Cargo.toml                     # Project dependencies
├── src/
│   ├── main.rs                    # CLI and Parquet I/O (with --skip-lists flag)
│   ├── parser.rs                  # Wikitext parsing logic with smart skipping
│   └── export_parsed.rs           # Parsed text file exporter
├── data/
│   ├── sample_wikitext.parquet    # Sample input data (10 articles)
│   └── crossection_diff/          # Production data (gitignored)
│       └── 2025-01-01/
│           ├── 20251001_*.parquet # Raw input files
│           └── parsed/            # Parsed output files
├── install.sh                     # Installation script
├── run.sh                         # Run parser script
├── parse_parallel.sh              # Parallel processing for large datasets
├── export_parallel.sh             # Parallel export parsed-only
├── verify_parsing.sh              # Automated quality checks
├── compare_sample.sh              # Sample comparison tool
├── extract_article.py             # Debug utility for specific articles
└── README.md                      # This file
```

## Verification & Inspection Tools

After running the parser, you can verify the output quality using these tools:

### 1. Export to Individual Text Files (Recommended for Manual Inspection)

Export each article to separate text files from parsed parquet files with automatic resume support:

```bash
# Export to separate wiki/ruwiki directories (default)
./export_parallel.sh

# Custom directories with 8 parallel jobs
./export_parallel.sh data/parsed data/wiki data/ruwiki 8
```

This creates:
```
data/parsed_export/
  wiki/
    158785_official.txt   (parsed Wikipedia text)
    12644_official.txt
    ...
  ruwiki/
    158785_clone.txt      (parsed Ruwiki text)
    12644_clone.txt
    ...
```

**Manual usage (single file):**
```bash
# Same directory for both
cargo run --release --bin export_parsed -- input.parquet output_dir

# Separate directories
cargo run --release --bin export_parsed -- input.parquet wiki_dir ruwiki_dir
```

Each file includes a header with page ID and title, making it easy to:
- Open files in any text editor for manual inspection
- Verify parsing quality across different articles
- Resume interrupted exports (skips existing files)

### 2. Debug Specific Articles

Use `extract_article.py` to inspect raw wikitext for problematic articles:

```bash
python3 extract_article.py data/crossection_diff/2025-01-01/parsed_file.parquet 5589360
```

Shows the original wikitext for debugging parsing issues.

## Advanced Features

### Smart Article Skipping

The parser automatically detects and skips articles with problematic patterns that cause parsing issues:

- **Large tables with many templates**: Articles with >50 table rows AND (>200 templates OR >50 images)
- These articles receive a placeholder: `[Article skipped: contains complex nested structures that cause parsing issues]`
- Prevents infinite loops and memory exhaustion on edge cases
- Typically affects <0.1% of articles (championship lists, large data tables, etc.)

### List Removal Option

Use `--skip-lists` to remove all lists from the output:

```bash
cargo run --release --bin wikitext_parser_rust -- --input data/input.parquet --output data/output.parquet --skip-lists
```

This removes:
- Bullet lists (unordered lists)
- Numbered lists (ordered lists)
- Definition lists

Useful when you only want narrative paragraph text without list structures (like bibliography sections).


## Dependencies

- `parse_wiki_text` - MediaWiki text parsing
- `parquet` - Parquet file I/O
- `arrow` - Columnar data processing
- `clap` - Command-line argument parsing
- `anyhow` - Error handling
- `regex` - Image fragment cleanup

## Performance

- Processes ~1000 articles/minute on typical hardware
- Parallel processing scales linearly with CPU cores
- Memory usage: ~50-100MB per process
- Resume support prevents wasted reprocessing
