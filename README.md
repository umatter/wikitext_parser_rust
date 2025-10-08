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

# Step 1: Parse wikitext (produces "dirty" parquet with potential template fragments)
cargo run --release --bin wikitext_parser_rust -- --input data/sample_wikitext.parquet --output data/dirty.parquet

# Step 2: Clean the parsed output (removes template fragments and image markup)
cargo run --release --bin clean_parsed -- --input data/dirty.parquet --output data/output.parquet

# Optional: Run with list removal (removes all bullet/numbered lists)
cargo run --release --bin wikitext_parser_rust -- --input data/sample_wikitext.parquet --output data/dirty.parquet --skip-lists

# Optional: Disable timeout for maximum speed (use only if dataset parses cleanly)
cargo run --release --bin wikitext_parser_rust -- --input data/sample_wikitext.parquet --output data/dirty.parquet --timeout 0

# Optional: Custom timeout (e.g., 60 seconds per article)
cargo run --release --bin wikitext_parser_rust -- --input data/sample_wikitext.parquet --output data/dirty.parquet --timeout 60
```

### Processing Large Datasets

For production datasets with multiple parquet files:

```bash
# Process all files in parallel (default: 4 jobs, 30s timeout)
./parse_parallel.sh data/input_dir data/output_dir

# Custom parallel jobs (e.g., 8 jobs)
./parse_parallel.sh data/input_dir data/output_dir 8

# No timeout for maximum speed (use only for known-clean datasets)
./parse_parallel.sh data/input_dir data/output_dir 4 0

# Custom timeout (e.g., 60 seconds per article)
./parse_parallel.sh data/input_dir data/output_dir 4 60

# Keep intermediate "dirty" files for debugging
./parse_parallel.sh data/input_dir data/output_dir 4 30 true
```

The parallel script uses a **two-phase approach**:
- **Phase 1**: Parses all files in parallel (produces "dirty" output in `output_dir/dirty/`)
- **Phase 2**: Cleans all files in parallel (produces final output in `output_dir/`)
- Automatically skips already-processed files (resume support)
- Shows progress for each file and phase
- Optionally keeps intermediate files for debugging
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
├── Cargo.toml                     # Project dependencies (3 binaries defined)
├── src/
│   ├── main.rs                    # wikitext_parser_rust: Fast parser (Phase 1)
│   ├── parser.rs                  # Core wikitext parsing logic (AST extraction)
│   ├── clean_parsed.rs            # clean_parsed: Text cleaner (Phase 2)
│   └── export_parsed.rs           # export_parsed: Export to individual text files
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

### Two-Phase Processing Architecture

The parser uses a two-phase approach for optimal performance:

**Phase 1: Fast Parsing** (`wikitext_parser_rust`)
- Extracts text from MediaWiki AST
- Outputs "dirty" parquet with potential template fragments
- Handles core parsing with optional timeout safety

**Phase 2: Text Cleaning** (`clean_parsed`)
- Removes leaked template syntax (`{{...}}`)
- Cleans image markup fragments
- Vectorized operations for high performance
- Processes entire parquet columns at once

This separation allows:
- Faster parsing (~30% speedup)
- Re-cleaning without re-parsing
- Easy debugging of intermediate output

### Timeout Control

Control timeout behavior for parsing each article:

```bash
# Default: 30-second timeout (safe for unknown datasets)
--timeout 30

# No timeout: Maximum speed (use only for known-clean datasets)
--timeout 0

# Custom timeout: Adjust for complex articles
--timeout 60
```

Articles that exceed the timeout receive a placeholder: `[Article skipped: parsing timeout after N seconds]`

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
