# Wikitext Parser for Russian Wikipedia

A high-performance Rust parser for extracting clean text from Russian Wikipedia articles in wikitext format. Removes all markup (templates, infoboxes, tables, references, links, etc.) and extracts only the main article text.

This repository holds two paths that share no parsing code:

- **The legacy prose path** (`wikitext_parser_rust`, `clean_parsed`, `parse_single`, `export_parsed` and the
  shell scripts), described from [Features](#features) on. It is **frozen**: its output is byte-identical to
  tag `v0.1.0`, which the paper's texts use, and a regression test (`tests/legacy_regression.rs`) holds it there.
  Its source files are not edited.
- **The structured parse** (`parse_components`), described next.

## Structured parse: `parse_components`

`parse_components` parses wikitext into its components and writes one long Parquet table per component type, every
row located in the page by byte and code-point span, section and paragraph. Schema `1.0.0` (frozen) has eight
tables:

- `pages`: one row per page version, always, with its `status` (`ok`, `redirect`, `null_input`, `empty`,
  `fallback`, `budget`, `depth_limit`, `panic`, `hang`), parser warnings and row counts;
- `timings`: parse and walk times per page version (the one table that differs between runs);
- `sections` (the section tree, with paths and roles), `paragraphs` (the page's own text, with list items);
- `templates` (every template with its parameters, never expanded), `refs` (inline, named, reused,
  list-defined and footnote-template refs, resolved), `ref_lists`, and `citations` (one row per source: citation
  templates with their fields, bare links and plain-text refs, archive URLs unwrapped, hosts and registrable
  domains, `{{sfn}}` resolution).

**[SCHEMA.md](SCHEMA.md) documents every table and column**: types, meaning, null rules, the exact normalisation
behind every `fp`, `title_norm`, `harv_key` and `host`, the statuses, the label conventions and the known
limitations. Templates are not expanded, so categories and refs that only templates add are invisible. The
paragraph text is not the legacy path's text. The design is in
`docs/superpowers/specs/2026-10-27-structured-parse-design.md`.

```bash
cargo build --release

# A crossection_diff file (pair layout): every text column gets a label, <key>=<site>:<version>.
./target/release/parse_components --input data/crossection_diff/2025-01-01/file.parquet --out out/ \
    --label official_text=wikipedia:2025-01-01 --label clone_text=ruwiki:2025-01-01

# A timeseries_diff file: both columns are Wikipedia.
./target/release/parse_components --input <dir> --out out/ \
    --label official_text=wikipedia:forkday --label clone_text=wikipedia:2025-01-01

# The test corpus (long layout): the keys are values of its version column.
./target/release/parse_components --input tests/corpus/wikipedia_v1.parquet --out out/ \
    --label wiki_forkday=wikipedia:forkday --label wiki_2025=wikipedia:2025-01-01 \
    --label deleted_wiki_2025=wikipedia:2025-01-01

# One .wiki file, as one JSON document.
./target/release/parse_components --input article.wiki --label wikipedia:adhoc --format json --out article.json
```

- **Labels are required and never inferred.** `<key>` is a text column (pair and single layouts) or a value of
  the `version` column (long layout); a `.wiki` file takes `<site>:<version>`. `<site>` selects the settings file
  the text is parsed with: `wikipedia` (`config/sites/ru.wikipedia.toml`) or `ruwiki` (`config/sites/ruwiki.toml`).
  A column name never says which site a text comes from: in `timeseries_diff` both columns are Wikipedia. A label
  that matches nothing, and a text without a label, are errors.
- The layout (`pair`, `single`, `long`) is detected from the columns, or set with `--layout`; `--text-column`
  names the text column of a single-column file.
- An input directory means its `*.parquet` files, sorted by name. Each input gets one part file per table,
  `<out>/<table>/<stem>.parquet`, written under a `.tmp` name and renamed when complete, plus
  `<out>/manifest.json` (versions, settings and registry hashes, arguments, inputs and outputs with row counts,
  status counts, pages per second).
- Options: `--tables refs,citations` (a subset; `pages` is always written), `--jobs N` (default: all cores),
  `--budget STEPS` (the parser's step budget, default 2,700,000,000), `--no-fallback` (failed pages get their
  failure status instead of the fallback scanner's rows), `--resume` (skip finished inputs),
  `--format json`, `--config-dir`.
- Work is bounded by counted steps, not by the clock, so a page's status does not depend on the machine. A page the
  parser cannot finish still gets rows from a linear fallback scanner (`status = fallback`).
- Measured on one row group of the 2025 cross-section (14,568 pairs) with `--jobs 12` on a 12-thread laptop CPU: about
  1,280 page versions per second, no page over budget. That projects to under an hour for each input family.

### Tests

```bash
cargo test --release                         # everything; corpus pages up to 260,000 characters are slow in debug
cargo test --release --features test-hooks   # adds the watchdog tests
```

- The public test corpus is `tests/corpus/wikipedia_v1.parquet` (180 Wikipedia page versions, CC BY-SA, see
  `tests/corpus/ATTRIBUTION.md`), with the expected outputs in `tests/expected/` and the schema in
  `tests/schema/v1.json`.
- **The private corpus** (the Ruwiki versions) never enters this repository. Tests read it from the path in
  `WIKITEXT_PRIVATE_CORPUS`, by default
  `../ruwiki_replication_package_sources/data/results/sources/test_corpus/parser_test_corpus_v1.parquet`, and check
  its sha256 first. When it is absent, each private test passes after printing
  `SKIPPED (private corpus absent): <test name>`. Cargo hides that line unless the test output is shown:
  `cargo test --release -- --nocapture 2>&1 | grep SKIPPED` lists them.
- `UPDATE_EXPECTED=1 cargo test --release --test snapshots` rewrites the structured snapshots, public and private
  together (it fails without the private corpus). The schema is frozen: `tests/schema/v1.json` is never rewritten.
- `#[ignore]` tests: `generate_selection` (picks the readable snapshots once), `sfn_resolution_rate`, and
  `locality_report_sample` (a locality report on a performance sample, path in `WIKITEXT_SAMPLE`, counts only).

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
