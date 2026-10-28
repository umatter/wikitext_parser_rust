# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A high-performance Rust parser for extracting clean text from Russian Wikipedia articles in wikitext format. Processes Parquet files containing both official Russian Wikipedia and Ruwiki fork versions, removing all markup (templates, infoboxes, tables, references, links) to extract plain paragraph text.

## Structured parse (`parse_components`) and the frozen legacy path

The repository has two paths that share no parsing code. Everything from "Building and Testing" on describes the
**legacy prose path**, which is **frozen**: `src/parser.rs`, `src/main.rs`, `src/clean_parsed.rs`,
`src/parse_single.rs`, `src/export_parsed.rs` and the `*.sh` scripts are never edited, they keep
`parse_wiki_text = "0.1"`, and their output stays byte-identical to tag `v0.1.0` (`tests/legacy_regression.rs`).

The **structured parse** is the library `wikitext_components` (`src/components/`) and the binary
`parse_components` (`src/parse_components.rs`), on the vendored `parse-wiki-text-2` (`vendor/`, patched only as
`vendor/parse-wiki-text-2/VENDORED.md` records). It writes one long Parquet table per component type: `pages`
(one row per page version, with its status), `timings`, `sections`, `paragraphs`, `templates`, `refs`,
`ref_lists`, `citations`, plus `manifest.json`.

- **`SCHEMA.md`** documents every table and column, the normalisation behind every `fp`, `title_norm`,
  `harv_key` and `host` (each citing the function that computes it), the statuses, the labels and the known
  limitations. Keep it in step with the code.
- **Schema `1.0.0` is frozen**: `tests/schema/v1.json` is never rewritten. A schema change bumps
  `SCHEMA_VERSION` (`src/components/model.rs`; minor: a column or table added; major: renamed, removed or changed
  meaning) and is pinned in a file of its own.
- **Labels**: every text needs `--label <key>=<site>:<version>`, never inferred. `<key>` is a text column (pair,
  single) or a `version` value (long); a `.wiki` file takes `<site>:<version>`. Sites are the `site` keys of
  `config/sites/*.toml`: `wikipedia`, `ruwiki`. Corpus labels: `wiki_forkday=wikipedia:forkday`,
  `wiki_2025=wikipedia:2025-01-01`, `deleted_wiki_2025=wikipedia:2025-01-01`, `ruwiki_2025=ruwiki:2025-01-01`;
  `crossection_diff`: `official_text=wikipedia:2025-01-01`, `clone_text=ruwiki:2025-01-01`.
- Site knowledge lives in `config/sites/*.toml`, template and section roles in `config/registry/`; the code knows
  no particular wiki.

```bash
cargo build --release
./target/release/parse_components --input <file-or-dir> --out <dir> --label <key>=<site>:<version> ... \
    [--layout pair|single|long] [--text-column C] [--format parquet|json] [--tables a,b] [--jobs N] \
    [--budget STEPS] [--no-fallback] [--resume] [--config-dir DIR]
cargo test --release                         # all tests; always --release (large corpus pages are slow in debug)
cargo test --release --features test-hooks   # plus the watchdog tests
```

- **Privacy.** Ruwiki markup never enters git, nor anything that reproduces it (expected outputs, snapshots,
  facts). The private corpus is read from `WIKITEXT_PRIVATE_CORPUS` (default
  `../ruwiki_replication_package_sources/data/results/sources/test_corpus/parser_test_corpus_v1.parquet`, sha256
  checked); readable private outputs live only in the gitignored `tests/private/`. Without the corpus, private
  tests print `SKIPPED (private corpus absent): <name>`, visible only with `-- --nocapture`.
- `UPDATE_EXPECTED=1` rewrites the structured snapshots (`tests/snapshots.rs`), public and private together; it
  fails when the private corpus is absent.
- No build, test or run contacts a website.

## Building and Testing

```bash
# Build project (release mode recommended for performance)
cargo build --release

# Two-phase processing (recommended)
# Phase 1: Parse wikitext (fast, produces "dirty" parquet)
cargo run --release --bin wikitext_parser_rust -- --input <input.parquet> --output <dirty.parquet>

# Phase 2: Clean output (removes template fragments, very fast)
cargo run --release --bin clean_parsed -- --input <dirty.parquet> --output <clean.parquet>

# Options for Phase 1
--skip-lists         # Remove all lists from output
--timeout 0          # Disable timeout for maximum speed (use only for known-clean datasets)
--timeout 60         # Custom timeout in seconds (default: 30)

# Export parsed text to individual files
cargo run --release --bin export_parsed -- <parsed.parquet> <output_dir_official> <output_dir_clone>

# Single-column parsing (for deleted/added page analysis)
cargo run --release --bin parse_single -- --input <input.parquet> --output <output.parquet>
# Options:
#   --text-column <name>   # Specify text column (auto-detected: text, content)
#   --skip-lists           # Remove all lists from output
#   --timeout <secs>       # Timeout per article (default: 30)

# Convenience scripts
./install.sh                                           # Install Rust and build
./run.sh [input] [output]                             # Run parser with defaults
./parse_parallel.sh <input_dir> <output_dir> [jobs] [timeout] [keep_dirty]  # Process multiple files (two-phase parallel)
./parse_single_parallel.sh <input_dir> <output_dir> [jobs] [timeout] [text_column]  # Single-column files (two-phase parallel)
./export_parallel.sh [parsed_dir] [wiki_dir] [ruwiki_dir] [jobs]  # Export to text files in parallel
```

## Architecture

The project uses a **two-phase processing architecture** with three binaries:

### Phase 1: Fast Parser (`src/main.rs` - `wikitext_parser_rust` binary)
- CLI entry point using clap for argument parsing
- Reads/writes Parquet files using Arrow/Parquet libraries
- **Optional timeout wrapper**: Spawns each article parse in a thread with configurable timeout (default 30s, 0 = disabled)
- Processes batches of records sequentially
- Transforms schema: replaces `official_text` and `clone_text` columns with `official_text_paragraphs` and `clone_text_paragraphs`
- Outputs "dirty" parquet with potential template fragments (for performance)
- Preserves all other columns (`page_id`, `page_title`, timestamps, etc.)

### Parser Module (`src/parser.rs`)
- Core wikitext parsing logic using `parse_wiki_text` crate
- Implements recursive AST traversal to extract plain text from parsed nodes
- **Template expansion**: Handles common Russian Wikipedia templates (dates, numbers)
- **Empty section removal**: Cleans up structural headings with no content
- **Optional list removal**: `skip_lists` parameter to exclude all list types (bullet, numbered, definition)
- Note: Heavy cleaning operations moved to Phase 2 for performance

### Phase 2: Text Cleaner (`src/clean_parsed.rs` - `clean_parsed` binary)
- Post-processing cleaner for parsed parquet files
- **Vectorized operations**: Processes entire columns at once (much faster than per-article)
- **Iterative template removal**: Removes leaked `{{...}}` syntax (up to 10 passes for nested templates)
- **Complex template handling**: Bounded regex for deeply nested cases
- **Image fragment removal**: Cleans up leaked image markup
- **Multi-newline cleanup**: Normalizes whitespace
- Can be re-run without re-parsing if cleaning logic needs adjustment

### Export Utility (`src/export_parsed.rs` - `export_parsed` binary)
- Exports parsed Parquet files to individual text files
- Creates separate directories for official Wikipedia vs Ruwiki fork versions
- Each file contains header with page ID and title
- Supports resume: skips already-exported files

### Single-Column Parser (`src/parse_single.rs` - `parse_single` binary)
- Handles single-column parquet files (not the two-column comparison format)
- **Use case**: Parsing deleted pages (Wikipedia-only) or added pages (Ruwiki-only)
- Auto-detects text column name (supports: `text`, `content`, `official_text`, `clone_text`)
- Auto-detects page ID column (`page_id` or `pageid`) and title column (`page_title` or `title`)
- Output column: `{text_column}_parsed` (e.g., `text_parsed`, `content_parsed`)
- Same two-phase processing: parse → clean (via `clean_parsed` binary)
- Supports flexible input schemas from different data sources

### Key Parsing Algorithm

The parser works in stages:
1. **AST extraction** (`extract_text_from_nodes`): Recursively walks parse tree, extracts text from specific node types (Text, Bold, Italic, Link, Heading, Lists), skips markup nodes (Template, Table, Image, Category, Tag)
2. **Template expansion** (`expand_common_templates`): Expands Russian date/number templates using regex
3. **Image cleanup** (`remove_image_fragments`): Removes leaked image markup with bounded regexes to prevent catastrophic backtracking
4. **Section cleanup** (`remove_empty_sections`): Removes structural headings with no content
5. **Paragraph assembly**: Splits by double newlines, trims whitespace

### Input Schema
Parquet files with columns:
- `page_id`: Wikipedia page identifier
- `page_title`: Article title (official)
- `official_text`: Wikitext from official Russian Wikipedia
- `official_timestamp`: Timestamp
- `clone_page_title`: Ruwiki fork title
- `clone_text`: Wikitext from Ruwiki fork
- `clone_timestamp`: Timestamp

### Output Schema
Same structure with renamed text columns:
- `official_text_paragraphs`: Parsed plain text (replaces `official_text`)
- `clone_text_paragraphs`: Parsed plain text (replaces `clone_text`)

## Important Implementation Details

### Timeout Safety
Articles that exceed the parsing timeout are automatically skipped:
- Default timeout: 30 seconds per article (configurable via `--timeout`)
- `--timeout 0` disables timeout for maximum speed on known-clean datasets
- Timed-out articles receive placeholder: `[Article skipped: parsing timeout after N seconds]`
- Prevents hanging on complex nested structures (<0.1% of articles)
- Implementation: Thread-based timeout wrapper in `main.rs:38-56`

### Regex Safety
All regexes use bounded quantifiers to prevent catastrophic backtracking:
- `{0,500}` for file markup
- `{0,200}` for image parameters
- `{0,100}` for alt text
- See `remove_image_fragments` in `parser.rs:43-80`

### List Handling
The `skip_lists` flag controls list extraction:
- `false` (default): Extract text from UnorderedList, OrderedList, DefinitionList nodes
- `true`: Skip all list nodes entirely
- Implementation in `extract_text_from_nodes` at `parser.rs:229-256`

### Node Type Extraction Strategy
- **Extract text from**: Text, Bold, Italic, BoldItalic, Link, ExternalLink, Heading, Preformatted, Tag (except `<ref>`)
- **Skip entirely**: Template, Table, Image, Category, Comment, MagicWord, Redirect, Parameter
- **Conditional**: Lists (depends on `skip_lists` flag)

## Dependencies

- `parse_wiki_text = "0.1"` - MediaWiki parsing (AST generation)
- `parquet = "53.3.0"` - Parquet I/O
- `arrow = "53.3.0"` - Columnar data structures
- `clap = "4.5"` - CLI parsing (derive feature)
- `anyhow = "1.0"` - Error handling
- `regex = "1.10"` - Text cleanup

## Data Flow

```
Input Parquet (wikitext)
    ↓
src/main.rs: Read batches, extract columns
    ↓
src/parser.rs: parse_wikitext() → AST traversal → text extraction → cleanup
    ↓
src/main.rs: Write batches with renamed columns
    ↓
Output Parquet (parsed text)
    ↓
src/export_parsed.rs: Export to individual .txt files (optional)
```
