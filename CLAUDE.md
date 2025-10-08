# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A high-performance Rust parser for extracting clean text from Russian Wikipedia articles in wikitext format. Processes Parquet files containing both official Russian Wikipedia and Ruwiki fork versions, removing all markup (templates, infoboxes, tables, references, links) to extract plain paragraph text.

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

# Convenience scripts
./install.sh                                           # Install Rust and build
./run.sh [input] [output]                             # Run parser with defaults
./parse_parallel.sh <input_dir> <output_dir> [jobs] [timeout] [keep_dirty]  # Process multiple files (two-phase parallel)
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
