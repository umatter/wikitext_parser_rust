#!/bin/bash
# Process single-column parquet files in parallel
# Two-phase approach: Phase 1 (parse wikitext) → Phase 2 (clean output)
#
# Usage: ./parse_single_parallel.sh <input_dir> <output_dir> [jobs] [timeout] [text_column]
#
# Arguments:
#   input_dir    - Directory containing input parquet files
#   output_dir   - Directory for parsed output files
#   jobs         - Number of parallel jobs (default: 4)
#   timeout      - Timeout per article in seconds (default: 30, 0=disabled)
#   text_column  - Name of text column to parse (auto-detected if not specified)

set -e

export PATH="$HOME/.cargo/bin:$PATH"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
INPUT_DIR="${1:-data/input}"
OUTPUT_DIR="${2:-data/parsed}"
PARALLEL_JOBS="${3:-4}"
TIMEOUT="${4:-30}"
TEXT_COLUMN="${5:-}"  # Empty = auto-detect

echo -e "${BLUE}==================================================="
echo "Single-Column Wikitext Parser (PARALLEL)"
echo "Two-phase: Parse → Clean"
echo -e "===================================================${NC}"
echo ""
echo -e "${GREEN}Input directory:${NC}  $INPUT_DIR"
echo -e "${GREEN}Output directory:${NC} $OUTPUT_DIR"
echo -e "${GREEN}Parallel jobs:${NC}    $PARALLEL_JOBS"
echo -e "${GREEN}Timeout:${NC}          $TIMEOUT seconds per article"
echo -e "${GREEN}Text column:${NC}      ${TEXT_COLUMN:-auto-detect}"
echo ""

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo -e "${YELLOW}Error: Input directory '$INPUT_DIR' not found${NC}"
    exit 1
fi

# Create output directories
mkdir -p "$OUTPUT_DIR"
DIRTY_DIR="$OUTPUT_DIR/dirty"
mkdir -p "$DIRTY_DIR"

# Find parquet files
PARQUET_FILES=$(find "$INPUT_DIR" -maxdepth 1 -type f -name "*.parquet" 2>/dev/null)
TOTAL_FILES=$(echo "$PARQUET_FILES" | grep -c . || echo 0)

if [ "$TOTAL_FILES" -eq 0 ]; then
    echo -e "${YELLOW}No parquet files found in $INPUT_DIR${NC}"
    exit 1
fi

echo -e "${GREEN}Found $TOTAL_FILES parquet file(s) to process${NC}"
echo ""

# Build optional arguments
EXTRA_ARGS=""
if [ -n "$TEXT_COLUMN" ]; then
    EXTRA_ARGS="--text-column $TEXT_COLUMN"
fi

# PHASE 1: Parse wikitext in parallel
echo -e "${BLUE}=== PHASE 1: Parsing wikitext ===${NC}"

parse_file() {
    local input_file="$1"
    local dirty_dir="$2"
    local timeout="$3"
    local extra_args="$4"

    local filename=$(basename "$input_file")
    local dirty_output="$dirty_dir/parsed_$filename"

    if [ -f "$dirty_output" ]; then
        echo "  [SKIP] $filename (already parsed)"
        return 0
    fi

    echo "  [PARSE] $filename"
    ./target/release/parse_single \
        --input "$input_file" \
        --output "$dirty_output" \
        --timeout "$timeout" \
        $extra_args 2>&1 | grep -v "Processing\|Done processing" || true
}

export -f parse_file
export DIRTY_DIR TIMEOUT EXTRA_ARGS

# Process files in parallel
echo "$PARQUET_FILES" | xargs -P "$PARALLEL_JOBS" -I {} bash -c 'parse_file "$@"' _ {} "$DIRTY_DIR" "$TIMEOUT" "$EXTRA_ARGS"

echo ""
echo -e "${GREEN}Phase 1 complete. Parsed files in: $DIRTY_DIR${NC}"

# PHASE 2: Clean parsed output
echo ""
echo -e "${BLUE}=== PHASE 2: Cleaning parsed output ===${NC}"

clean_file() {
    local dirty_file="$1"
    local output_dir="$2"

    local filename=$(basename "$dirty_file")
    local clean_filename="${filename#parsed_}"  # Remove "parsed_" prefix
    local clean_output="$output_dir/$clean_filename"

    if [ -f "$clean_output" ]; then
        echo "  [SKIP] $filename (already cleaned)"
        return 0
    fi

    echo "  [CLEAN] $filename"
    ./target/release/clean_parsed \
        --input "$dirty_file" \
        --output "$clean_output" 2>&1 | grep -v "Processing\|Cleaning" || true
}

export -f clean_file

# Find and clean all parsed files
PARSED_FILES=$(find "$DIRTY_DIR" -maxdepth 1 -type f -name "parsed_*.parquet" 2>/dev/null)

if [ -n "$PARSED_FILES" ]; then
    echo "$PARSED_FILES" | xargs -P "$PARALLEL_JOBS" -I {} bash -c 'clean_file "$@"' _ {} "$OUTPUT_DIR"
fi

echo ""
echo -e "${GREEN}Phase 2 complete. Cleaned files in: $OUTPUT_DIR${NC}"

# Summary
echo ""
echo -e "${BLUE}=== Summary ===${NC}"
CLEAN_COUNT=$(find "$OUTPUT_DIR" -maxdepth 1 -type f -name "*.parquet" 2>/dev/null | wc -l)
echo -e "${GREEN}Processed $CLEAN_COUNT file(s)${NC}"
echo -e "${GREEN}Output directory: $OUTPUT_DIR${NC}"
