#!/bin/bash
# Run script for wikitext_parser_rust

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================================="
echo "Wikitext Parser - Run Script"
echo -e "===================================================${NC}"
echo ""

# Ensure cargo is in PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "Error: Rust is not installed. Please run ./install.sh first"
    exit 1
fi

# Default input and output paths
INPUT_FILE="${1:-data/sample_wikitext.parquet}"
OUTPUT_FILE="${2:-data/output.parquet}"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' not found"
    echo ""
    echo "Usage: ./run.sh [input_file] [output_file]"
    echo "  Default input:  data/sample_wikitext.parquet"
    echo "  Default output: data/output.parquet"
    exit 1
fi

echo -e "${GREEN}Input file:${NC}  $INPUT_FILE"
echo -e "${GREEN}Output file:${NC} $OUTPUT_FILE"
echo ""

# Build if needed (skip if already built)
if [ ! -f "target/release/wikitext_parser_rust" ]; then
    echo "Building project (first run only)..."
    cargo build --release
    echo ""
fi

# Run the parser
echo -e "${BLUE}Processing wikitext...${NC}"
echo ""

cargo run --release -- --input "$INPUT_FILE" --output "$OUTPUT_FILE"

echo ""
echo -e "${GREEN}✓ Done!${NC}"
echo -e "Output saved to: ${GREEN}$OUTPUT_FILE${NC}"
