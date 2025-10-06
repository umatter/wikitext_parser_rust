#!/bin/bash
# Process real Wikipedia data from crossection_diff directory IN PARALLEL

set -e

export PATH="$HOME/.cargo/bin:$PATH"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
INPUT_DIR="${1:-data/crossection_diff/2025-01-01}"
OUTPUT_DIR="${2:-data/crossection_diff/2025-01-01/parsed}"
PARALLEL_JOBS="${3:-4}"  # Number of parallel jobs (default: 4)

echo -e "${BLUE}==================================================="
echo "Real Data Processing Script (PARALLEL)"
echo -e "===================================================${NC}"
echo ""
echo -e "${GREEN}Input directory:${NC}  $INPUT_DIR"
echo -e "${GREEN}Output directory:${NC} $OUTPUT_DIR"
echo -e "${GREEN}Parallel jobs:${NC}   $PARALLEL_JOBS"
echo ""

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo -e "${YELLOW}Error: Input directory '$INPUT_DIR' not found${NC}"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Count total files
TOTAL_FILES=$(ls "$INPUT_DIR"/20251001_* 2>/dev/null | wc -l)

if [ "$TOTAL_FILES" -eq 0 ]; then
    echo -e "${YELLOW}No input files found in $INPUT_DIR${NC}"
    exit 1
fi

echo "Found $TOTAL_FILES files to process"
echo "Processing with $PARALLEL_JOBS parallel jobs"
echo ""

START_TIME=$(date +%s)

# Function to process a single file
process_file() {
    local input_file="$1"
    local output_dir="$2"
    local basename=$(basename "$input_file")
    local output_file="$output_dir/parsed_${basename}"

    # Skip if already processed
    if [ -f "$output_file" ]; then
        echo -e "${YELLOW}Skipping (already exists): $basename${NC}"
        return 0
    fi

    echo -e "${BLUE}Processing: $basename${NC}"

    # Run parser
    if cargo run --release --bin wikitext_parser_rust -- --input "$input_file" --output "$output_file" 2>&1 | grep -q "Processing complete"; then
        SIZE=$(du -h "$output_file" | cut -f1)
        echo -e "${GREEN}✓ Complete: $basename ($SIZE)${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠ Warning: Processing may have failed for $basename${NC}"
        return 1
    fi
}

export -f process_file
export OUTPUT_DIR
export GREEN BLUE YELLOW NC

# Check if GNU parallel is available
if command -v parallel &> /dev/null; then
    echo "Using GNU parallel for processing..."
    echo ""

    # Use GNU parallel for optimal parallelization
    ls "$INPUT_DIR"/20251001_* | parallel -j "$PARALLEL_JOBS" --bar process_file {} "$OUTPUT_DIR"

else
    echo "GNU parallel not found, using xargs for parallel processing..."
    echo ""

    # Fallback to xargs -P for parallel processing
    # Need to inline the function since bash -c doesn't inherit exported functions
    ls "$INPUT_DIR"/20251001_* | xargs -I {} -P "$PARALLEL_JOBS" bash -c '
        input_file="$1"
        output_dir="$2"
        basename=$(basename "$input_file")
        output_file="$output_dir/parsed_${basename}"

        # Skip if already processed
        if [ -f "$output_file" ]; then
            echo "Skipping (already exists): $basename"
            exit 0
        fi

        echo "Processing: $basename"

        # Run parser
        export PATH="$HOME/.cargo/bin:$PATH"
        if cargo run --release --bin wikitext_parser_rust -- --input "$input_file" --output "$output_file" 2>&1 | grep -q "Processing complete"; then
            SIZE=$(du -h "$output_file" | cut -f1)
            echo "✓ Complete: $basename ($SIZE)"
            exit 0
        else
            echo "⚠ Warning: Processing may have failed for $basename"
            exit 1
        fi
    ' _ {} "$OUTPUT_DIR"
fi

END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_MIN=$((TOTAL_TIME / 60))
TOTAL_SEC=$((TOTAL_TIME % 60))

echo ""
echo -e "${BLUE}==================================================="
echo "✓ Processing Complete!"
echo -e "===================================================${NC}"
echo ""
echo "Total files processed: $TOTAL_FILES"
echo "Total time: ${TOTAL_MIN}m ${TOTAL_SEC}s"
echo "Parallel jobs: $PARALLEL_JOBS"
echo ""
echo "Output files in: $OUTPUT_DIR"
echo ""
echo "To verify output:"
echo "  ls -lh $OUTPUT_DIR"
