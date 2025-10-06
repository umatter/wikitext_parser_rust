#!/bin/bash
# Export parsed parquet files to text files IN PARALLEL

set -e

export PATH="$HOME/.cargo/bin:$PATH"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
INPUT_DIR="${1:-data/crossection_diff/2025-01-01/parsed}"
OUTPUT_DIR_OFFICIAL="${2:-data/parsed_export/wiki}"
OUTPUT_DIR_CLONE="${3:-data/parsed_export/ruwiki}"
PARALLEL_JOBS="${4:-4}"  # Number of parallel jobs (default: 4)

echo -e "${BLUE}==================================================="
echo "Parsed Text Export Script (PARALLEL)"
echo -e "===================================================${NC}"
echo ""
echo -e "${GREEN}Input directory:${NC}       $INPUT_DIR"
echo -e "${GREEN}Output dir (wiki):${NC}    $OUTPUT_DIR_OFFICIAL"
echo -e "${GREEN}Output dir (ruwiki):${NC}  $OUTPUT_DIR_CLONE"
echo -e "${GREEN}Parallel jobs:${NC}        $PARALLEL_JOBS"
echo ""

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo -e "${YELLOW}Error: Input directory '$INPUT_DIR' not found${NC}"
    exit 1
fi

# Create output directories
mkdir -p "$OUTPUT_DIR_OFFICIAL"
mkdir -p "$OUTPUT_DIR_CLONE"

# Count total files
TOTAL_FILES=$(ls "$INPUT_DIR"/parsed_* 2>/dev/null | wc -l)

if [ "$TOTAL_FILES" -eq 0 ]; then
    echo -e "${YELLOW}No input files found in $INPUT_DIR${NC}"
    exit 1
fi

echo "Found $TOTAL_FILES files to export"
echo "Processing with $PARALLEL_JOBS parallel jobs"
echo ""

START_TIME=$(date +%s)

# Check if GNU parallel is available
if command -v parallel &> /dev/null; then
    echo "Using GNU parallel for processing..."
    echo ""

    # Use GNU parallel for optimal parallelization
    export -f
    ls "$INPUT_DIR"/parsed_* | parallel -j "$PARALLEL_JOBS" --bar \
        'basename=$(basename {}); echo "Processing: $basename"; \
         export PATH="$HOME/.cargo/bin:$PATH"; \
         cargo run --release --bin export_parsed -- {} "'"$OUTPUT_DIR_OFFICIAL"'" "'"$OUTPUT_DIR_CLONE"'" 2>&1 | grep -q "Export complete" && \
         echo "✓ Complete: $basename" || echo "⚠ Warning: $basename"'

else
    echo "GNU parallel not found, using xargs for parallel processing..."
    echo ""

    # Fallback to xargs -P for parallel processing
    ls "$INPUT_DIR"/parsed_* | xargs -I {} -P "$PARALLEL_JOBS" bash -c '
        input_file="$1"
        output_dir_official="$2"
        output_dir_clone="$3"
        basename=$(basename "$input_file")

        echo "Processing: $basename"

        # Run export
        export PATH="$HOME/.cargo/bin:$PATH"
        if cargo run --release --bin export_parsed -- "$input_file" "$output_dir_official" "$output_dir_clone" 2>&1 | grep -q "Export complete"; then
            echo "✓ Complete: $basename"
            exit 0
        else
            echo "⚠ Warning: Export may have failed for $basename"
            exit 1
        fi
    ' _ {} "$OUTPUT_DIR_OFFICIAL" "$OUTPUT_DIR_CLONE"
fi

END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_MIN=$((TOTAL_TIME / 60))
TOTAL_SEC=$((TOTAL_TIME % 60))

echo ""
echo -e "${BLUE}==================================================="
echo "✓ Export Complete!"
echo -e "===================================================${NC}"
echo ""
echo "Total files processed: $TOTAL_FILES"
echo "Total time: ${TOTAL_MIN}m ${TOTAL_SEC}s"
echo "Parallel jobs: $PARALLEL_JOBS"
echo ""
echo "Output directories:"
echo "  Wiki:   $OUTPUT_DIR_OFFICIAL"
echo "  Ruwiki: $OUTPUT_DIR_CLONE"
echo ""
echo "To verify output:"
echo "  ls -lh $OUTPUT_DIR_OFFICIAL | head -20"
echo "  ls -lh $OUTPUT_DIR_CLONE | head -20"
