#!/bin/bash
# Process real Wikipedia data from crossection_diff directory IN PARALLEL
# Two-phase approach: Phase 1 (parse wikitext) → Phase 2 (clean output)

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
TIMEOUT="${4:-30}"       # Timeout in seconds per article (default: 30, 0=disabled)
KEEP_DIRTY="${5:-false}" # Keep intermediate "dirty" files (default: false)

echo -e "${BLUE}==================================================="
echo "Real Data Processing Script (PARALLEL)"
echo "Two-phase: Parse → Clean"
echo -e "===================================================${NC}"
echo ""
echo -e "${GREEN}Input directory:${NC}  $INPUT_DIR"
echo -e "${GREEN}Output directory:${NC} $OUTPUT_DIR"
echo -e "${GREEN}Parallel jobs:${NC}   $PARALLEL_JOBS"
echo -e "${GREEN}Timeout:${NC}         $TIMEOUT seconds per article"
echo -e "${GREEN}Keep dirty files:${NC} $KEEP_DIRTY"
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

# Phase 1: Parse wikitext (produces "dirty" output with potential template fragments)
process_file_phase1() {
    local input_file="$1"
    local dirty_dir="$2"
    local timeout="$3"
    local basename=$(basename "$input_file")
    local dirty_file="$dirty_dir/dirty_${basename}"

    # Skip if already processed
    if [ -f "$dirty_file" ]; then
        echo -e "${YELLOW}Phase 1 - Skipping (already exists): $basename${NC}"
        return 0
    fi

    echo -e "${BLUE}Phase 1 - Parsing: $basename${NC}"

    # Run parser with timeout
    if cargo run --release --bin wikitext_parser_rust -- --input "$input_file" --output "$dirty_file" --timeout "$timeout" 2>&1 | grep -q "Processing complete"; then
        SIZE=$(du -h "$dirty_file" | cut -f1)
        echo -e "${GREEN}✓ Phase 1 complete: $basename ($SIZE)${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠ Warning: Phase 1 may have failed for $basename${NC}"
        return 1
    fi
}

# Phase 2: Clean parsed output (removes template fragments and image markup)
process_file_phase2() {
    local dirty_file="$1"
    local output_dir="$2"
    local basename=$(basename "$dirty_file")
    # Remove "dirty_" prefix for final output
    local final_basename="${basename#dirty_}"
    local output_file="$output_dir/parsed_${final_basename}"

    # Skip if already processed
    if [ -f "$output_file" ]; then
        echo -e "${YELLOW}Phase 2 - Skipping (already exists): $final_basename${NC}"
        return 0
    fi

    echo -e "${BLUE}Phase 2 - Cleaning: $final_basename${NC}"

    # Run cleaner
    if cargo run --release --bin clean_parsed -- --input "$dirty_file" --output "$output_file" 2>&1 | grep -q "Cleaning complete"; then
        SIZE=$(du -h "$output_file" | cut -f1)
        echo -e "${GREEN}✓ Phase 2 complete: $final_basename ($SIZE)${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠ Warning: Phase 2 may have failed for $final_basename${NC}"
        return 1
    fi
}

export -f process_file_phase1
export -f process_file_phase2
export OUTPUT_DIR DIRTY_DIR TIMEOUT
export GREEN BLUE YELLOW NC

# Check if GNU parallel is available
if command -v parallel &> /dev/null; then
    echo "Using GNU parallel for processing..."
    echo ""

    # PHASE 1: Parse all files in parallel
    echo -e "${BLUE}=== PHASE 1: Parsing wikitext ===${NC}"
    PHASE1_START=$(date +%s)
    ls "$INPUT_DIR"/20251001_* | parallel -j "$PARALLEL_JOBS" --bar process_file_phase1 {} "$DIRTY_DIR" "$TIMEOUT"
    PHASE1_END=$(date +%s)
    PHASE1_TIME=$((PHASE1_END - PHASE1_START))
    echo -e "${GREEN}✓ Phase 1 complete (${PHASE1_TIME}s)${NC}"
    echo ""

    # PHASE 2: Clean all files in parallel
    echo -e "${BLUE}=== PHASE 2: Cleaning output ===${NC}"
    PHASE2_START=$(date +%s)
    ls "$DIRTY_DIR"/dirty_* 2>/dev/null | parallel -j "$PARALLEL_JOBS" --bar process_file_phase2 {} "$OUTPUT_DIR"
    PHASE2_END=$(date +%s)
    PHASE2_TIME=$((PHASE2_END - PHASE2_START))
    echo -e "${GREEN}✓ Phase 2 complete (${PHASE2_TIME}s)${NC}"
    echo ""

else
    echo "GNU parallel not found, using xargs for parallel processing..."
    echo ""

    # PHASE 1: Parse all files in parallel
    echo -e "${BLUE}=== PHASE 1: Parsing wikitext ===${NC}"
    PHASE1_START=$(date +%s)
    ls "$INPUT_DIR"/20251001_* | xargs -I {} -P "$PARALLEL_JOBS" bash -c '
        input_file="$1"
        dirty_dir="$2"
        timeout="$3"
        basename=$(basename "$input_file")
        dirty_file="$dirty_dir/dirty_${basename}"

        # Skip if already processed
        if [ -f "$dirty_file" ]; then
            echo "Phase 1 - Skipping (already exists): $basename"
            exit 0
        fi

        echo "Phase 1 - Parsing: $basename"

        # Run parser with timeout
        export PATH="$HOME/.cargo/bin:$PATH"
        if cargo run --release --bin wikitext_parser_rust -- --input "$input_file" --output "$dirty_file" --timeout "$timeout" 2>&1 | grep -q "Processing complete"; then
            SIZE=$(du -h "$dirty_file" | cut -f1)
            echo "✓ Phase 1 complete: $basename ($SIZE)"
            exit 0
        else
            echo "⚠ Warning: Phase 1 may have failed for $basename"
            exit 1
        fi
    ' _ {} "$DIRTY_DIR" "$TIMEOUT"
    PHASE1_END=$(date +%s)
    PHASE1_TIME=$((PHASE1_END - PHASE1_START))
    echo -e "${GREEN}✓ Phase 1 complete (${PHASE1_TIME}s)${NC}"
    echo ""

    # PHASE 2: Clean all files in parallel
    echo -e "${BLUE}=== PHASE 2: Cleaning output ===${NC}"
    PHASE2_START=$(date +%s)
    ls "$DIRTY_DIR"/dirty_* 2>/dev/null | xargs -I {} -P "$PARALLEL_JOBS" bash -c '
        dirty_file="$1"
        output_dir="$2"
        basename=$(basename "$dirty_file")
        final_basename="${basename#dirty_}"
        output_file="$output_dir/parsed_${final_basename}"

        # Skip if already processed
        if [ -f "$output_file" ]; then
            echo "Phase 2 - Skipping (already exists): $final_basename"
            exit 0
        fi

        echo "Phase 2 - Cleaning: $final_basename"

        # Run cleaner
        export PATH="$HOME/.cargo/bin:$PATH"
        if cargo run --release --bin clean_parsed -- --input "$dirty_file" --output "$output_file" 2>&1 | grep -q "Cleaning complete"; then
            SIZE=$(du -h "$output_file" | cut -f1)
            echo "✓ Phase 2 complete: $final_basename ($SIZE)"
            exit 0
        else
            echo "⚠ Warning: Phase 2 may have failed for $final_basename"
            exit 1
        fi
    ' _ {} "$OUTPUT_DIR"
    PHASE2_END=$(date +%s)
    PHASE2_TIME=$((PHASE2_END - PHASE2_START))
    echo -e "${GREEN}✓ Phase 2 complete (${PHASE2_TIME}s)${NC}"
    echo ""
fi

# Optionally remove dirty files
if [ "$KEEP_DIRTY" = "false" ]; then
    echo "Cleaning up intermediate files..."
    rm -rf "$DIRTY_DIR"
    echo -e "${GREEN}✓ Dirty files removed${NC}"
    echo ""
fi

END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_MIN=$((TOTAL_TIME / 60))
TOTAL_SEC=$((TOTAL_TIME % 60))

echo ""
echo -e "${BLUE}==================================================="
echo "✓ Two-Phase Processing Complete!"
echo -e "===================================================${NC}"
echo ""
echo "Total files processed: $TOTAL_FILES"
echo "Phase 1 (Parse):  ${PHASE1_TIME}s"
echo "Phase 2 (Clean):  ${PHASE2_TIME}s"
echo "Total time:       ${TOTAL_MIN}m ${TOTAL_SEC}s"
echo "Parallel jobs:    $PARALLEL_JOBS"
echo "Timeout setting:  $TIMEOUT seconds"
echo ""
echo "Output files in: $OUTPUT_DIR"
echo ""
echo "To verify output:"
echo "  ls -lh $OUTPUT_DIR"
