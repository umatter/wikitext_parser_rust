#!/bin/bash
# Installation script for wikitext_parser_rust

set -e

echo "==================================================="
echo "Wikitext Parser - Installation Script"
echo "==================================================="
echo ""

# Check if Rust is already installed
if command -v cargo &> /dev/null; then
    echo "✓ Rust is already installed"
    rustc --version
    cargo --version
else
    echo "Installing Rust toolchain..."
    echo ""

    # Install Rust using rustup
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

    # Add cargo to PATH for this session
    export PATH="$HOME/.cargo/bin:$PATH"

    echo ""
    echo "✓ Rust installed successfully"
    rustc --version
    cargo --version
fi

echo ""
echo "==================================================="
echo "Building the project..."
echo "==================================================="
echo ""

# Ensure cargo is in PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Build the project in release mode for better performance
cargo build --release

echo ""
echo "==================================================="
echo "✓ Installation complete!"
echo "==================================================="
echo ""
echo "The parser uses a two-phase process:"
echo ""
echo "Phase 1 - Parse wikitext:"
echo "  cargo run --release --bin wikitext_parser_rust -- --input input.parquet --output dirty.parquet"
echo ""
echo "Phase 2 - Clean output:"
echo "  cargo run --release --bin clean_parsed -- --input dirty.parquet --output clean.parquet"
echo ""
echo "Options:"
echo "  --skip-lists     Remove all lists from output"
echo "  --timeout 0      Disable timeout for maximum speed"
echo "  --timeout 60     Custom timeout (seconds)"
echo ""
