#!/usr/bin/env bash
# Regenerates the legacy baselines from the frozen v0.1.0 binaries.
#   tests/expected/legacy_wikipedia_v1.parquet   public outputs (committed)
#   tests/expected/legacy_private.sha256         hashes of private outputs (committed)
#   tests/private/legacy_ruwiki_v1.parquet       readable private outputs (gitignored)
# The private part runs only when the private corpus exists; its path is
# $WIKITEXT_PRIVATE_CORPUS or the default below, relative to the repository root.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PRIVATE="${WIKITEXT_PRIVATE_CORPUS:-../ruwiki_replication_package_sources/data/results/sources/test_corpus/parser_test_corpus_v1.parquet}"
TMP="$(mktemp -d)"
cleanup() {
  git -C "$ROOT" worktree remove --force "$TMP/v010" 2>/dev/null || true
  git -C "$ROOT" worktree prune
  rm -rf "$TMP"
}
trap cleanup EXIT

git worktree add --detach "$TMP/v010" v0.1.0
cp Cargo.lock "$TMP/v010/Cargo.lock"
(cd "$TMP/v010" && cargo build --release)
BIN="$TMP/v010/target/release"

cargo build --release --examples
EX="$ROOT/target/release/examples"
RUN="$TMP/run"
mkdir -p "$RUN"
if [ -f "$PRIVATE" ]; then
  "$EX/reshape_legacy" tests/corpus/wikipedia_v1.parquet "$PRIVATE" "$RUN"
  SETS="public private"
else
  echo "private corpus absent, generating the public baseline only"
  "$EX/reshape_legacy" tests/corpus/wikipedia_v1.parquet "$RUN"
  SETS="public"
fi

for set in $SETS; do
  d="$RUN/$set"
  "$BIN/parse_single" --input "$d/single.parquet" --output "$d/single.dirty.parquet" --timeout 0 >/dev/null 2>&1
  "$BIN/clean_parsed" --input "$d/single.dirty.parquet" --output "$d/single.clean.parquet" >/dev/null 2>&1
  "$BIN/wikitext_parser_rust" --input "$d/pair.parquet" --output "$d/pair.dirty.parquet" --timeout 0 >/dev/null 2>&1
  "$BIN/clean_parsed" --input "$d/pair.dirty.parquet" --output "$d/pair.clean.parquet" >/dev/null 2>&1
  "$BIN/export_parsed" "$d/pair.clean.parquet" "$d/export/official" "$d/export/clone" >/dev/null 2>&1
done

"$EX/collect_legacy" "$RUN" tests
