# Vendored: parse-wiki-text-2

- Upstream: https://github.com/soerenmeier/parse-wiki-text-2
- Commit: bc6bde5cc463c57bfa38d6e9a27df49b4cf4a8a7
- Copied: 2026-10-27
- License: see `LICENSE` (kept as shipped upstream; `Cargo.toml` uses `license-file`).
- Omitted from the copy: `.git`, `.github`, `.gitignore`, `Cargo.lock`.

## Patches

1. Step budget (`src/parse.rs`, `src/configuration.rs`).
   - `parse::parse` takes `Limit::{Duration, Steps}` instead of a `Duration`, counts one step
     per main-loop iteration, and returns `(Output, steps)` on success.
   - New `ParseError::BudgetExceeded { steps, output }`: returned once the counter exceeds a
     non-zero budget, after flushing as the timeout branch does.
   - New `Configuration::parse_with_budget(text, max_steps)`; `0` means unlimited.
   - `parse`, `parse_with_timeout` and `parse_without_timeout` keep their signatures and
     behaviour (they drop the step count).

No build adjustments were needed: upstream `Cargo.toml` has no `[workspace]`, and its
dev-dependencies (clap) are not built for a path dependency.
