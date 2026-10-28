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
     non-zero budget, after flushing as the timeout branch does. The doc comment of `ParseError`
     names both errors.
   - New `Configuration::parse_with_budget(text, max_steps)`; `0` means unlimited.
   - `parse`, `parse_with_timeout` and `parse_without_timeout` keep their signatures and
     behaviour (they drop the step count).
2. Trie (`src/trie.rs`). Marked "Patch: not in upstream".
   - Upstream's trie stores a word's last byte as a final edge, so a word that is a prefix of another
     cannot be stored: whichever is inserted second is dropped. Site settings need both (`#перенапр`
     and `#перенаправление`, `__ОГЛ__` and `__ОГЛАВЛЕНИЕ__`), and a silently dropped alias makes
     redirects and magic words go undetected.
   - Every state can now carry a payload and edges; `find` returns the longest stored word that matches
     and falls back to the last word seen on the path when a longer candidate fails. Insertion order does
     not matter. `add_term` still returns `false` for a word that is already stored. Case folding and the
     `find` result shape are unchanged. Unit tests in `src/trie.rs`.

No build adjustments were needed: upstream `Cargo.toml` has no `[workspace]`, and its
dev-dependencies (clap) are not built for a path dependency.
