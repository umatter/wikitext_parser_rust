//! Build facts for the `parse_components` manifest:
//! - the git commit: `GIT_COMMIT` if set, else `git rev-parse --short HEAD`, else `"unknown"`,
//!   exposed as `env!("GIT_COMMIT")`;
//! - the vendored parser's patch titles (the `## Patches` section of
//!   `vendor/parse-wiki-text-2/VENDORED.md`) and a sha256 of its source, written to
//!   `$OUT_DIR/vendored.rs` as `VENDORED_PATCHES` and `VENDORED_SRC_SHA256`. They identify the
//!   patched parser when there is no git commit (a build from a source archive).
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

const VENDORED_MD: &str = "vendor/parse-wiki-text-2/VENDORED.md";
const VENDORED_SRC: &str = "vendor/parse-wiki-text-2/src";

fn git(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn git_commit() -> String {
    println!("cargo:rerun-if-env-changed=GIT_COMMIT");
    // HEAD and the current ref (`.git/HEAD` and `.git/refs/heads/<branch>` in a plain checkout).
    // Resolved through git so that a worktree, whose `.git` is a file, works too; only existing
    // paths are named, since a missing one would rerun this script on every build.
    if let Some(dir) = git(&["rev-parse", "--git-dir"]) {
        let head = Path::new(&dir).join("HEAD");
        if head.exists() {
            println!("cargo:rerun-if-changed={}", head.display());
        }
        if let Some(r) = git(&["symbolic-ref", "-q", "HEAD"]) {
            let common = git(&["rev-parse", "--git-common-dir"]).unwrap_or(dir);
            let loose = Path::new(&common).join(&r);
            let packed = Path::new(&common).join("packed-refs");
            if loose.exists() {
                println!("cargo:rerun-if-changed={}", loose.display());
            } else if packed.exists() {
                println!("cargo:rerun-if-changed={}", packed.display());
            }
        }
    }
    std::env::var("GIT_COMMIT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| git(&["rev-parse", "--short", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_string())
}

/// The title of a list item or heading: its first sentence, without the closing full stop.
fn title(s: &str) -> String {
    let s = s.trim();
    let s = s.split_once(". ").map_or(s, |(first, _)| first);
    s.strip_suffix('.').unwrap_or(s).trim().to_string()
}

/// One title per patch in the `## Patches` section: its `###` headings if it has any, else its
/// top-level list items (`1.`, `-` or `*` at the start of a line).
fn patch_titles(md: &str) -> Vec<String> {
    let mut in_section = false;
    let (mut headings, mut items) = (vec![], vec![]);
    for line in md.lines() {
        // A level-1 or level-2 heading opens the next section.
        if line.starts_with("# ") || line.starts_with("## ") {
            in_section = line.strip_prefix("## ").is_some_and(|h| h.trim() == "Patches");
            continue;
        }
        if !in_section {
            continue;
        }
        if let Some(h) = line.strip_prefix("### ") {
            headings.push(title(h));
        } else if let Some(rest) = line.strip_prefix("- ").or_else(|| line.strip_prefix("* ")) {
            items.push(title(rest));
        } else {
            let digits = line.chars().take_while(char::is_ascii_digit).count();
            if digits > 0 {
                if let Some(rest) = line[digits..].strip_prefix(". ") {
                    items.push(title(rest));
                }
            }
        }
    }
    if headings.is_empty() {
        items
    } else {
        headings
    }
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()));
    for e in entries {
        let p = e.unwrap_or_else(|e| panic!("reading {}: {e}", dir.display())).path();
        if p.is_dir() {
            files_under(&p, out);
        } else {
            out.push(p);
        }
    }
}

/// sha256 over the files under `dir`, in sorted relative-path order: each file's path (relative
/// to `dir`, `/`-separated, UTF-8) then its contents.
fn tree_sha256(dir: &Path) -> String {
    let mut files = vec![];
    files_under(dir, &mut files);
    let mut rel: Vec<(String, PathBuf)> = files
        .into_iter()
        .map(|p| {
            let r = p.strip_prefix(dir).unwrap();
            let parts: Vec<String> = r.iter().map(|c| c.to_string_lossy().into_owned()).collect();
            (parts.join("/"), p)
        })
        .collect();
    rel.sort();
    let mut h = Sha256::new();
    for (r, p) in rel {
        h.update(r.as_bytes());
        h.update(std::fs::read(&p).unwrap_or_else(|e| panic!("reading {}: {e}", p.display())));
    }
    h.finalize().iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn main() {
    let commit = git_commit();
    println!("cargo:rustc-env=GIT_COMMIT={commit}");

    println!("cargo:rerun-if-changed={VENDORED_MD}");
    println!("cargo:rerun-if-changed={VENDORED_SRC}");
    let md = std::fs::read_to_string(VENDORED_MD).unwrap_or_else(|e| panic!("reading {VENDORED_MD}: {e}"));
    let patches = patch_titles(&md);
    assert!(!patches.is_empty(), "{VENDORED_MD}: no patches found under '## Patches'");
    let sha = tree_sha256(Path::new(VENDORED_SRC));

    let mut code = String::from("/// Patch titles from `vendor/parse-wiki-text-2/VENDORED.md` (`## Patches`).\n");
    code.push_str("pub const VENDORED_PATCHES: &[&str] = &[\n");
    for p in &patches {
        let _ = writeln!(code, "    {p:?},");
    }
    code.push_str("];\n");
    code.push_str("/// sha256 over `vendor/parse-wiki-text-2/src/`: each file in sorted relative-path order,\n");
    code.push_str("/// its path (relative, `/`-separated) then its contents.\n");
    let _ = writeln!(code, "pub const VENDORED_SRC_SHA256: &str = {sha:?};");
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("vendored.rs");
    std::fs::write(&out, code).unwrap_or_else(|e| panic!("writing {}: {e}", out.display()));
}
