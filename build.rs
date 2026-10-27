//! Records the git commit for the `parse_components` manifest: `GIT_COMMIT` if set, else
//! `git rev-parse --short HEAD`, else `"unknown"`. Exposed to the crate as `env!("GIT_COMMIT")`.
use std::path::Path;
use std::process::Command;

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

fn main() {
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

    let commit = std::env::var("GIT_COMMIT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| git(&["rev-parse", "--short", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_COMMIT={commit}");
}
