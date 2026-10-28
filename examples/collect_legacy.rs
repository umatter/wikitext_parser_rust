//! Collects the outputs of the legacy binaries into the committed baselines.
//! Usage: collect_legacy <run_dir> <out_dir>
//! Writes <out_dir>/expected/legacy_wikipedia_v1.parquet and, when <run_dir>/private
//! exists, <out_dir>/expected/legacy_private.sha256 and <out_dir>/private/legacy_ruwiki_v1.parquet.
//! <out_dir> is the repository's tests/ directory.

use anyhow::{bail, Result};
use std::path::Path;
use wikitext_components::corpus;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        bail!("usage: collect_legacy <run_dir> <out_dir>");
    }
    let (run, out) = (Path::new(&args[0]), Path::new(&args[1]));
    std::fs::create_dir_all(out.join("expected"))?;
    let public = corpus::collect_legacy(&run.join("public"))?;
    corpus::write_legacy_long(&public, &out.join("expected/legacy_wikipedia_v1.parquet"))?;
    println!("public: {} rows", public.len());
    if run.join("private").exists() {
        let private = corpus::collect_legacy(&run.join("private"))?;
        std::fs::write(out.join("expected/legacy_private.sha256"), corpus::hash_lines(&private))?;
        std::fs::create_dir_all(out.join("private"))?;
        corpus::write_legacy_long(&private, &out.join("private/legacy_ruwiki_v1.parquet"))?;
        println!("private: {} rows", private.len());
    }
    Ok(())
}
