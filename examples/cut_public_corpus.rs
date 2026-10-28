//! Cuts the public (Wikipedia-only) test corpus out of the private v1 file.
//! Usage: cut_public_corpus <v1.parquet> <out_dir>

use anyhow::{bail, Result};
use std::fmt::Write as _;
use std::path::Path;
use wikitext_components::corpus::{self, CorpusRow};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        bail!("usage: cut_public_corpus <v1.parquet> <out_dir>");
    }
    let (src, out) = (Path::new(&args[1]), Path::new(&args[2]));
    let got = corpus::sha256_file(src)?;
    if got != corpus::PRIVATE_V1_SHA256 {
        bail!("v1 hash mismatch: {got}");
    }
    let rows = corpus::load(src)?;
    let public: Vec<CorpusRow> = rows
        .into_iter()
        .filter(|r| !r.version.starts_with("ruwiki"))
        .collect();
    std::fs::create_dir_all(out)?;
    let parquet = out.join("wikipedia_v1.parquet");
    corpus::write_public(&public, &parquet)?;
    std::fs::write(
        out.join("wikipedia_v1.sha256"),
        format!("{}\n", corpus::sha256_file(&parquet)?),
    )?;

    let mut md = String::from(
        "# Attribution\n\n\
         The wikitext in `wikipedia_v1.parquet` comes from the Russian Wikipedia (ru.wikipedia.org).\n\
         It is licensed under the Creative Commons Attribution-ShareAlike 4.0 International\n\
         license (CC BY-SA 4.0, https://creativecommons.org/licenses/by-sa/4.0/). Authorship of\n\
         each page is recorded in its revision history on ru.wikipedia.org. The pages were\n\
         selected and cut for parser testing, otherwise unmodified.\n\n\
         page_id | title | version | revision timestamp | source\n",
    );
    for r in &public {
        writeln!(
            md,
            "{} | {} | {} | {} | https://ru.wikipedia.org/?curid={}",
            r.page_id, r.page_title, r.version, r.timestamp, r.page_id
        )?;
    }
    std::fs::write(out.join("ATTRIBUTION.md"), md)?;
    println!("wrote {} public rows to {}", public.len(), out.display());
    Ok(())
}
