//! Builds the inputs of the frozen legacy binaries from the test corpus.
//! Usage: reshape_legacy <public.parquet> [<private v1.parquet>] <out_dir>
//! Writes <out_dir>/public/ and, when the private file is given, <out_dir>/private/.

use anyhow::{bail, Result};
use std::path::Path;
use wikitext_components::corpus;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (public, private, out) = match args.as_slice() {
        [a, o] => (a, None, o),
        [a, b, o] => (a, Some(b), o),
        _ => bail!("usage: reshape_legacy <public.parquet> [<private v1>] <out_dir>"),
    };
    let out = Path::new(out);
    let pub_rows = corpus::load(Path::new(public))?;
    let (singles, pairs) = corpus::public_legacy_selection(&pub_rows);
    corpus::write_legacy_inputs(&out.join("public"), &singles, &pairs)?;
    println!("public: {} singles, {} pairs", singles.len(), pairs.len());
    if let Some(p) = private {
        let got = corpus::sha256_file(Path::new(p))?;
        if got != corpus::PRIVATE_V1_SHA256 {
            bail!("private corpus hash differs: {got}");
        }
        let ruwiki: Vec<_> = corpus::load(Path::new(p))?
            .into_iter()
            .filter(|r| r.version == "ruwiki_2025")
            .collect();
        let (singles, pairs) = corpus::private_legacy_selection(&pub_rows, &ruwiki);
        corpus::write_legacy_inputs(&out.join("private"), &singles, &pairs)?;
        println!("private: {} singles, {} pairs", singles.len(), pairs.len());
    }
    Ok(())
}
