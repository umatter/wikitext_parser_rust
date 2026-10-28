//! `parse_components`: structured parse of wikitext into one long Parquet table per component type.
//!
//! parse_components --input <file-or-dir> --out <dir> --label <key>=<site>:<version> ...
//! parse_components --input article.wiki --label wikipedia:adhoc --format json --out article.json
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use wikitext_components::driver::{self, Format, RunArgs};
use wikitext_components::input::{parse_label, Layout};
use wikitext_components::page::{self, Limits};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LayoutArg {
    Pair,
    Single,
    Long,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum FormatArg {
    Parquet,
    Json,
}

#[derive(Parser, Debug)]
#[command(
    name = "parse_components",
    about = "Structured parse of wikitext: one long Parquet table per component type"
)]
struct Cli {
    /// Input: a Parquet file, a directory (its *.parquet files, sorted by name) or a .wiki file.
    /// Repeatable.
    #[arg(long, required = true)]
    input: Vec<PathBuf>,

    /// Output directory (for a .wiki input with --format json: the JSON file).
    #[arg(long)]
    out: PathBuf,

    /// <key>=<site>:<version>, required for every text and never inferred. <key> is a text column
    /// (pair and single layouts) or a value of the version column (long layout). A .wiki input takes
    /// <site>:<version>.
    #[arg(long = "label", required = true)]
    labels: Vec<String>,

    /// Input layout; detected from the columns when omitted.
    #[arg(long, value_enum)]
    layout: Option<LayoutArg>,

    /// Text column of a single-column input (detected as parse_single does when omitted).
    #[arg(long)]
    text_column: Option<String>,

    /// parquet: one part file per input and table. json: one document per page version
    /// (`<out>/<stem>.jsonl` for a Parquet input, a pretty document at --out for a .wiki input).
    #[arg(long, value_enum, default_value = "parquet")]
    format: FormatArg,

    /// Write only these tables (comma-separated); pages is always written.
    #[arg(long, value_delimiter = ',')]
    tables: Option<Vec<String>>,

    /// Worker threads (default: one per core).
    #[arg(long)]
    jobs: Option<usize>,

    /// Parser step budget per page.
    #[arg(long, default_value_t = page::DEFAULT_PARSE_BUDGET)]
    budget: u64,

    /// Give pages the parser cannot finish their failure status instead of the fallback scanner's rows.
    #[arg(long)]
    no_fallback: bool,

    /// Skip inputs whose pages part is finished; pages in <out>/quarantine.list get status "hang".
    #[arg(long)]
    resume: bool,

    /// Directory holding sites/*.toml (default: config/ of the source tree this binary was built from).
    #[arg(long)]
    config_dir: Option<PathBuf>,
}

fn run_args(cli: Cli) -> Result<RunArgs> {
    Ok(RunArgs {
        inputs: cli.input,
        out: cli.out,
        labels: cli.labels.iter().map(|l| parse_label(l)).collect::<Result<_>>()?,
        layout: cli.layout.map(|l| match l {
            LayoutArg::Pair => Layout::Pair,
            LayoutArg::Single => Layout::Single,
            LayoutArg::Long => Layout::Long,
        }),
        text_column: cli.text_column,
        format: match cli.format {
            FormatArg::Parquet => Format::Parquet,
            FormatArg::Json => Format::Json,
        },
        tables: cli.tables,
        jobs: cli.jobs.unwrap_or(0),
        limits: Limits {
            parse_budget: cli.budget,
            walk_budget: page::DEFAULT_WALK_BUDGET,
            depth_cap: page::DEFAULT_DEPTH_CAP,
            fallback: !cli.no_fallback,
        },
        resume: cli.resume,
        config_dir: cli.config_dir.unwrap_or_else(driver::default_config_dir),
    })
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = run_args(cli).and_then(|args| driver::run(&args).map(|m| (args, m)));
    match result {
        Ok((_, m)) if m.failed_inputs.is_empty() => ExitCode::SUCCESS,
        Ok((args, m)) => {
            eprintln!(
                "error: {} input file(s) failed; see failed_inputs in {}",
                m.failed_inputs.len(),
                args.out.join("manifest.json").display()
            );
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}
