//! The only door to the wikitext parser: every later stage parses through here.

use parse_wiki_text_2::{Configuration, Output, ParseError};

/// A successful parse and the number of main-loop steps it took.
pub struct Parsed<'a> {
    pub output: Output<'a>,
    pub steps: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendError {
    /// The step budget was exceeded after `steps` steps.
    Budget { steps: u64 },
}

/// Parse `text` with a deterministic step budget (`0` means unlimited).
pub fn parse<'a>(
    text: &'a str,
    config: &Configuration,
    budget: u64,
) -> Result<Parsed<'a>, FrontendError> {
    match config.parse_with_budget(text, budget) {
        Ok((output, steps)) => Ok(Parsed { output, steps }),
        Err(ParseError::BudgetExceeded { steps, .. }) => Err(FrontendError::Budget { steps }),
        Err(ParseError::TimedOut { .. }) => unreachable!("a step budget never uses the wall clock"),
    }
}
