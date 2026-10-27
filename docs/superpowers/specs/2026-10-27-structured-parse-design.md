# Structured parse of a page: design

Date: 2026-10-27. Branch: `feature/components`. Status: design, not yet planned.

## Why

The parser turns a page into prose and throws everything else away. That was right for the paper's text
comparison, but the references-and-sources workstream of the revision (`ruwiki_replication_package_sources`,
branch `rev/sources`: R4.M3D, restore citations and sources and analyse sourcing; R2.M2b, the shift towards
Russian government sources) needs what the parser drops. Its pair-layer design note says so directly: "A real
sourcing analysis (R4.M3D) would need the parser to keep references, and is a separate workstream."

The target is a full structured parse of a page:

- the section tree and the paragraphs;
- references: inline `<ref>`, named and reused refs, citation templates with their fields, and the reference
  list;
- templates with their parameters, infoboxes included;
- links (internal, external, interwiki), categories, files, tables and lists.

Every component keeps its position in the page, so that it can be tied to the prose around it and compared
across the two versions of the same page (Wikipedia and Ruwiki).

## Goals

1. **Complete and general**: every component type listed above, from any wikitext article, in any version.
   That covers both columns of every pair family (cross-section, Wikipedia and Ruwiki time series, so the
   fork-day text too), single-column inputs (deleted and added pages), and a plain `.wiki` file. The code
   knows no site: what it knows about a wiki lives in that site's configuration file and in the registry, so
   another wiki needs data, not code (D3).
2. **Aligned**: every component carries its source span, its section, and, when it sits inside prose, the
   paragraph and the character offset in that paragraph's text. Components carry content fingerprints and
   section paths so the two versions of a page can be joined.
3. **Robust**: malformed markup, deep nesting and pathological pages never stop a run. Every input page
   yields a row that says what happened to it. A page that fails is recorded with the reason, never skipped
   silently.
4. **Efficient**: streams Parquet in and out, runs in parallel inside one process, and handles the full
   corpus on one machine.
5. **Stable**: a versioned output schema; a fixed test corpus of real pages with expected outputs; the
   current prose output unchanged, with v0.1.0 as the regression baseline.

## Non-goals

- **Expanding templates.** No Lua, no parser functions, no transclusion. See decision D4.
- **Matching components across the two versions.** The parser emits the keys a match needs (fingerprints,
  section paths, positions). The matching rules (when is a citation "the same, but changed"?) are analysis
  decisions. They belong to the sources workstream, or to a later design.
- **Classifying sources.** Whether a domain is a government site or state media is decided downstream. The
  parser extracts the URL, host and registrable domain.
- **Changing the paper's texts.** The paper stays on v0.1.0 (pinned in `setup_rust_env.sh` of the replication
  package). The new prose in the structured output is a different text and is documented as such.
- HTML rendering, writing wikitext back, resolving table grids with row and column spans.
- Categories, refs and links that only templates produce. They are invisible without expansion. This is a
  documented limitation, not a bug.

## Anti-goals

These would make the work a failure even if it runs:

- **Any byte of legacy output changes.** `wikitext_parser_rust`, `clean_parsed`, `parse_single` and
  `export_parsed` produce exactly what v0.1.0 produces.
- **Parser artefacts that look like edits.** The pair layer already lost time to this ("parser residue",
  `08c_parser_residue.py`): the parser dropped words that readers saw, and the diff showed an insertion that
  no editor made. In sourcing terms, the risk is a citation template that is recognised on one side and not
  on the other (a Ruwiki rename, a redirect alias), so that a citation seems to be removed. Every
  side-specific rule must be explicit, and the template coverage checks run on both sides.
- **Silent loss.** A page, a ref or a citation that disappears without a status. This matters most for the
  pages that time out: they are the longest and most heavily sourced articles, which the sourcing analysis
  can least afford to lose.
- **Non-reproducible output.** Same input, same build, same bytes, on any machine and under any load. A page's
  status must not depend on how fast the machine is (D8). No wall-clock values go in the component tables or
  in `pages`. Timings go to the manifest and to a separate `timings` table, which is excluded from every
  reproducibility check.

## Constraints

- Rust, in this repository, on this branch.
- The legacy source files (`src/parser.rs`, `src/main.rs`, `src/clean_parsed.rs`, `src/parse_single.rs`,
  `src/export_parsed.rs`) and the shell scripts keep their behaviour. Prefer not to touch them at all. The
  legacy path keeps `parse_wiki_text = "0.1"` (0.1.5).
- Input schemas as today: the two-column pair layout (`page_id`, `page_title`, `official_text`,
  `official_timestamp`, `clone_page_title`, `clone_text`, `clone_timestamp`) and the single-column layouts
  that `parse_single` auto-detects. Three file families share the pair layout
  (`01_fetch_wikitext_data.R`):
  - `crossection_diff`: Wikipedia 2025 against Ruwiki 2025;
  - `timeseries_diff`: Wikipedia at the fork day against Wikipedia later, so both columns are Wikipedia;
  - `ruwiki_timeseries_diff`.

  So the column name does not tell which site a text comes from. A file can also hold several rows for the
  same `page_id` (`08b_forkday_wikitext.py` deduplicates them by timestamp).
- Consumers read the output in R (arrow), Python (pyarrow, pandas) and Athena (the replication package
  exports a Glue catalog). Column types must survive all three: no unsigned 64-bit integers, no Arrow map
  type.
- `.gitignore` excludes `*.parquet`, `*.txt` and `Cargo.lock`. The test corpus and the lockfile must not
  fall under those rules (see D10).
- The machine at hand has 12 cores and 62 GB of RAM.

## What exists now

Findings from reading the code and the libraries, which shape the decisions below.

- **The parser runs on the crate's English default configuration** (`Configuration::default()`). It knows
  `File:`, `Image:` and `Category:`, but not `Файл:`, `Изображение:` or `Категория:` (or `К:`). So
  `[[Файл:...]]` and `[[Категория:...]]` are parsed as ordinary links, which is why `remove_image_fragments`
  and the `Категория:` checks in `remove_empty_sections` exist, and why captions of `[[Файл:]]` links survive
  while those of `[[File:]]` links do not (as `08c_parser_residue.py` observes). The link trail is Latin
  letters only. Russian Wikipedia's is `/^([a-zабвгдеёжзийклмнопрстуфхцчшщъыьэюя]+)(.*)$/sDu`.
- **The original crate is gone upstream.** `parse_wiki_text` 0.1.5 was released on 2019-02-12, and its
  repository (`github.com/portstrom/parse_wiki_text`) now returns 404. Only the crates.io copy is left.
- **A maintained fork exists.** `parse-wiki-text-2` (`github.com/soerenmeier/parse-wiki-text-2`) has the same
  AST: the same `Node` variants with byte `start`/`end` on every node. It adds a cooperative timeout
  (`Configuration::parse_with_timeout`, which returns a `ParseError` instead of hanging). On master (May 2026,
  not released; the last release is 0.2.0 from 2024-03-03) it fixes the bug where text before `<nowiki>` and
  `<math>` was dropped (issue #10, one of the residue rules in `08c`), fixes a timeout panic on non-UTF-8
  boundaries (#8) and adds `onlyinclude`/`includeonly`/`noinclude`. It still has an open issue: "Parsing has
  worst case exponential runtime" (#1). This is the likely cause of the pages that time out today. It uses
  edition 2024 and MSRV 1.90. The local toolchain is 1.99.1. Same permissive license as the original.
- **`Tag` nodes carry no attributes.** `<ref name="a" group="b">` arrives as `Tag { name: "ref", nodes, start,
  end }`. Attributes have to be read from the source slice. The contents of `ref` are parsed into nodes, so
  templates inside refs arrive as `Template` nodes. The contents of `nowiki` and `math` are raw.
- **The timeout leaks threads.** `parse_wikitext_with_timeout` spawns a thread per article and abandons it on
  timeout. The thread keeps running, because Rust threads cannot be killed. Every page is processed
  sequentially, and `main` collects the whole input file into memory before processing. Regexes are compiled
  on every call. Parallelism comes from the shell scripts, one `cargo run` per file.
- **No tests.** There is no test directory and no expected output.
- **A newer wiki crate exists, but it is too young.** `wikrs` (0.4.0, July 2026, MIT/Apache) claims linear-time
  parsing with span-carrying diagnostics. It is two months old and has one maintainer. Its design document
  calls the repository private, and it does not document spans on AST nodes or namespace configuration for
  other languages. It is worth watching, but too early to rest the paper's sourcing analysis on.
- **Ruwiki's API refused a siteinfo request** from this machine (HTTP 403). Russian Wikipedia's answered.

## Decisions

### D1. Two paths: legacy frozen, structured new

The legacy prose path stays exactly as it is, on `parse_wiki_text` 0.1.5 with the default configuration and
its quirks, because those quirks are part of v0.1.0's output. The structured parse is a new binary,
`parse_components`, with its own frontend and configuration. The two share no parsing code. The structured
output has its own paragraph text (D6). It is cleaner text: Russian namespaces are recognised, so file
captions and category links stay out, and Cyrillic link trails stay in. But it is not the paper's text, and
the documentation says so.

This turns the first open question into a narrower one: the old crate has to stay for the legacy path no
matter what. The choice is only about the structured path.

### D2. Frontend: vendored `parse-wiki-text-2` behind an adapter (open question 1)

**Answer: move the structured path to `parse-wiki-text-2`, vendored at a recorded upstream commit, and keep
the rest of the code off its types.**

- **Why the fork.** It has the same AST and the same byte spans, so nothing is lost relative to today. It adds
  a cooperative timeout, which ends the leaked-thread problem (we patch it into a counted step budget, D8),
  and on master it fixes the `<nowiki>` text drop
  that the pair layer already had to work around. A rewrite or a two-month-old crate would put the sourcing
  analysis on unproven ground.
- **Why vendored, not a crates.io or git dependency.** The fixes are unreleased, so 0.2.0 from crates.io lacks
  them. A git dependency on a personal repository can disappear, which is exactly what happened to the
  original. Vendoring (`vendor/parse-wiki-text-2/`, with its LICENSE and a `VENDORED.md` naming the upstream
  commit) also lets us fix the exponential worst case ourselves if the rate of pages over budget calls for it (see D8).
- **Why an adapter.** All parsing goes through one internal module that turns the crate's `Node` tree into our
  own component model. If `wikrs` matures, or we write our own tokenizer for the worst cases, only that module
  changes.

### D3. One site configuration per site

Each text column is parsed with the configuration of the site it comes from: `config/sites/ru.wikipedia.json`
or `config/sites/ruwiki.ru.json`. The site is named on the command line for each column (see Command line)
and is never inferred from the column name, because in `timeseries_diff` both columns are Wikipedia. Each
configuration holds what parsing and classification need from the site's
`siteinfo`: namespaces and their aliases (all of them, not only File and Category, so links can be
classified), the interwiki map (with the language flag that separates interlanguage links from interwiki
links), extension tags, magic words (behaviour switches, redirect, and the `img_*` option aliases such as
`мини`, `справа`, `NNNпкс` that separate image options from captions), URL protocols and the link trail.
Each file records where and when it was fetched. The manifest records each file's hash.

Russian Wikipedia's file comes from its API. Ruwiki refused the request (403), so its file starts as a copy of
Russian Wikipedia's (Ruwiki forked from it on 2023-05-01) and is then checked against the data. The builder
scans the clone-side wikitext of a large sample for `[[Prefix:` prefixes and `<tag` names that the copy does
not know, and adds what Ruwiki actually uses, listing every addition in the file. If the API becomes reachable,
the real siteinfo replaces this derived version.

The same code and the same template registry (D4) run on both sides. Only these two files differ.

Nothing about Russian is hard-coded. A committed script (`tools/fetch_siteinfo`) writes the configuration
file for any MediaWiki site from its API. Registry entries are keyed by template name, with an optional
`site` field for the rare entry that applies to one site only. So parsing another wiki means adding its
configuration file and, for useful roles and render rules, its registry entries. Without registry entries it
still parses; its templates just have role `other`.

### D4. Templates: record every one, expand none, render a declared few (open question 2)

**Answer: templates are never expanded. Each one is recorded as a component with its parameters and
position, and only templates listed in a versioned registry contribute text to the prose, through
declared rules.**

Why not expand:

- Expansion needs MediaWiki: parser functions, Lua modules, transclusion of the template source at the right
  revision. For the Wikipedia side that would mean the template versions of 2023 and 2025. For Ruwiki it would
  mean templates we cannot fetch, and that Ruwiki has edited: `{{объект в Крыму}}` renders differently on the
  two sites by design.
- Parsoid HTML (Wikimedia Enterprise dumps) exists for Wikipedia only. One expanded side and one unexpanded
  side would make every difference suspect.
- At millions of pages, a call to an expansion API per template is not an option.

What we do instead:

- **Structure.** Every template invocation becomes a `templates` row. Its name is normalised: namespace
  prefix (`Шаблон:`, `Template:`, `Ш:`, `T:`) stripped, underscores to spaces, whitespace collapsed, first
  letter case-insensitive. Each row has positional and named parameters (raw and as plain text, with spans),
  its parent component, its nesting depth and its position. Parser functions (`{{#if:...}}`,
  `{{#invoke:...}}`) and magic variables (`{{PAGENAME}}`) are marked by `kind`.
- **Roles from a registry.** `config/registry/templates.toml` gives a template (and its redirect aliases) a
  role: `infobox`, `citation`, `footnote` (`{{sfn}}`, `{{efn}}`, `{{комментарий}}`), `ref_list`
  (`{{примечания}}`, `{{reflist}}`), `inline_text`, `interlanguage` (`{{нп3}}`, `{{iw}}`,
  `{{не переведено}}`), `navigation`, `maintenance`. An unknown template has role `other`. Infoboxes also get a
  `looks_like_infobox` flag from a structural heuristic (top of the lead, mostly named parameters), so that an
  infobox missing from the registry is still found and the registry can be completed.
- **Rendering.** An `inline_text` entry declares how the template reads in prose. The rules are small and
  declarative: `drop`, `param(2|1)` (the second positional parameter, otherwise the first), `literal`, and a
  few built-ins such as the date rule that the legacy `{{СС3}}` code implements. A template without a rule
  contributes no text but keeps its anchor in the paragraph (D6). Text that came from a template is
  identifiable (`rendered_len` on the template row), so an analysis can exclude it. A template that renders
  differently by design on the two sites is marked `site_dependent` and is never rendered.
- **Coverage, measured.** `parse_components --report-templates` lists templates by frequency, per side and
  per context (inside a ref, inside a paragraph, at the top of the lead), registered or not. The registry is
  complete enough when, on a 10,000-page sample per side, at least 99% of template occurrences inside refs
  have a role, and the 200 most frequent templates inside paragraphs have either a render rule or an explicit
  `drop`. These thresholds are my call. The point is that coverage is a number in a report, not a feeling.

### D5. Output: one long table per component type

Output is a directory with one Parquet dataset per component type, and one part file per input file:

```
<out>/
  manifest.json
  pages/<input_stem>.parquet
  timings/
  sections/  paragraphs/  refs/  ref_lists/  citations/  templates/
  links/  categories/  files/  tables/  table_cells/  lists/
```

I chose long tables over one row per page with nested lists. "Count citations by domain and side" and "join
the refs of the two versions of a page" are one-liners in R, pandas and Athena on long tables, and awkward on
nested ones. Each part file is written under a temporary name and renamed when complete, so a crashed run
never leaves a half-written file that looks finished.

Every row of every component table starts with the same keys:

| Column | Type | Meaning |
|---|---|---|
| `source_file` | string (dict) | input file stem; one value per part file, so it costs almost nothing |
| `row` | int32 | row index in the input file; with `source_file`, the key that survives duplicate `page_id`s |
| `page_id` | string | as in the input |
| `site` | string (dict) | `wikipedia` or `ruwiki`: whose markup this is, and which site configuration parsed it |
| `version` | string (dict) | the run's label for the text, e.g. `2025-01-01` or `forkday` |
| `column` | string (dict) | the input column the text came from (`official_text`, `clone_text`, `text`, ...) |
| `id` | int32 | index of the component within its table and page version, in document order |
| `start`, `end` | int32 | UTF-8 byte offsets into that version's wikitext, 0-based, end exclusive. Document order across tables is `start`, with an enclosing component before what it contains |
| `start_char`, `end_char` | int32 | the same span in Unicode code points, for slicing in R and Python (`substr(x, start_char + 1, end_char)`) |
| `section_id` | int32 | the section the component sits in (0 is the lead) |
| `para_id` | int32, null | the paragraph whose text contains the component, if any |
| `para_offset` | int32, null | code-point offset into that paragraph's `text` where the component sits |
| `parent_type`, `parent_id` | string, int32, null | the enclosing component (a ref inside an infobox parameter, a template inside a ref) |
| `fallback` | bool | row produced by the fallback scanner (D8), not the parser |
| `fp` | string | content fingerprint: 16 hex characters of a stable 64-bit hash (xxh3, fixed seed) of the normalised content. Hex, because R reads 64-bit integers badly |

Every file carries `wikitext_parser.schema_version` in its Parquet key-value metadata.

### D6. Positions, prose and alignment

- **Source spans are the shared coordinate.** Everything is located in the wikitext of its side by byte and
  code-point span. Containment of spans answers "which section, which paragraph, which template".
- **Paragraph text with anchors.** The structured path builds its own paragraph text while walking the tree,
  and keeps an offset map from source to text. Text includes plain text and character entities (decoded),
  bold and italic content, link labels (or the target title, plus the link trail), labels of external links
  (bare URLs contribute nothing), `nowiki` content, and templates that have a render rule. It excludes refs,
  templates without a rule, files, categories, comments and tables (tables have their own text, in
  `table_cells`). Everything excluded that sits inside a paragraph keeps a `para_offset`. That is how a ref is
  tied to the sentence it supports: the sentence of paragraph 7 that ends at or before the ref's offset.
- **Keys for comparing versions.** Sections carry `path` (titles from the top, normalised) and paragraphs
  carry `fp`. Citations carry normalised URLs and `fp`, and templates carry name and parameter fingerprints.
  Joining the official and clone tables on `page_id` plus these keys is the comparison. The parser does no
  matching itself (see Non-goals).

### D7. References and citations, the core of the first deliverable

- **`refs`**: one row per `<ref>` and per footnote template, with `kind`:
  - `inline`: a ref with a body and no name;
  - `named`: a ref with a body and a name (a definition);
  - `reuse`: a self-closing named ref;
  - `list_defined`: a ref defined inside `<references>...</references>` or in a ref-list template parameter
    (`{{примечания|refs=...}}`);
  - `template`: a footnote template (`{{sfn}}`, `{{efn}}`, `{{комментарий}}`).

  Columns: `name`, `group` (read from the tag's source slice, since the crate exposes no attributes),
  `body_raw`, `body_text`,
  `def_ref_id` (a reuse points to its definition, resolved by group and name within the page), `n_uses` (on
  definitions), `resolved` (false for a reuse whose definition is missing). Footnote templates also carry
  `harv_key` and `cites_citation_id`, described below.
- **`ref_lists`**: each `<references>` tag or ref-list template, with its `group` and position.
- **`citations`**: one row per source as written in the markup. A citation row comes from the body of an
  `inline`, `named` or `list_defined` ref, from the body of a footnote template that holds a citation (an
  `{{efn}}` can), or from a citation template in a bibliography section. A `reuse` ref writes no source, so it
  yields no citation row: it points to its definition through `def_ref_id`. An `{{sfn}}` yields none either:
  it points to its bibliography entry through `cites_citation_id`. Counting by source means counting
  `citations`. Counting by use means joining `refs` to `citations` through those two links. Both are one join,
  and neither inflates the other. Each row has a `context`: `in_ref`, `bibliography` (a citation
  template in a section whose role is bibliography or external links, such as `Литература`, `Источники` or
  `Ссылки`, outside any ref) or `other`. `ref_id` links back to the ref. A ref can hold several citations
  (`{{cite web}}; {{cite news}}`). Each row has a `family`:
  - from a citation template (`web`, `news`, `book`, `journal`, `encyclopedia`, `other_template`):
    `template_id` and `template_name`, all parameters as `params` (list of name, raw value, plain value), and
    canonical fields mapped from Russian and English parameter names by the registry (`url`, `archive_url`,
    `title`, `authors` (list), `date_raw`, `year`, `publisher`, `work` (website, newspaper, journal or
    издание), `language`, `isbn`, `doi`, `access_date_raw`, `harv_key`);
  - `bare_link`: a ref with an external link and no citation template (`url`, `title` from the label);
  - `plain_text`: a ref with neither (`text` only).

  Every URL also yields `host` (lowercased, `www.` stripped) and `registrable_domain` (public-suffix list).
  That is the field R2.M2b needs (`kremlin.ru`, `tass.ru`, `ria.ru`). Every ref with a body yields at least
  one citation row, so "refs without an identifiable source" is countable rather than missing. An unresolved
  `{{sfn}}` shows up as `resolved = false` on the ref, not as a `plain_text` citation.
- **Short footnotes (`{{sfn}}`).** These are common on Russian Wikipedia. `{{sfn|Иванов|2005|с=12}}` points to
  a `{{книга|...|ref=Иванов}}` or `{{cite book}}` in the `Литература` section. The parser builds MediaWiki's
  `CITEREF` key (author surnames, up to four, plus year, or the explicit `ref=` value) on both ends and
  resolves within the page. The result is `cites_citation_id`, or `resolved = false`. The exact key rules are
  the registry's, and the builder verifies them on corpus pages against the rendered page.
- **Section roles** come from `config/registry/sections.toml` (`Примечания`, `Комментарии`: notes;
  `Литература`, `Источники`: bibliography; `Ссылки`: external links; `См. также`: see also).

### D8. Robustness

**Work is limited by counted steps, not by the clock.** The fork's timeout checks the wall clock every
10,000 iterations of its main loop (`parse.rs`). A page near the limit would then come out `ok` on an idle
machine and `fallback` on a loaded one, with different rows. So the vendored copy is patched to count
iterations against a budget instead: `ParseError::BudgetExceeded`, with the patch recorded in
`VENDORED.md`. Our walk counts nodes the same way. `--budget` takes the parser's budget in iterations. Its
default is calibrated once on the 10,000-page sample to about the work the parser does in 30 seconds on this
machine, which matches today's timeout. The calibrated value is then fixed in the code, not measured again at
run time. The clock is kept only for the watchdog (step 6).

Per page, in order:

1. Parse within the step budget. Exceeding it gives `status = budget`.
2. Walk the tree within its node budget and with a depth cap (`status = depth_limit`). The walk must not
   overflow the stack on the deepest corpus page. A stack overflow aborts the process and cannot be caught,
   so the walk is iterative, or the worker threads get a large stack. Builder's choice, but the corpus must
   prove it.
3. Everything runs inside `catch_unwind`. A panic gives `status = panic` and the message.
4. Parser warnings (the crate's `Warning` list, with spans) go into `pages.warnings`: counts by kind, plus
   the first 100 with spans.
5. **Fallback for pages the parser could not finish.** A linear-time lexical scanner reads the raw wikitext
   and recovers headings, `<ref>` tags (with name and group), ref lists, templates by brace matching (bounded
   depth), wiki links, external links and categories. Citation templates are parsed by splitting parameters
   at depth 0. It emits no paragraphs, tables or lists. Its rows have `fallback = true`, and the page gets
   `status = fallback` with the original failure in `status_detail`. This is my call, and the reason is
   selection: the pages that time out are long and heavily sourced, so dropping them would bias the sourcing
   analysis.
6. **Watchdog.** A supervisor thread watches the workers on the wall clock. If one page takes more than ten
   minutes, the budgets have failed (a bug, since budgets bound all the work). The page's `source_file`, `row`
   and column are appended to `<out>/quarantine.list` and the process aborts. `--resume` skips completed part
   files and gives quarantined pages `status = hang` plus fallback rows. `hang` is the one status that can
   depend on the machine, and it should never occur.

Every input row yields exactly one `pages` row per text column. A null text gives `status = null_input`, an empty one
`status = empty`, a redirect `status = redirect` with `redirect_target`.

On the 10,000-page sample, the builder measures the rate of pages over budget and the distribution of steps
and times. If more than 0.05% of pages exceed the budget, the next step is to look at the vendored parser's
worst case (fork issue #1) before the full run, because a bounded fix there beats both the budget and the
fallback. That threshold is my call.

### D9. Efficiency

- One process handles one input file or a whole directory, so the per-file `cargo run` in the scripts goes
  away.
- Batches are streamed from the reader (a few hundred rows at a time). Pages are parsed in parallel with
  rayon (`--jobs`, default all cores), and rows are written in input order, so output is deterministic. Memory
  is bounded by the batches in flight, not by the file.
- Regexes are compiled once. There is no thread per article.
- Parquet output uses zstd and dictionary encoding for repeated strings (site, version, template names, kinds).
- Target: each input family (the cross-section, each time series), both columns, in one night (8 hours or
  less) on this 12-core machine, pages over budget included. The manifest reports pages per second and the time spent on pages over budget,
  so the split is visible. The 10,000-page sample measures this before the full run.

### D10. Stability

- **Schema version.** `SCHEMA.md` documents every table and column, including the normalisation behind each
  `fp`. `schema_version` starts at `1.0.0`. Adding a column is a minor change. Renaming, removing or
  changing meaning is a major one. A test serialises the Arrow schema of every table to
  `tests/schema/v1.json` and fails on any difference, so a schema change cannot slip in without a version
  bump. `1.0.0` is frozen at the first release to `rev/sources` (end of milestone 2). Tables that arrive
  later are minor additions, and the tables released then keep their ids, because ids count document order
  within one table and no later milestone adds rows to a released table.
- **Manifest.** `manifest.json` records the schema version, the crate version and git commit, the vendored
  parser's upstream commit, the hashes of the site configurations and registries, the arguments, each input
  file with its row count, each output file with its row count, status counts per site and version, and throughput.
- **Fixed test corpus.** About 50 pages, stored as plain files so diffs are reviewable:
  `tests/corpus/<page_id>/<site>_<version>.wiki` plus `meta.json` (titles, timestamps, the input file and row
  each version came from, and why the page was chosen). A page carries all three versions where they exist:
  `wikipedia_forkday`, `wikipedia_2025-01-01` and `ruwiki_2025-01-01`. A few deleted and added pages carry
  one. Use `.wiki`, not `.txt`, which is gitignored. The pages are CC BY-SA, so add
  `tests/corpus/ATTRIBUTION.md`. A committed selection script pulls them from the cross-section and fork-day
  input files. The pages cover, at least:
  - a stub;
  - a long featured article;
  - politically salient pages from the paper (Crimea, the war, a page Ruwiki rewrote heavily);
  - `{{sfn}}`-heavy and `<ref name>`-heavy pages, and list-defined refs;
  - grouped refs (`group=`);
  - a large infobox;
  - big tables, nested tables, galleries;
  - interlanguage-link templates;
  - a redirect, a disambiguation page, list-heavy pages;
  - pages that timed out under v0.1.0;
  - malformed markup (unclosed templates, tables and tags);
  - the deepest-nested page in the sample;
  - page 127787, which already has wikitext and parsed output in the replication package
    (`data/edits/diff_example_fallback/`).
- **Legacy regression.** For each corpus page that v0.1.0 can parse, the expected output of the legacy
  binaries, both phase 1 (`wikitext_parser_rust`) and phase 2 (`clean_parsed`), plus `parse_single`, is
  generated once by building tag `v0.1.0` in a separate worktree. A committed script does this. The test
  asserts byte equality with `--timeout 0`. Pathological pages are excluded, since the legacy placeholder only
  tests the timer.
- **Structured expected output.** Every corpus page has a snapshot of its structured output as stable JSON
  (`<page_id>/structured.snap.json`), reviewed on every change. The snapshots catch drift. Evidence that the
  parse is right comes from two kinds of check:
  - **Wikipedia side, against the rendered page.** For about 12 pages, the builder resolves the revision from
    the stored timestamp through the ru.wikipedia API (`prop=revisions`, `rvstart`, `rvdir=older`), fetches
    its rendered HTML (`action=parse&oldid=`), and records facts in `facts.toml`: footnotes in the notes
    list, reused refs, sfn targets resolved, infobox field count, section titles, external-link hosts. Where
    the rendered page includes what templates add (categories, template-made refs), the fact is a lower
    bound and says so. The builder does this, not a person. Your own spot check is welcome, but the plan does
    not wait for it.
  - **Ruwiki side, by symmetry.** Ruwiki refused our requests, and the inputs carry timestamps, not
    revision ids, so its pages cannot be rendered from here. Instead, two tests guard the anti-goal of
    artefacts that look like edits. First, where two versions are byte-identical, every component table must
    be identical apart from the labels. The fork-day text makes this a cross-site test with real data: a page
    Ruwiki never edited still has the markup of the fork-day Wikipedia version, so the Ruwiki configuration
    and the Wikipedia configuration must parse it the same way. A difference there is either a real
    configuration difference, which goes in the site file's notes, or a bug. Second, a locality test: between the two versions of a
    page, every component that differs must overlap a region the wikitext diff marks as changed. A component
    that differs where the markup did not change is a parser artefact, such as a site-configuration
    mismatch or a template recognised on one side only. This runs on the corpus as a test, and on the
    10,000-page sample as a report the builder reads before the full run. If Ruwiki becomes reachable, the
    clone side gets its own `facts.toml` entries.
- **Robustness test.** Every corpus page truncated at every 997th character boundary, and with its
  closing braces removed, parses without a panic and within the budget, and yields a status.
- **Lockfile.** Remove `Cargo.lock` from `.gitignore` and commit it. This is a binary behind a replication
  package, and without a lockfile its dependency versions float.

## Output schema v1.0, beyond the common keys

| Table | Columns (besides the common keys) |
|---|---|
| `pages` | `title`, `timestamp` (input type), `status` (`ok`, `budget`, `depth_limit`, `panic`, `fallback`, `hang`, `redirect`, `null_input`, `empty`), `status_detail`, `parse_steps`, `n_bytes`, `n_chars`, `n_warnings`, `warnings` (list of kind, start, end), `redirect_target`, one count column per component table. `pages` has no `id`, `para_*`, `parent_*` or `fp`. |
| `timings` | `parse_ms`, `walk_ms` per page version, keyed like `pages`. The one table that differs between runs, and it is excluded from every reproducibility check. |
| `sections` | `level` (0 for the lead), `title_text`, `title_raw`, `title_norm`, `path` (list of normalised titles), `parent_section_id`, `heading_start`, `heading_end`, `subtree_end`, `role` (from the section registry, else `content`) |
| `paragraphs` | `kind` (`prose`, `list_item`, `definition_term`, `definition`, `preformatted`), `list_id`, `item_depth`, `text`, `n_chars` |
| `refs` | see D7 |
| `ref_lists` | `group`, `kind` (`tag`, `template`), `template_id`, `n_entries` |
| `citations` | see D7 |
| `templates` | `name_raw`, `name`, `kind` (`template`, `parser_function`, `module`, `variable`), `role`, `looks_like_infobox`, `depth`, `rendered_len`, `params` (list of name, positional, raw value, plain value, start, end) |
| `links` | `kind` (`internal`, `interwiki`, `interlanguage`, `external`), `target_raw`, `ns`, `title`, `fragment`, `prefix`, `label_text`, `trail`, `url`, `host`, `registrable_domain`, `para_len` |
| `categories` | `name`, `sort_key` |
| `files` | `kind` (`link`, `gallery`), `file_name`, `options` (list), `caption_text`, `caption_raw` |
| `tables` | `attributes_raw`, `caption_text`, `n_rows`, `n_cols_max`, `depth` |
| `table_cells` | `table_id`, `row`, `col`, `kind` (`header`, `data`), `rowspan`, `colspan`, `text` |
| `lists` | `kind` (`unordered`, `ordered`, `definition`), `depth`, `n_items` |

## Command line

```
parse_components --input <file-or-dir> --out <dir>
    --label <column>=<site>:<version>   # one per text column, required, never inferred
                                        #   crossection_diff: --label official_text=wikipedia:2025-01-01
                                        #                     --label clone_text=ruwiki:2025-01-01
                                        #   timeseries_diff:  --label official_text=wikipedia:forkday ...
    [--layout pair|single]          # auto-detected from the columns, as parse_single does today
    [--text-column <name>]          # single-column input, as parse_single
    [--format parquet|json]         # json: one document per page version, for a .wiki input and the snapshots
    [--jobs N] [--budget STEPS] [--no-fallback] [--resume]

parse_components --input article.wiki --label wikipedia:adhoc --format json --out article.json
    [--tables refs,citations,...]   # write a subset; pages is always written
    [--report-templates <file>]
```

Column detection for single-column input moves from `parse_single.rs` into the new library code by copying,
not moving, so the legacy file stays untouched.

## Build order

Each milestone ends green on the tests that exist by then.

0. **Lock the baseline.** Commit the lockfile, the corpus with its selection script, the legacy expected
   outputs generated from `v0.1.0`, and the legacy regression test. Nothing else changes.
1. **Frontend and driver.** Vendor the fork and patch in the step budget. Build the site configurations
   (with the Ruwiki scan). Then the adapter, the streaming parallel driver, the `pages` and `timings` tables,
   `catch_unwind`, the watchdog, the manifest and the schema test. On the 10,000-page sample: calibrate the
   budget, and measure throughput and the rate of pages over budget.
2. **Sources first, complete.** Sections, paragraphs with anchors, the full `templates` table (every
   template with its parameters; roles only for citation, footnote and ref-list templates so far), refs, ref
   lists and citations: the citation part of the registry, sfn resolution, hosts and domains. Also the ref
   and citation part of the fallback scanner, so pages over budget are not missing from the sourcing data.
   The Wikipedia-side facts and the symmetry and locality tests for refs and citations. This is the release
   to `rev/sources`, and schema `1.0.0` is frozen here.
3. **The rest of the templates, and links.** Roles for all templates and the infobox heuristic, the inline
   render rules, `--report-templates`, links, categories and files. Complete the registry to the coverage
   thresholds of D4.
4. **Tables and lists.**
5. **The rest of the fallback scanner** (headings, templates, links and categories) and the truncation
   robustness test.
6. **Full runs** over every input family. Write `SCHEMA.md`, update the README and CLAUDE.md, and tag
   `v0.2.0`.
7. **Optional: a bridge to the paper's text.** For each v0.1.0 paragraph, the structured paragraphs it
   corresponds to, by deterministic normalised-text matching, as a `legacy_map` table. This would let the
   sources workstream tie a ref to the paper's paragraphs and to the pair layer's hunks. It is my call to
   include it and the easiest thing to drop.

## Left to the builder

- Module layout inside the new code, and the shape of the adapter.
- Prose edge cases (`<br>`, `<math>`, `<poem>`, `<sup>`), within the principles in D6, documented in
  `SCHEMA.md`.
- Batch size, row-group size, compression level.
- The exact normalisation behind each `fp` and `title_norm`, documented in `SCHEMA.md`.
- Crates for the public-suffix list, hashing (xxh3 suggested) and snapshot testing.
- Iterative walk or recursion with a large stack.
- The individual corpus pages, within the categories in D10.

## Decided by you

- The goals, constraints and the two open questions as set out for this session (the library, and how to
  handle templates).
- "The parser needs to be applicable to any wikitext article, including the fork-day text." (Goal 1, D3,
  D10, the full runs in milestone 6.)

## Calls I made, for you to check

1. Legacy path frozen on `parse_wiki_text` 0.1.5. The structured path moves to the fork, vendored, not
   fetched.
2. Templates are never expanded. A registry declares roles and the few render rules, and coverage is
   measured.
3. Long tables, one per component type, instead of nested rows per page.
4. The parser emits keys for comparing versions but does no matching.
5. A fallback scanner for pages the parser cannot finish. Its rows are flagged, not dropped.
6. Ruwiki's site configuration is derived from Russian Wikipedia's plus a scan of the data, because Ruwiki's
   API refused us.
7. Test corpus of about 50 page pairs as plain files in git, CC BY-SA attributed.
8. Commit `Cargo.lock`.
9. Both byte and code-point offsets. Fingerprints as hex strings.
10. The thresholds: 99% in-ref template coverage, the top 200 paragraph templates, 0.05% of pages over budget
    before looking at the parser's worst case, one night for the full run.
11. The bridge to the paper's text is optional and last.
12. A counted step budget instead of a timeout, so a page's status does not depend on the machine. Timings
    live in their own table.
13. Each text column gets an explicit `site:version` label on the command line, with no default, so the
    Ruwiki configuration can never be applied to Wikipedia text by accident.
14. The citation unit is the source as written: reuses and `{{sfn}}` link to it rather than repeating it.
15. Evidence of correctness comes from rendered pages on the Wikipedia side, checked by the builder, and
    from symmetry and locality tests on the Ruwiki side. The release to `rev/sources` (milestone 2) is
    complete for refs and citations, fallback included.
16. I read "any wikitext article" as: any version, any input family, any single `.wiki` file, and any
    MediaWiki site given its configuration file. Only Russian Wikipedia and Ruwiki ship with configurations
    and registry entries.

## Sources

- `parse-wiki-text-2`: https://github.com/soerenmeier/parse-wiki-text-2 (issues #1, #8, #9, #10), crates.io
  0.2.0.
- `parse_wiki_text` 0.1.5: https://docs.rs/parse_wiki_text (upstream repository no longer exists).
- `wikrs` design document: https://docs.rs/crate/wikrs/0.4.0/source/docs/DESIGN.md
- Russian Wikipedia siteinfo: `https://ru.wikipedia.org/w/api.php?action=query&meta=siteinfo` (fetched
  2026-10-27).
- `ruwiki_replication_package_sources`: `CLAUDE.md`, `docs/revision/pair_llm_layer_design.md` (line 25),
  `code/data_prep/07_pair_llm_layer/08c_parser_residue.py`.
