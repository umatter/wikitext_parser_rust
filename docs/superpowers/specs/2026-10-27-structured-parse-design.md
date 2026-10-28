# Structured parse of a page: design

Date: 2026-10-27. Branch: `feature/components`. Status: design, not yet planned.

## Why

The parser turns a page into prose and throws everything else away. The target is a general tool: a full
structured parse of any wikitext article.

- the section tree and the paragraphs;
- references: inline `<ref>`, named and reused refs, citation templates with their fields, and the reference
  list;
- templates with their parameters, infoboxes included;
- links (internal, external, interwiki), categories, files, tables and lists.

Every component keeps its position in the page, so that it can be tied to the prose around it and compared
across versions of the same page.

Its first user is the references-and-sources workstream of the replication package (`rev/sources`). The pair
layer's design note puts it this way: "A real sourcing analysis would need the parser to keep references".
That workstream runs the parser on its inputs and does the analysis. The parser is not shaped by its
questions (`docs/revision/sources_design.md` in that package says the same).

This is parser development only. No data is collected, and no build, run or test contacts a live site. The
parser is developed and tested on markup that is already stored.

## Goals

1. **Complete and general.** Every component type listed above, from any wikitext article in any version:
   - both columns of every pair input (the 2025 cross-section and the Wikipedia and Ruwiki time series, so
     the fork-day text too);
   - single-column inputs (deleted and added pages);
   - the long format of the test corpus;
   - a plain `.wiki` file.

   The code knows no particular wiki. What it knows about one lives in that site's settings file and in the
   registry, so another wiki needs data, not code (D3).
2. **Aligned**: every component carries its source span, its section, and, when it sits inside prose, the
   paragraph and the character offset in that paragraph's text. Components carry content fingerprints and
   section paths, so versions of a page can be joined.
3. **Robust**: malformed markup, deep nesting and pathological pages never stop a run. Every input page
   yields a row that says what happened to it. A page that fails is recorded with the reason, never skipped
   silently.
4. **Efficient**: streams Parquet in and out, runs in parallel inside one process, and is fast enough for the
   full stored corpus on one machine.
5. **Stable**: a versioned output schema; a fixed test corpus of real pages with expected outputs; the
   current prose output unchanged, with v0.1.0 as the regression baseline.

## Non-goals

- **Expanding templates.** No Lua, no parser functions, no transclusion. See decision D4.
- **Matching components across versions.** The parser emits the keys a match needs (fingerprints, section
  paths, positions). The matching rules (when is a citation "the same, but changed"?) are analysis decisions,
  for whoever analyses the output.
- **Classifying sources.** Whether a domain is a government site or state media is decided downstream. The
  parser extracts the URL, host and registrable domain.
- **Running the parser over the full stored corpus.** That run belongs to the replication package, which
  holds the inputs (24 GB compressed on S3). The parser repository delivers the binary and measures its
  speed on a sample (D9).
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
  no editor made. For references, the risk is a citation template that is recognised in one version and not
  in the other (a Ruwiki rename, a redirect alias), so that a citation seems to be removed. Every site-specific
  rule must be explicit, and coverage is checked on both sites.
- **Silent loss.** A page, a ref or a citation that disappears without a status. This matters most for the
  pages the parser cannot finish. They are the longest and most heavily sourced articles, so losing them
  would bias any analysis of references.
- **Non-reproducible output.** Same input, same build, same bytes, on any machine and under any load. A page's
  status must not depend on how fast the machine is (D8). No wall-clock values go in the component tables or
  in `pages`. Timings go to the manifest and to a separate `timings` table, which is excluded from every
  reproducibility check.

## Constraints

- Rust, in this repository, on this branch. The repository is public. Wikipedia markup may be committed (CC
  BY-SA, attributed). Ruwiki markup, and anything that reproduces it, never enters the repository: expected
  outputs, snapshots and hand-checked facts of Ruwiki versions all stay in the private replication package
  (D10).
- The legacy source files (`src/parser.rs`, `src/main.rs`, `src/clean_parsed.rs`, `src/parse_single.rs`,
  `src/export_parsed.rs`) and the shell scripts keep their behaviour. Prefer not to touch them at all. The
  legacy path keeps `parse_wiki_text = "0.1"` (0.1.5).
- Offline: building, testing and running need no network, apart from fetching crates.
- Input formats:
  - **Pair**: the fetches' own format (`page_id`, `page_title`, `official_text`, `official_timestamp`,
    `clone_page_title`, `clone_text`, `clone_timestamp`). Three file families share it
    (`01_fetch_wikitext_data.R`): `crossection_diff` (Wikipedia 2025 against Ruwiki 2025), `timeseries_diff`
    (Wikipedia on the fork day against Wikipedia later, so both columns are Wikipedia) and
    `ruwiki_timeseries_diff`. So a column name does not say which site a text comes from.
  - **Single**: the single-column layouts that `parse_single` auto-detects.
  - **Long**: the test corpus (`page_id`, `page_title`, `version`, `timestamp`, `wikitext`, ...), one row per
    page version.

  A file can hold several rows for the same `page_id` (`08b_forkday_wikitext.py` deduplicates them by
  timestamp).
- Consumers read the output in R (arrow), Python (pyarrow, pandas) and Athena (the replication package
  exports a Glue catalog). Column types must survive all three: no unsigned 64-bit integers, no Arrow map
  type.
- `.gitignore` excludes `*.parquet`, `*.txt` and `Cargo.lock`. Fixtures and the lockfile must not fall under
  those rules (D10).
- The machine has 12 cores and 62 GB of RAM, and about 79 GB of free disk. Never download a whole fetch. A
  performance sample is one Parquet row group (about 14,000 pages, about 0.5 GB), and the user is told the
  download size before it starts.

## What exists now

Findings from reading the code, the libraries and the stored markup, which shape the decisions below.

- **The parser runs on the crate's English default configuration** (`Configuration::default()`). It knows
  `File:`, `Image:` and `Category:`, but not `Файл:`, `Изображение:` or `Категория:` (or `К:`). So
  `[[Файл:...]]` and `[[Категория:...]]` are parsed as ordinary links. That is why `remove_image_fragments`
  and the `Категория:` checks in `remove_empty_sections` exist, and why captions of `[[Файл:]]` links survive
  while those of `[[File:]]` links do not, as `08c_parser_residue.py` observes. The link trail is Latin
  letters only, where Russian Wikipedia's is `/^([a-zабвгдеёжзийклмнопрстуфхцчшщъыьэюя]+)(.*)$/sDu`.
- **The original crate is gone upstream.** `parse_wiki_text` 0.1.5 was released on 2019-02-12, and its
  repository (`github.com/portstrom/parse_wiki_text`) now returns 404. Only the crates.io copy is left.
- **A maintained fork exists.** `parse-wiki-text-2` (`github.com/soerenmeier/parse-wiki-text-2`) has the same
  AST: the same `Node` variants, with byte `start`/`end` on every node.
  - It adds a cooperative timeout: `Configuration::parse_with_timeout` returns a `ParseError` instead of
    hanging.
  - Master (May 2026, not released; the last release is 0.2.0 from 2024-03-03) fixes the bug where text
    before `<nowiki>` and `<math>` was dropped (issue #10, one of the residue rules in `08c`), fixes a
    timeout panic on non-UTF-8 boundaries (#8), and adds `onlyinclude`/`includeonly`/`noinclude`.
  - Issue #1 is still open: "Parsing has worst case exponential runtime". This is the likely cause of today's
    timeouts: 131 articles timed out at 30 s in the pair layer's fork-day parse.
  - It uses edition 2024 and MSRV 1.90 (the local toolchain is 1.99.1), under the same permissive license as
    the original.
- **`Tag` nodes carry no attributes.** `<ref name="a" group="b">` arrives as `Tag { name: "ref", nodes, start,
  end }`. Attributes have to be read from the source slice. The contents of `ref` are parsed into nodes, so
  templates inside refs arrive as `Template` nodes. The contents of `nowiki` and `math` are raw.
- **The timeout leaks threads.** `parse_wikitext_with_timeout` spawns a thread per article and abandons it on
  timeout, and the thread keeps running, because Rust threads cannot be killed. Pages are processed one at a
  time, and `main` loads the whole input file into memory first. Regexes are compiled on every call.
  Parallelism comes from the shell scripts, one `cargo run` per file.
- **No tests.** There is no test directory and no expected output.
- **A newer crate, `wikrs`, is too young.** Version 0.4.0 (July 2026, MIT/Apache) claims linear-time parsing
  with diagnostics that carry spans. But it is two months old, has one maintainer, its design document calls
  the repository private, and it documents neither spans on AST nodes nor namespace settings for other
  languages. Worth watching, but too early to build on.
- **A test corpus already exists** (the replication package's S1a), drawn from the stored markup: 105 pages in
  255 versions. A rough regex count over it finds 6,868 `<ref>`s:
  - 90% contain a URL and 91% a citation template. `cite web` is 67%, followed by `wayback`, `cite news`,
    `книга`, `статья`, `webarchive`, `недоступная ссылка` and `публикация`.
  - `{{sfn}}` occurs.
  - Archive-wrapped URLs are common.

## Decisions

### D1. Two paths: legacy frozen, structured new

The legacy prose path stays exactly as it is, on `parse_wiki_text` 0.1.5 with the default configuration and
its quirks, because those quirks are part of v0.1.0's output. The structured parse is a new binary,
`parse_components`, with its own frontend and settings. The two share no parsing code.

The structured output has its own paragraph text (D6). It is cleaner text: Russian namespaces are recognised,
so file captions and category links stay out, and Cyrillic link trails stay in. But it is not the paper's
text, and the documentation says so.

This turns the first open question into a narrower one: the old crate has to stay for the legacy path no
matter what. The choice is only about the structured path.

### D2. Frontend: vendored `parse-wiki-text-2` behind an adapter (open question 1)

**Answer: move the structured path to `parse-wiki-text-2`, vendored at a recorded upstream commit, and keep
the rest of the code off its types.**

- **Why the fork.** It has the same AST and the same byte spans, so nothing is lost relative to today. Its
  cooperative timeout ends the leaked-thread problem (we patch it into a counted step budget, D8), and master
  fixes the `<nowiki>` text drop that the pair layer had to work around. A rewrite or a two-month-old crate
  would put the parser on unproven ground.
- **Why vendored, not a crates.io or git dependency.** The fixes are unreleased, so 0.2.0 from crates.io lacks
  them. A git dependency on a personal repository can disappear, which is exactly what happened to the
  original. Vendoring (`vendor/parse-wiki-text-2/`, with its LICENSE and a `VENDORED.md` naming the upstream
  commit and our patches) also lets us patch it, for the step budget and, if needed, the exponential worst
  case (D8).
- **Why an adapter.** All parsing goes through one internal module that turns the crate's `Node` tree into our
  own component model. If `wikrs` matures, or we write our own tokenizer for the worst cases, only that module
  changes.

### D3. Site settings: a static file per site

The parser needs a few settings of the wiki software to read Russian markup correctly:
- namespace names and aliases (`Файл`, `Изображение`, `File`, `Image`; `Категория`, `К`; `Шаблон`, `Ш`;
  `Модуль`; all of them, so links can be classified);
- interwiki and language prefixes (to tell interlanguage links from interwiki links);
- extension tags (`ref`, `references`, `nowiki`, `gallery`, `templatestyles`, ...);
- magic words: behaviour switches, the redirect word (`#перенаправление`), and the image options that
  separate options from captions (`мини`, `справа`, `200пкс`);
- URL protocols, and the link trail.

These live in a static, committed file per site: `config/sites/ru.wikipedia.toml` and
`config/sites/ruwiki.toml`. They are written once, from MediaWiki's Russian language file
(`languages/messages/MessagesRu.php` in the MediaWiki source) and Russian Wikipedia's published settings.
Ruwiki is a fork of Russian Wikipedia, so its file starts as a copy. A private test (D10) scans every Ruwiki
version in the corpus for `[[Prefix:` prefixes and `<tag` names that its file does not know, and fails on any
it finds, so an addition on Ruwiki's side shows up from the markup itself. Nothing is fetched at build, run or test time.
The manifest records each file's hash.

Each text is parsed with the settings of the site it comes from. The site is named on the command line for
each column or version (see Command line), and is never inferred from a column name, because in
`timeseries_diff` both columns are Wikipedia.

Nothing about Russian is hard-coded. Registry entries (D4) are keyed by template name, with an optional
`site` field for the rare entry that applies to one site only. Parsing another wiki means writing its
settings file and, for useful roles and render rules, its registry entries. Without registry entries it still
parses, and its templates just have role `other`.

### D4. Templates: record every one, expand none, render a declared few (open question 2)

**Answer: templates are never expanded. Each one is recorded as a component with its parameters and
position, and only templates listed in a versioned registry contribute text to the prose, through
declared rules.**

Why not expand:

- Expansion needs MediaWiki: parser functions, Lua modules, transclusion of the template source at the right
  revision, that is, the template versions of 2023 and of 2025, on both sites. Ruwiki has edited its
  templates: `{{объект в Крыму}}` renders differently on the two sites by design.
- Parsoid HTML (Wikimedia Enterprise dumps) exists for Wikipedia only. One expanded site and one unexpanded
  site would make every difference suspect.
- It would make the parser depend on a running MediaWiki or a live site, which this design rules out.

What we do instead:

- **Structure.** Every template invocation becomes a `templates` row.
  - Its name is normalised: namespace prefix (`Шаблон:`, `Template:`, `Ш:`, `T:`) stripped, underscores to
    spaces, whitespace collapsed, first letter case-insensitive.
  - Each row has its positional and named parameters (raw and as plain text, with spans), its parent
    component, its nesting depth and its position.
  - `kind` marks parser functions (`{{#if:...}}`, `{{#invoke:...}}`) and magic variables (`{{PAGENAME}}`).
- **Roles from a registry.** `config/registry/templates.toml` gives a template, and its redirect aliases, one
  of these roles:
  - `infobox`;
  - `citation`, including archive wrappers (`{{webarchive}}`, `{{wayback}}`);
  - `footnote` (`{{sfn}}`, `{{efn}}`, `{{комментарий}}`);
  - `ref_list` (`{{примечания}}`, `{{reflist}}`);
  - `link_status` (`{{недоступная ссылка}}`, `{{dead link}}`);
  - `inline_text`;
  - `interlanguage` (`{{нп3}}`, `{{iw}}`, `{{не переведено}}`);
  - `navigation`, `maintenance`.

  An unknown template has role `other`. Infoboxes also get a `looks_like_infobox` flag from a structural
  heuristic (top of the lead, mostly named parameters), so that an infobox missing from the registry is still
  found and the registry can be completed.
- **Rendering.** An `inline_text` entry declares how the template reads in prose.
  - The rules are small and declarative: `drop`, `param(2|1)` (the second positional parameter, otherwise
    the first), `literal`, and a few built-ins, such as the date rule that the legacy `{{СС3}}` code
    implements.
  - A template without a rule adds no text, but keeps its anchor in the paragraph (D6).
  - Text that came from a template can be identified (`rendered_len` on the template row), so an analysis can
    exclude it.
  - A template that renders differently on the two sites by design is marked `site_dependent` and is never
    rendered.
- **Coverage, measured.** `parse_components --report-templates` lists templates by frequency, per site and per
  context (inside a ref, inside a paragraph, at the top of the lead), registered or not. The registry is
  complete enough when two conditions hold on the corpus and on the performance sample, for each site:
  - at least 99% of template occurrences inside refs have a role;
  - each of the 200 most frequent templates inside paragraphs has a render rule or an explicit `drop`.

  These thresholds are my call. The point is that coverage is a number in a report, not a feeling.

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

I chose long tables over one row per page with nested lists. "Count citations by domain and site" and "join
the refs of two versions of a page" are one-liners in R, pandas and Athena on long tables, and awkward on
nested ones. Each part file is written under a temporary name and renamed when complete, so a crashed run
never leaves a half-written file that looks finished.

Every row of every component table starts with the same keys:

| Column | Type | Meaning |
|---|---|---|
| `source_file` | string (dict) | input file stem; one value per part file, so it costs almost nothing |
| `row` | int32 | row index in the input file; with `source_file`, the key that survives duplicate `page_id`s |
| `page_id` | string | as in the input |
| `site` | string (dict) | `wikipedia` or `ruwiki`: whose markup this is, and which settings parsed it |
| `version` | string (dict) | the run's label for the text, e.g. `forkday` or `2025-01-01` |
| `column` | string (dict) | the input column the text came from (`official_text`, `clone_text`, `text`, `wikitext`, ...) |
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

- **Source spans are the shared coordinate.** Everything is located in the wikitext of its version by byte and
  code-point span. Containment of spans answers "which section, which paragraph, which template".
- **Paragraph text with anchors.** The structured path builds its own paragraph text while walking the tree,
  and keeps an offset map from source to text.
  - The text includes: plain text and character entities (decoded), bold and italic content, link labels (or
    the target title, plus the link trail), labels of external links (bare URLs add nothing), `nowiki`
    content, and templates that have a render rule.
  - It excludes: refs, templates without a rule, files, categories, comments and tables. Tables have their own
    text, in `table_cells`.

  Everything excluded that sits inside a paragraph keeps a `para_offset`. That is how a ref is tied to the
  sentence it supports: the sentence of paragraph 7 that ends at or before the ref's offset.
- **Keys for comparing versions.** Sections carry `path` (titles from the top, normalised), and paragraphs
  carry `fp`. Citations carry normalised URLs and `fp`, and templates carry name and parameter fingerprints.
  Joining two versions' tables on `page_id` plus these keys is the comparison. The parser does no matching
  itself (see Non-goals).

### D7. References and citations

- **`refs`**: one row per `<ref>` and per footnote template, with `kind`:
  - `inline`: a ref with a body and no name;
  - `named`: a ref with a body and a name (a definition);
  - `reuse`: a self-closing named ref;
  - `list_defined`: a ref defined inside `<references>...</references>` or in a ref-list template parameter
    (`{{примечания|refs=...}}`);
  - `template`: a footnote template (`{{sfn}}`, `{{efn}}`, `{{комментарий}}`).

  Columns:
  - `name` and `group`, read from the tag's source slice, since the crate exposes no attributes;
  - `body_raw`, `body_text`;
  - `def_ref_id`: a reuse points to its definition, resolved by group and name within the page;
  - `n_uses`, on definitions;
  - `resolved`: false for a reuse whose definition is missing.

  Footnote templates also carry `harv_key` and `cites_citation_id`, described below.
- **`ref_lists`**: each `<references>` tag or ref-list template, with its `group` and position.
- **`citations`**: one row per source as written in the markup.
  - **Where rows come from.** A citation row comes from the body of an `inline`, `named` or `list_defined`
    ref, from the body of a footnote template that holds a citation (an `{{efn}}` can), or from a citation
    template in a bibliography section.
  - **What yields none.** A `reuse` ref writes no source, so it yields no citation row: it points to its
    definition through `def_ref_id`. An `{{sfn}}` yields none either: it points to its bibliography entry
    through `cites_citation_id`.
  - **Counting.** Counting by source means counting `citations`. Counting by use means joining `refs` to
    `citations` through those two links. Both are one join, and neither inflates the other.
  - **`context`**: `in_ref`; `bibliography` (a citation template outside any ref, in a section whose role is
    bibliography or external links, such as `Литература`, `Источники` or `Ссылки`); or `other`.
  - **`ref_id`** links back to the ref. A ref can hold several citations (`{{cite web}}; {{cite news}}`).

  Each row has a `family`:
  - from a citation template (`web`, `news`, `book`, `journal`, `encyclopedia`, `other_template`):
    `template_id` and `template_name`, all parameters as `params` (list of name, raw value, plain value), and
    canonical fields mapped from Russian and English parameter names by the registry (`url`, `archive_url`,
    `archive_date_raw`, `title`, `authors` (list), `date_raw`, `year`, `publisher`, `work` (website,
    newspaper, journal or издание), `language`, `isbn`, `doi`, `access_date_raw`, `harv_key`);
  - `bare_link`: a ref with an external link and no citation template (`url`, `title` from the label);
  - `plain_text`: a ref with neither (`text` only).

  Further rules:
  - **Archived URLs.** `url` is always the original URL. When the original arrives wrapped in an archive
    link (a `web.archive.org/web/<timestamp>/<url>` or `archive.today` URL, or a `{{webarchive}}` or
    `{{wayback}}` template next to it or around it), the original goes in `url`, and the archive link goes in
    `archive_url`.
  - **Dead links.** A `link_status` template inside the same ref sets `dead_link = true`.
  - **Hosts and domains.** Every URL also yields `host` (lowercased, `www.` stripped) and
    `registrable_domain` (from the public-suffix list), always computed from the original URL, never from the
    archive URL.
  - **Every ref with a body yields at least one citation row,** so "refs without an identifiable source" is
    countable rather than missing. An unresolved `{{sfn}}` shows up as `resolved = false` on the ref, not as a
    `plain_text` citation.
- **Short footnotes (`{{sfn}}`).** `{{sfn|Иванов|2005|с=12}}` points to a `{{книга|...|ref=Иванов}}` or a
  `{{cite book}}` in the `Литература` section. The parser builds MediaWiki's `CITEREF` key on both ends (author
  surnames, up to four, plus year, or the explicit `ref=` value) and resolves it within the page. The result
  is `cites_citation_id`, or `resolved = false`. The exact key rules live in the registry, and the builder
  verifies them on corpus pages by reading the markup.
- **Section roles** come from `config/registry/sections.toml` (`Примечания`, `Комментарии`: notes;
  `Литература`, `Источники`: bibliography; `Ссылки`: external links; `См. также`: see also).

### D8. Robustness

**Work is limited by counted steps, not by the clock.** The fork's timeout checks the wall clock every
10,000 iterations of its main loop (`parse.rs`). A page near the limit would then come out `ok` on an idle
machine and `fallback` on a loaded one, with different rows. So the vendored copy is patched to count
iterations against a budget instead (`ParseError::BudgetExceeded`, recorded in `VENDORED.md`), and our walk
counts nodes the same way. `--budget` sets the parser's budget in iterations. Its default is calibrated once,
on the corpus and the performance sample, to about the work the parser does in 30 seconds on this machine,
which matches today's timeout. The calibrated value is then fixed in the code and never measured again at
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
5. **Fallback for pages the parser could not finish.** A linear-time lexical scanner reads the raw wikitext.
   - It recovers headings, `<ref>` tags (with name and group), ref lists, templates by brace matching (to a
     bounded depth), wiki links, external links and categories.
   - It parses citation templates by splitting parameters at depth 0.
   - It emits no paragraphs, tables or lists.

   Its rows have `fallback = true`, and the page gets `status = fallback`, with the original failure in
   `status_detail`. This is my call, because of selection: the pages the parser cannot finish are long and
   heavily sourced.
6. **Watchdog.** A supervisor thread watches the workers on the wall clock.
   - If one page takes more than ten minutes, the budgets have failed. That is a bug, since the budgets bound
     all the work.
   - The page's `source_file`, `row` and column are then appended to `<out>/quarantine.list`, and the process
     aborts.
   - `--resume` skips completed part files and gives quarantined pages `status = hang`, plus fallback rows.
   - `hang` is the one status that can depend on the machine, and it should never occur.

Every input row yields exactly one `pages` row per text. A null text gives `status = null_input`, an empty one
`status = empty`, and a redirect `status = redirect` with `redirect_target`.

On the performance sample, the builder measures the rate of pages over budget and the distribution of steps
and times. If more than 0.05% of pages exceed the budget, the next step is to look at the vendored parser's
worst case (fork issue #1) before release, because a bounded fix there beats both the budget and the
fallback. That threshold is my call. The 10 corpus pages that timed out under v0.1.0 are the first test of
any such fix.

#### Calibration (2026-10-27)

Measured with `cargo run --release --example measure` (single thread, one machine, 12 cores), release build at
commit `181dfb8` plus the Task 10 changes. Only aggregate figures from the performance sample appear here.

- **Rate.** On the 20 largest non-slow versions of the public corpus (`tests/corpus/wikipedia_v1.parquet`, parsed
  with no budget), steps per second ranged from 4.4e7 to 1.07e8 with a median of 8.98e7 (about 90 M steps per
  second, 260,000-character pages in 2.5 to 5.7 ms). The 30-second budget is then 30 x 8.98e7 = 2.69e9, rounded
  to two significant digits: `DEFAULT_PARSE_BUDGET = 2_700_000_000`.
- **Slow and largest pages.** All 19 distinct corpus rows marked `slow` (10) or `largest` (9) finish under that
  budget with status `ok`. The largest takes 339,730 steps and about 5.6 ms (the 10 pages that timed out under
  v0.1.0 take at most 207,613 steps and 7 ms). None is over budget. So the step budget is about four orders
  of magnitude above anything in the corpus: it is a guard against a runaway parse, not a limit real pages meet.
- **Performance sample.** One row group (row group 0) of the 2025 cross-section: 14,568 page pairs, 29,136 page
  versions. Pages over budget: 0 of 14,568 official (0.000%), 0 of 14,568 clone (0.000%), below the 0.05%
  threshold. Steps (median / p90 / p99 / p99.9 / max): official 7,170 / 28,250 / 118,224 / 271,987 / 684,448;
  clone 6,846 / 26,502 / 107,772 / 264,761 / 617,729. Parse time in ms (same quantiles, max): official 0.13 /
  0.47 / 1.82 / 4.37 / 11.5; clone 0.12 / 0.43 / 1.72 / 4.13 / 10.4.
- **Throughput.** `parse_components --jobs 12` on the sample, default budget: 6,100 to 7,300 pages per second
  over three runs (wall time 4.8 s for 29,136 versions), 0 seconds over budget. The walk is still a stub,
  so this is parse plus input, output and the `pages` and `timings` tables. Projected on this machine at
  6,100 pages per second: 2 x 866,484 versions about 4.7 min; 1.4 M about 3.8 min; 1.1 M about 3.0 min. All are
  far below the 8-hour target (D9). The real walk will add to this.
- **Batch stalls.** No page exceeded the budget, so no stall from a budget page was observed. The sum of
  per-page parse times is 6.6 s, or 0.55 s over 12 threads; with 256-row batches, waiting for each batch's
  slowest page raises that bound only to 0.56 s. The measured wall time is 4.8 s, so parse time is not what limits
  the run; reading, converting and writing are. If a page ever ran to the budget (about 30 s), its batch would
  wait for it.

#### Release 1.0.0 (2026-10-28)

Measured with the full walk (all eight tables), release build at commit `8b4b801` plus the Task 21 changes, on
the same performance sample (row group 0 of the 2025 cross-section: 14,568 pairs, 29,136 page versions, 148 MB),
`parse_components --jobs 12`, default budget, labels `official_text=wikipedia:2025-01-01` and
`clone_text=ruwiki:2025-01-01`, output outside the repository. The machine has 12 hardware threads. Only
aggregates from the manifest and the `timings` table appear here. The sample is kept outside the repository; it
was not deleted.

- **Throughput.** 1,284, 1,287 and 1,290 pages per second over three runs (`pages_per_second`; wall time 22.7 to
  22.8 s, 55 s of user time, peak resident memory 1.2 GB). Output: 227 MB for the sample, about 13.5 GB projected
  for the whole cross-section.
- **Budget.** 0 of 29,136 versions over budget (0.000%), `seconds_over_budget` 0.0, no failed inputs. Status
  counts: `wikipedia`/`2025-01-01` 14,562 `ok` and 6 `redirect`; `ruwiki`/`2025-01-01` 14,568 `ok`. No
  `fallback`, `budget`, `depth_limit`, `panic` or `hang`. The largest parse took 684,448 steps.
- **Per page** (ms, median / p90 / p99 / p99.9 / max), parse plus walk: Wikipedia 0.73 / 2.78 / 10.83 / 27.66 /
  59.3; Ruwiki 0.67 / 2.56 / 9.05 / 23.10 / 41.2. The walk takes about four times the parse (7.4 s of parse and
  30.4 s of walk summed over all versions).
- **Projection** at the slowest run's 1,284 pages per second, counting two versions per input row (each family's
  files have two text columns):
  - cross-section, 866,484 pairs = 1,732,968 versions: about 22.5 min (0.37 h);
  - Wikipedia time series, 1.4 M rows = 2.8 M versions: about 36 min (0.61 h); 18 min if the 1.4 M are
    versions, as the calibration note read them;
  - Ruwiki time series, 1.1 M rows = 2.2 M versions: about 29 min (0.48 h); 14 min if versions.

  Every family is far below the 8-hour target (D9), by a factor of 13 or more. No page was over budget, so
  the projection includes no budget pages; one at the full budget would cost its batch about 30 s.
- **Where the time goes.** Parse and walk sum to 37.8 s of CPU time, 3.2 s over 12 threads, against 22.7 s of
  wall time. As in the calibration note, the run is limited by the serial part (reading, converting, serialising
  and writing), not by parsing. That is the place to speed it up, should it ever matter.
- **Locality on the sample** (`locality_report_sample`, counts only). 14,568 pairs, 368 byte-identical.
  Comparing each Wikipedia version with its Ruwiki version, 344,409 rows differ in `fp`; 345 of them lie
  outside every changed line range (with its one-line widening) and are not exempted by the section rule. They
  fall in 21 pairs (5 with one, 7 with 2 to 9, 9 with 10 or more), none byte-identical. By table: paragraphs
  319, sections 10, templates 10, refs 3, citations 3. By class, read locally without recording any content:
  - **Enclosure changes**, 334 (paragraphs 319, templates 9, refs 3, citations 3): unchanged lines that sit inside a comment, a table or a
    template on one side only, because an edit elsewhere added, removed or moved the opener or closer
    (commented-out passages, a list wrapped in a layout table, a table whose closing `|}` was removed). In the
    report these are rows that are absent on the other side, or present at another span. The line diff cannot
    see this long-range effect of an edit. Tables carry no text in 1.0.0, so text that moves into or out of a
    table disappears or appears.
  - **Section reparenting**, 10: a heading inserted, removed or given another level elsewhere gives later
    sections, at the same span, another parent and so another path. The ancestor rule (a section is touched
    through its ancestors' headings) exempts only the version whose changed heading is an ancestor.
  - **Repeated `fp`**, 1: a template row that is the surplus of a repeated `fp`, inside such a commented region.

  No class points at a settings or registry difference between the sites (no row at the same span with
  differing parameters or text, no citation lost where its template remains), and byte-identical pairs have
  none.


### D9. Efficiency

- One process handles one input file or a whole directory, so the per-file `cargo run` in the scripts goes
  away.
- Batches are streamed from the reader (a few hundred rows at a time). Pages are parsed in parallel with
  rayon (`--jobs`, default all cores), and rows are written in input order, so output is deterministic. Memory
  is bounded by the batches in flight, not by the file.
- Regexes are compiled once. There is no thread per article.
- Parquet output uses zstd, and dictionary encoding for repeated strings (site, version, template names,
  kinds).
- **Measured on a sample, not on the full corpus.** The performance sample is one row group of the 2025
  cross-section (about 14,000 pages, about 0.5 GB), downloaded after telling the user, deleted afterwards.
  - The manifest reports pages per second and the time spent on pages over budget.
  - The 5 largest corpus pages (up to about 260,000 characters) and the 10 slow ones are timed separately.
  - The target: the throughput implies that each input family (866,484 pairs in the cross-section; 1.4
    million rows in the Wikipedia time series, 1.1 million in Ruwiki's) parses in one night (8 hours or
    less) on this 12-core machine, pages over budget included.

  The replication package runs the real thing, one file or row group at a time.

### D10. Stability

- **Schema version.**
  - `SCHEMA.md` documents every table and column, including the normalisation behind each `fp`.
  - `schema_version` starts at `1.0.0`. Adding a column is a minor change. Renaming, removing or changing
    meaning is a major one.
  - A test serialises the Arrow schema of every table to `tests/schema/v1.json` and fails on any difference,
    so a schema change cannot slip in without a version bump.
  - `1.0.0` is frozen at the first release (end of milestone 2). Tables that arrive later are minor
    additions. The tables released then keep their ids, because ids count document order within one table,
    and no later milestone adds rows to a released table.
- **Manifest.** `manifest.json` records:
  - the schema version, the crate version and git commit, and the vendored parser's upstream commit and
    patches;
  - the hashes of the settings and registry files, and the arguments;
  - each input file and each output file, with its row count;
  - status counts per site and version, and throughput.
- **Fixed test corpus.** `parser_test_corpus_v1.parquet`, drawn in the replication package
  (`code/data_prep/09_sources/02_parser_test_corpus.py`, seed 20261027; `sources_design.md`, S1a), sha256
  `9800a5a3a11feee64af7bf2ebcaf44d281ea59619b82f81af1a67835ef263d06`. Tests check the hash before using it.
  It has 105 pages in 255 versions, in the long format:
  - 75 heavily edited articles, each in three versions (`wiki_forkday`, `wiki_2025`, `ruwiki_2025`), spread
    over topic and size tercile, including the 5 largest;
  - 21 pages Ruwiki deleted (`deleted_wiki_2025`);
  - 10 pages whose fork-day parse timed out at 30 s under v0.1.0.

  The three versions of the same article are what the version-comparison tests run on. The corpus is
  split by site:
  - **Public part, committed.** The 180 Wikipedia versions of all 105 pages (`wiki_forkday`, `wiki_2025`,
    `deleted_wiki_2025`): 7.7 MB of markup, about 1.75 MB as zstd Parquet, in
    `tests/corpus/wikipedia_v1.parquet`, with a `.gitignore` exception. A committed script cuts it from v1 and
    checks the result's hash.
    - It keeps `page_id`, `page_title`, `version`, `timestamp`, `wikitext` and `n_chars`.
    - It replaces the package's `why` column with a `case` column that holds only what tests need (`slow`,
      `largest`, or empty). The topic labels are the package's, not Wikipedia's, so they stay there.
    - `tests/corpus/ATTRIBUTION.md`, generated from the corpus manifest, names each page, its revision
      timestamp and the CC BY-SA 4.0 license.
  - **Private part, never committed.** The 75 Ruwiki versions (`ruwiki_2025`) stay in the replication
    package. Tests read them from v1 by path (`WIKITEXT_PRIVATE_CORPUS`, defaulting to the package's
    `data/results/sources/test_corpus/`) and check the whole file's hash first. When the file is absent,
    the private tests report themselves as skipped, by name, and never pass silently. So the public
    repository and GitHub's CI run the public part, and this machine runs both.

  Expected outputs follow the same split. Those of Wikipedia versions are committed. Those of Ruwiki
  versions are not:
  - only their sha256 hashes are committed (`tests/expected/private.sha256`), which reveal no text;
  - the readable copies sit in `tests/private/`, gitignored, and are regenerated locally;
  - their hand-checked facts sit next to the corpus in the replication package.
- **Hand-written fixtures** for cases the corpus may lack. The builder first checks the corpus for each, then
  writes a small `.wiki` snippet by hand for any that is missing: a redirect, grouped refs (`group=`),
  list-defined refs, a gallery, nested tables, unclosed templates, tables and tags, deep nesting, an unresolved
  `{{sfn}}`, a ref reused before its definition. These are ours, small, and committed under `tests/fixtures/`
  (`.wiki`, not `.txt`, which is gitignored).
- **Legacy regression.** The legacy binaries read the fetch format, so a test helper reshapes the corpus.
  - Every version goes through `parse_single`.
  - The two-column `wikitext_parser_rust` runs on two kinds of pairs. Publicly, `wiki_forkday` with
    `wiki_2025`, shaped like a `timeseries_diff` file. Privately, `wiki_2025` with `ruwiki_2025`, shaped like a
    `crossection_diff` file.
  - Both outputs go through `clean_parsed`, and then `export_parsed`.
  - The expected outputs are generated once, by building tag `v0.1.0` in a separate worktree, with a
    committed script.
  - The test asserts byte equality, with `--timeout 0`. The 10 slow pages are excluded, since the legacy
    placeholder only tests the timer.
- **Structured expected output.** Every corpus version's structured output is pinned, which catches drift.
  - Readable JSON snapshots are committed for about 40 public versions (at least one per version label and
    size tercile, plus a few slow ones), and reviewed on every change.
  - For every other public version, only the hash of its canonical output is committed. On a mismatch, the
    test writes the new output to `target/snapshots-new/` for diffing.
  - The size cap is my call: the five largest pages alone would make the readable snapshots heavy in git.

  Evidence that the parse is right comes from these checks, all offline:
  - **Facts read from the markup.** For about 12 page versions, across both sites and all four version labels,
    the builder reads the markup and records facts: number of refs by kind, reuses and their targets, sfn
    targets, citations by family, original URLs behind archive links, infobox field count, section titles,
    external-link hosts. These are counted from the markup by reading, not with the parser, so they check it.
    Facts for Wikipedia versions go in `tests/facts.toml`, and those for Ruwiki versions in the private part.
  - **Symmetry.** Where two versions are byte-identical, every component table must be identical apart from
    the labels. In public, every Wikipedia version is also parsed with Ruwiki's settings file, and must give
    the same tables, which tests the two settings files against each other without any Ruwiki markup. In
    private, the same holds with real data: a page Ruwiki never edited still has the markup of the fork-day
    Wikipedia version.
  - **Locality.** Between two versions of a page, every component that differs must overlap a region that the
    wikitext diff marks as changed. A component that differs where the markup did not change is a parser
    artefact: a settings mismatch, or a template recognised on one side only. It runs publicly on
    `wiki_forkday` against `wiki_2025`, and privately on `wiki_2025` against `ruwiki_2025`. It also runs on
    the performance sample, as a report the builder reads before release.
- **Robustness test.** Every corpus version is truncated at every 997th character boundary, and separately
  stripped of its closing braces. Each must parse without a panic and within the budget, and yield a status.
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
| `citations` | see D7 (including `archive_url`, `dead_link`, `host`, `registrable_domain`) |
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
    --label <key>=<site>:<version>  # required for every text, never inferred. <key> is a text column
                                    # (pair and single) or a value of the version column (long):
                                    #   crossection_diff: --label official_text=wikipedia:2025-01-01
                                    #                     --label clone_text=ruwiki:2025-01-01
                                    #   timeseries_diff:  --label official_text=wikipedia:forkday ...
                                    #   test corpus:      --label wiki_forkday=wikipedia:forkday
                                    #                     --label ruwiki_2025=ruwiki:2025-01-01 ...
    [--layout pair|single|long]     # auto-detected from the columns
    [--text-column <name>]          # single-column input, as parse_single
    [--format parquet|json]         # json: one document per page version, for a .wiki input and the snapshots
    [--tables refs,citations,...]   # write a subset; pages is always written
    [--jobs N] [--budget STEPS] [--no-fallback] [--resume]
    [--report-templates <file>]

parse_components --input article.wiki --label wikipedia:adhoc --format json --out article.json
```

A label with no matching column or version value is an error, and so is a text with no label. Column
detection for single-column input is copied from `parse_single.rs` into the new code, not moved, so the
legacy file stays untouched.

## Build order

Each milestone ends green on the tests that exist by then.

0. **Lock the baseline.** Commit:
   - the lockfile;
   - the public corpus file with its cutting script and attribution, and the private corpus access with its
     hash check and visible skip;
   - the reshaping helper;
   - the legacy expected outputs generated from `v0.1.0` (committed for Wikipedia versions, hashes only for
     Ruwiki versions);
   - the legacy regression test.

   Nothing else changes.
1. **Frontend and driver.**
   - Vendor the fork and patch in the step budget.
   - Write the two settings files and the unknown-prefix test.
   - Build the adapter, the streaming parallel driver for all three layouts and `.wiki` files, the `pages`
     and `timings` tables, `catch_unwind`, the watchdog, the manifest and the schema test.
   - Calibrate the budget, then measure throughput and the rate of pages over budget on the performance
     sample, telling the user the download size first.
2. **References first, complete.**
   - Sections and paragraphs with anchors.
   - The full `templates` table: every template with its parameters, with roles so far only for citation,
     footnote, ref-list and link-status templates.
   - Refs, ref lists and citations: the citation part of the registry, archive unwrapping, sfn resolution,
     hosts and domains.
   - The ref and citation part of the fallback scanner, so that pages over budget are not missing from
     references.
   - The facts, symmetry and locality tests for refs and citations.

   This is the first release, to the sources workstream, and schema `1.0.0` is frozen here.
3. **The rest of the templates, and links.** Roles for all templates and the infobox heuristic, the inline
   render rules, `--report-templates`, links, categories and files. Complete the registry to the coverage
   thresholds of D4.
4. **Tables and lists.**
5. **The rest of the fallback scanner** (headings, templates, links and categories) and the truncation
   robustness test.
6. **Release.** Write `SCHEMA.md`, update the README and CLAUDE.md, and tag `v0.2.0`.
7. **Optional: a bridge to the paper's text.** For each v0.1.0 paragraph, the structured paragraphs it
   corresponds to, by deterministic normalised-text matching, as a `legacy_map` table. This would tie a ref
   to the paper's paragraphs and to the pair layer's hunks. It is my call to include it, and the easiest
   thing to drop.

## Left to the builder

- Module layout inside the new code, and the shape of the adapter.
- Prose edge cases (`<br>`, `<math>`, `<poem>`, `<sup>`), within the principles in D6, documented in
  `SCHEMA.md`.
- Batch size, row-group size, compression level.
- The exact normalisation behind each `fp` and `title_norm`, documented in `SCHEMA.md`.
- Crates for the public-suffix list, hashing (xxh3 suggested) and snapshot testing.
- Iterative walk or recursion with a large stack.
- Which page versions get hand-checked facts, within D10.

## Decided by you

- The goals, constraints and the two open questions as set out for this session (the library, and how to
  handle templates).
- "The parser needs to be applicable to any wikitext article, including the fork-day text." (Goal 1, D3,
  D10.)
- No data collection: "this is purely about developing the parser we need". Development and tests run on
  stored markup, offline (Why, Constraints, D3, D10).
- "The test corpus data originating from Wikipedia can also be part of the public parser repo. The Ruwiki
  parts not." (Constraints, D10, milestone 0.)

## Calls I made, for you to check

1. Legacy path frozen on `parse_wiki_text` 0.1.5. The structured path moves to the fork, vendored, not
   fetched.
2. Templates are never expanded. A registry declares roles and the few render rules, and coverage is
   measured.
3. Long tables, one per component type, instead of nested rows per page.
4. The parser emits keys for comparing versions but does no matching.
5. A fallback scanner for pages the parser cannot finish. Its rows are flagged, not dropped.
6. Static settings files, one per site. Ruwiki's starts as a copy of Russian Wikipedia's, and a test fails on
   any prefix or tag in Ruwiki's markup that the file does not know.
7. The replication package's corpus (105 pages, 255 versions) as the fixed test corpus, plus small
   hand-written fixtures for cases it lacks.
8. Commit `Cargo.lock`.
9. Both byte and code-point offsets. Fingerprints as hex strings.
10. The thresholds: 99% in-ref template coverage, the top 200 paragraph templates, 0.05% of pages over budget
    before looking at the parser's worst case, one night per input family as the throughput target.
11. The bridge to the paper's text is optional and last.
12. A counted step budget instead of a timeout, so a page's status does not depend on the machine. Timings
    live in their own table.
13. Every text gets an explicit `site:version` label on the command line, with no default, so the Ruwiki
    settings can never be applied to Wikipedia text by accident.
14. The citation unit is the source as written: reuses and `{{sfn}}` link to it rather than repeating it.
    `url` is always the original URL, with the archive copy in `archive_url`.
15. Correctness evidence, all offline: facts read from the markup, plus symmetry and locality tests across
    versions. The first release (milestone 2) is complete for refs and citations, fallback included.
16. I read "any wikitext article" as: any version, any input layout, any single `.wiki` file, and any
    MediaWiki site given its settings file. Only Russian Wikipedia and Ruwiki ship with settings and
    registry entries.
17. The full run over the stored corpus belongs to the replication package. The parser repository measures
    throughput on one row group.
18. How your corpus split is carried out:
    - The public file drops the package's topic labels and keeps only `slow` and `largest` markers.
    - Ruwiki versions are tested from the package by path, and their tests skip visibly when it is absent.
    - Only the hashes of Ruwiki expected outputs are committed, never their text.
    - Readable snapshots are kept for about 40 public versions; the rest are pinned by hash.

## Sources

- `parse-wiki-text-2`: https://github.com/soerenmeier/parse-wiki-text-2 (issues #1, #8, #9, #10), crates.io
  0.2.0.
- `parse_wiki_text` 0.1.5: https://docs.rs/parse_wiki_text (upstream repository no longer exists).
- `wikrs` design document: https://docs.rs/crate/wikrs/0.4.0/source/docs/DESIGN.md
- MediaWiki's Russian language file: `languages/messages/MessagesRu.php` in the MediaWiki source.
- `ruwiki_replication_package_sources`: `docs/revision/sources_design.md` (S1, S1a),
  `data/results/sources/test_corpus/`, `docs/revision/pair_llm_layer_design.md` (line 25),
  `code/data_prep/07_pair_llm_layer/08c_parser_residue.py`.
