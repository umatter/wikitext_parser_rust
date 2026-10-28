# Structured parse output: schema 1.0.0

This is the output of `parse_components`, the structured parse of wikitext (design:
`docs/superpowers/specs/2026-10-27-structured-parse-design.md`). It is a new path beside the legacy binaries and
shares no parsing code with them. Its paragraph text is not the text of `wikitext_parser_rust` v0.1.0 (the
paper's text): Russian namespaces are recognised, so file captions and category links stay out, Cyrillic link
trails stay in, and the rules below decide the rest.

Schema `1.0.0` holds eight tables: `pages`, `timings`, `sections`, `paragraphs`, `templates`, `refs`,
`ref_lists` and `citations`. It is frozen: `tests/schema/v1.json` pins every column's name, type, nullability and
metadata, and the test fails on any difference.
- Adding a column or a table is a minor change (`1.1.0`). Renaming or removing one, or changing what it means,
  is a major one (`2.0.0`).
- Tables that arrive in later milestones (`links`, `categories`, `files`, `tables`, `table_cells`, `lists`) are
  minor additions. The released tables keep their ids: an id counts document order within one table, and no
  later milestone adds rows to a released table.

The code is the source of truth. Each rule below names the function or module that computes it
(`src/components/...`).

## Files

```
<out>/
  manifest.json
  pages/<stem>.parquet       timings/<stem>.parquet
  sections/<stem>.parquet    paragraphs/<stem>.parquet
  templates/<stem>.parquet   refs/<stem>.parquet
  ref_lists/<stem>.parquet   citations/<stem>.parquet
```

- One part file per input file and table; `<stem>` is the input file name without its extension. A part is
  written as `<stem>.parquet.tmp` and renamed when complete, and `pages` is renamed last, so a finished `pages`
  part means every other part of that input is finished too.
- `--tables a,b` writes a subset; `pages` is always written.
- `--format json` writes, instead, one JSON document per page version: `<out>/<stem>.jsonl` for a Parquet input,
  or one pretty-printed document at `--out` for a `.wiki` input. A document is
  `{"page": {...}, "sections": [...], "paragraphs": [...], "templates": [...], "refs": [...], "ref_lists": [...],
  "citations": [...]}` with the columns below as keys, in column order, without the key columns in the component
  rows and without timings (`output::page_value`).
- Parquet files are zstd-compressed. `source_file`, `site`, `version` and `column` are dictionary-encoded
  (`Dictionary(Int32, Utf8)`). Every file carries `wikitext_parser.schema_version = 1.0.0` in its key-value
  metadata, and so does every Arrow field, nested fields included.

### Types

| Here | Arrow |
|---|---|
| string | `Utf8` (never `LargeUtf8`) |
| string (dict) | `Dictionary(Int32, Utf8)` |
| int32, int64 | `Int32`, `Int64` |
| float64 | `Float64` |
| bool | `Boolean` |
| list\<x\> | `List` (never `LargeList`) of non-null `x` |
| struct | `Struct` of the fields named |

There are no unsigned 64-bit integers and no map type, so R (arrow), Python (pyarrow, pandas) and Athena read
every column. A column marked *null* may be null; every other column is never null. A list is never null: it
is empty instead. A non-null string can be `""` where its rule says so.

### Order and reproducibility

Rows are in input order (input row, then the text columns in the order of their layout), then in document
order. Same input, same build, same bytes, on any machine and under any load, for every table except
`timings`. A page's status never depends on how fast the machine is (budgets count steps, not seconds); the one
exception is `hang`, which should never occur.

## Labels and keys

Every text is labelled on the command line, `--label <key>=<site>:<version>`, and a label is never inferred
(`input::parse_label`, `input::open`).
- `<key>` is a text column (pair and single layouts) or a value of the `version` column (long layout). A `.wiki`
  input takes `<site>:<version>` with no key.
- `<site>` names a settings file by its `site` key: `wikipedia` (`config/sites/ru.wikipedia.toml`) or `ruwiki`
  (`config/sites/ruwiki.toml`). The text is parsed with that site's settings and registry entries. A site with
  no settings file is an error.
- `<version>` is free text, carried into the `version` column.
- A label that matches no column or version value is an error, and so is a text without a label, and so is a key
  given twice.

The layout is detected from the columns (`input::detect_layout`), or forced with `--layout`:

| Layout | Detected by | Texts, with their `title` and `timestamp` columns | `--label` keys |
|---|---|---|---|
| `long` | columns `version` and `wikitext` | `wikitext` (or `--text-column`), `page_title` or `title`, `timestamp` | the values of `version` |
| `pair` | columns `official_text` and `clone_text` | `official_text` (`page_title`, `official_timestamp`), then `clone_text` (`clone_page_title`, `clone_timestamp`) | `official_text`, `clone_text` |
| `single` | a text column, found as `parse_single` finds it: `text`, `content`, `official_text`, `clone_text`, else the first column whose name contains `text` | that column (or `--text-column`), `page_title` or `title`, `timestamp` | the text column's name |

The page id column is `page_id` or `pageid` (string or integer). A `.wiki` file is one page version whose
`page_id` and `source_file` are the file's stem, `row` 0 and `column` `wikitext` (`input::wiki_file`).

The labels in use:

| Input | Labels |
|---|---|
| test corpus (long) | `wiki_forkday=wikipedia:forkday`, `wiki_2025=wikipedia:2025-01-01`, `deleted_wiki_2025=wikipedia:2025-01-01`, `ruwiki_2025=ruwiki:2025-01-01` |
| `crossection_diff` (pair) | `official_text=wikipedia:2025-01-01`, `clone_text=ruwiki:2025-01-01` |
| `timeseries_diff` (pair) | `official_text=wikipedia:forkday`, and `clone_text=wikipedia:<date>`: both columns are Wikipedia |
| `ruwiki_timeseries_diff` (pair) | `official_text=ruwiki:forkday`, and `clone_text=ruwiki:<date>`: both columns are Ruwiki (the fetch reads both from Ruwiki's edit history) |

A column name never says which site a text comes from, so the site is always given.

### Key columns

Every table starts with these six columns, in this order.

| Column | Type | Meaning |
|---|---|---|
| `source_file` | string (dict) | the input file's stem |
| `row` | int32 | the 0-based row index in the input file; with `source_file`, the key that survives duplicate `page_id`s |
| `page_id` | string | as in the input; an integer id as its decimal text; a null id as `""` (the row stays apart by `row`) |
| `site` | string (dict) | the label's site: whose markup this is, and which settings parsed it |
| `version` | string (dict) | the label's version |
| `column` | string (dict) | the input column the text came from (`official_text`, `clone_text`, `text`, `wikitext`, ...) |

A page version is identified by (`source_file`, `row`, `column`); join a version's component rows to its `pages`
row on those three.

## Columns of every component table

`sections`, `paragraphs`, `templates`, `refs`, `ref_lists` and `citations` continue with these twelve columns,
in this order. `pages` and `timings` have none of them.

| Column | Type | Meaning |
|---|---|---|
| `id` | int32 | index of the row within its table and page version, dense from 0, in document order |
| `start`, `end` | int32 | UTF-8 byte offsets into the version's wikitext, 0-based, end exclusive; always inside the text |
| `start_char`, `end_char` | int32 | the same span in Unicode code points (`spans::CharIndex`), for slicing in R and Python: `substr(x, start_char + 1, end_char)`, `x[start_char:end_char]` |
| `section_id` | int32 | the section the component sits in: the one opened by the last heading before it (0 is the lead); a list-defined ref takes its list's |
| `para_id` | int32, null | the paragraph the component sits in, if any |
| `para_offset` | int32, null | the code-point offset in that paragraph's `text` where the component sits; null exactly when `para_id` is |
| `parent_type` | string, null | the table of the nearest enclosing component: `template`, `ref` or `ref_list`; null at the top level |
| `parent_id` | int32, null | that component's `id` in its table; null exactly when `parent_type` is |
| `fallback` | bool | true when the row comes from the fallback scanner (pages with status `fallback` or `hang`), not the parser |
| `fp` | string | content fingerprint: 16 lowercase hex characters (see Fingerprints) |

Document order is `start`, with an enclosing component before what it contains.

**Placement** (`walk/mod.rs`, `Walker::place`). A component is placed where the walk enters it.
- Inside running text it takes the open paragraph and the length of that paragraph's text so far. That is how a
  ref is tied to the sentence it supports: the sentence of its paragraph that ends at or before `para_offset`.
- Everything excluded from the paragraph text (a template, a ref, a table, a comment, ...) keeps the paragraph
  and offset where it sits, and so does everything inside it: a component inside excluded content takes the
  `para_id` and `para_offset` of the outermost excluded element around it.
- `para_id` and `para_offset` are null in a heading, in a table, in a paragraph that ends up with no text (it is
  dropped and takes no id), and on every fallback row.
- A paragraph row has `para_id` = its own `id` and `para_offset` null. A section row has `section_id` = its own
  `id`, and `para_id`, `para_offset`, `parent_type` and `parent_id` null; so do paragraph rows for the parent.

**Parents** (`walk/mod.rs`, "Parents"; `walk/collect.rs`). The nearest enclosing component of any table: a
template (in its name or a parameter value, however deep inside links or tags), a ref (inside its body), or a
`<references>` tag (`ref_list`, inside the tag).
- A footnote template (`{{sfn}}`, `{{efn}}`) is both a `templates` row and a `refs` row, the ref innermost: what
  its parameters contain has the ref as parent. Both rows have the parent of the template.
- A ref-list template (`{{примечания|refs=...}}`) is a parent as a `template`.
- A template inside a ref inside a template has the ref as parent.

## Fingerprints and normalisations

- **`fp(s)`** (`fingerprint::fp`) is XXH3-64 with the default seed 0 (`xxhash_rust::xxh3::xxh3_64`) of the UTF-8
  bytes of `s`, printed as 16 lowercase hex digits.
- **`norm(s)`** (`fingerprint::norm`) lowercases `s` (Rust `str::to_lowercase`, full Unicode), splits it at
  Unicode whitespace (`char::is_whitespace`, so a no-break space too) and joins the pieces with one space: runs
  collapse and both ends are trimmed.
- `␟` below is U+001F (unit separator), the joiner of every compound fingerprint.

| Table | `fp` of | Code |
|---|---|---|
| `sections` | the path's titles joined by `␟`: `fp(path[0] ␟ path[1] ␟ ...)`; the lead's path is empty, so its `fp` is `fp("")` | `walk/sections.rs`, `Sections::finish` |
| `paragraphs` | `fp(norm(text))` | `walk/text.rs`, `TextBuilder::break_paragraph` |
| `templates` | `fp(name ␟ p₁ ␟ p₂ ...)`, where each `pᵢ` is `param.name + "=" + norm(param.value_text)` and the pairs are sorted by parameter name (byte order, stable, so duplicates keep source order); a template without parameters is `fp(name ␟)` | `walk/templates.rs`, `Templates::close` |
| `refs` | `fp(kind ␟ group ␟ name ␟ norm(body_text))`, `name` `""` when null | `walk/refs.rs`, `Refs::finish` |
| `ref_lists` | `fp(kind ␟ group ␟ template name)`, the name `""` for a tag | `walk/refs.rs`, `Refs::finish` |
| `citations` | `fp(family ␟ identity)`: `identity` is `url` when present; else, when there is a `title` or an author, `norm(title + " " + authors joined by " ")` (a null title as `""`); else `norm(text)` (a null text as `""`) | `walk/citations.rs`, `Citations::finish` |

So a section's `fp` changes when an ancestor's title changes (the spec joins versions by section path), a
template's does not depend on parameter order, and a citation's is its URL whenever it has one.

- **`title_norm`** of a section is `norm(title_text)` (`Sections::finish`). Section roles are looked up by it.
- **Template `name`** (`walk/templates.rs`, `Templates::enter`):
  - kind `template`: `Settings::template_name(name_raw)`, that is `settings::normalize_template_name`: `_` read
    as a space; trimmed; a prefix before the first `:` that names namespace 10 in the site's settings (`Шаблон`,
    `Template`, `Ш`, `T`, matched ignoring case) stripped; whitespace runs collapsed to one space; the
    first character lowercased (only the first). `{{Шаблон:Cite_web}}` is `cite web`, `{{ВТ-ЭСБЕ}}` is
    `вТ-ЭСБЕ`.
  - kinds `parser_function` and `module` with a colon: `norm` of the word before the first colon of the name
    (`{{#invoke:X|f}}` is `#invoke`, `{{#if: ...}}` is `#if`, `{{lc:...}}` is `lc`); without a colon, and for
    kind `variable`: `norm(name_raw)` (`{{PAGENAME}}` is `pagename`, `{{!}}` is `!`). Magic words are not page
    names, so they are lowercased fully.
  - The registry is keyed by the same rule (`registry::entry_key`), so a template's `role` is the registry entry
    of its `name` on its `site`.
- **`harv_key`** (MediaWiki's `CITEREF` anchor; `walk/citations.rs`):
  - `citeref(names, year)` = `"CITEREF"` + each name trimmed, with its inner whitespace runs as `_`, + `year`
    (`citations::citeref`).
  - A citation that is not of family `short` (`citations::harv_key`):
    - with a non-empty `harv_ref` (the registry's `ref=` parameters) that is not `harv` (in any case):
      `citeref([harv_ref], year)`, the year `""` when there is none;
    - else, when it has names and a year: `citeref(names, year)`. The names are the non-empty parameters of the
      entry's `harv_names` list in order (`last1`..`last4`), else the surnames of `authors` (each author's text
      before its first whitespace or comma); at most four;
    - else null.
    - The year is `year` as digits. It keeps a letter only when the year parameter itself is a whole
      `^\d{4}[a-zа-я]$` (`1990a`); a year read from `date_raw` never carries one. A year parameter that is a whole
      date `^\d{1,2}\.\d{1,2}\.\d{4}$` (`04.08.2009`) is used as written, as pages key their `{{sfn}}` calls by
      it.
  - A short footnote (`{{sfn}}`) and a citation of family `short` (`{{harvnb}}`): `sfn_key` on its positional
    parameters. Those before the first one whose trimmed `value_text` matches `^\d{4}[a-zа-я]?$` are the names
    (empty ones skipped), that one the year; with no such parameter all are names and the year is `""`. Null
    without a name.
- **URLs** (`urls::analyse`), for every `url` and `archive_url`, from the parameter's `value_raw` or the external
  link's URL as written:
  - character entities decoded (`&amp;`, `&lt;`, `&gt;`, `&quot;`, `&apos;`, `&nbsp;` as a space, and numeric
    `&#N;`, `&#xH;`), then trimmed; a protocol-relative `//host/...` becomes `https://host/...`;
  - an archive URL is unwrapped (`urls::original_from_archive`): `web.archive.org/web/<timestamp>/<original>`
    (also `archive.org/web/...` and `www.`), `archive.today`, `archive.ph`, `archive.is` and `archive.li`
    `/<timestamp>/<original>`, and `webcitation.org/...?url=<original>`. The original goes in `url` and the
    archive URL in `archive_url`. An original without a scheme gets one: `http:/x` becomes `http://x`, a bare
    `x.ru/a` becomes `http://x.ru/a`.
- **`host`** (`urls::host_of`): the host of `url` as `url::Url` parses it, IDNA-decoded to Unicode, lowercased,
  without the port, without a leading `www.` and trailing dots; an IP address as written by `url`. Null when
  the URL does not parse or has no host. Always computed from the original, never the archive URL.
- **`registrable_domain`** (`urls::registrable`): the registrable domain of `host` under the public-suffix list
  (`psl` crate, on the host's ASCII form), back in Unicode. Null for an IP address or when the list gives none.

## pages

One row per page version, always, whatever happened to it (`page.rs`). It has no `id`, `para_*`, `parent_*`
or `fp`.

| Column | Type | Meaning |
|---|---|---|
| *(keys)* | | the six key columns |
| `title` | string, null | the title column of the text's layout (see Labels); null when there is none or the value is null |
| `timestamp` | string, null | the timestamp column: a string passes through as it is; an Arrow timestamp becomes RFC 3339 UTC (`2024-12-28T01:38:34Z`, fractional seconds only when non-zero); null when there is none or the value is null |
| `status` | string | see Statuses |
| `status_detail` | string, null | why a page is not `ok`, as `<status>: <message>` (see Statuses); null for `ok`, `redirect`, `null_input` and `empty` |
| `parse_steps` | int64 | the parser's counted main-loop steps; for a page over the budget, the count at which it stopped; 0 when the text was not parsed (`null_input`, `empty`, `hang`, a panic in the parser) |
| `n_bytes` | int32 | the text's length in UTF-8 bytes; 0 for a null text |
| `n_chars` | int32 | the text's length in code points; 0 for a null text |
| `n_warnings` | int32 | the number of warnings the parser gave |
| `warning_counts` | list\<struct{`kind` string, `n` int32}\> | every warning counted by kind, sorted by kind |
| `warnings` | list\<struct{`kind` string, `start` int32, `end` int32}\> | the first 100 warnings in the parser's order, with their byte spans |
| `redirect_target` | string, null | for `redirect`, the target as the parser reads it; else null |
| `n_sections`, `n_paragraphs`, `n_templates`, `n_refs`, `n_ref_lists`, `n_citations` | int32 | the number of the version's rows in each component table |

A warning's `kind` is the name of the vendored parser's `WarningMessage` variant (`DefinitionTermContinuation`,
`EndTagInComment`, `InvalidCharacter`, `InvalidHeadingSyntaxRewinding`, `InvalidLinkSyntax`,
`InvalidParameterSyntax`, `InvalidTagSyntax`, `MissingEndTagRewinding`, `RepeatedEmptyLine`, `StrayTextInTable`,
`TextAfterRedirect`, `UnexpectedEndTag`, `UnexpectedEndTagRewinding`, `UnexpectedHeadingLevelCorrecting`,
`UnrecognizedTagName`, `UselessTextInParameter`, `UselessTextInRedirect`). Warnings are recorded only when the
parse finished.

### Statuses

Every page version gets exactly one (`page.rs`, `process_with`). A failure carries a status, never silence.

| Status | When | Component rows | `status_detail` |
|---|---|---|---|
| `ok` | parsed and walked | from the parser | null |
| `redirect` | the text's first node is a redirect (`#перенаправление [[X]]`, `#REDIRECT`) | none | null; `redirect_target` set |
| `null_input` | the text is null | none | null |
| `empty` | the text is empty or only whitespace | none | null |
| `fallback` | the parse or the walk failed and the fallback scanner ran (the default) | from the scanner, `fallback = true` | the original failure: `budget: ...`, `depth_limit: ...` or `panic: ...` |
| `budget` | with `--no-fallback`: the parse exceeded its step budget (`--budget`, default 2,700,000,000), or the walk its node budget (50,000,000) | none | `budget: parse exceeded B steps (used S)`, `budget: walk exceeded its budget (used N nodes)` |
| `depth_limit` | with `--no-fallback`: templates, tags and links nested deeper than 400 | none | `depth_limit: nesting deeper than the cap (depth D)` |
| `panic` | with `--no-fallback`: the parser or the walk panicked; with the fallback, the scanner itself panicked | none | `panic: <message>`, or `panic: fallback scan panicked: <message> (after <original failure>)` |
| `hang` | the watchdog quarantined the page in an earlier run (over 10 minutes of wall time) and `--resume` met it in `quarantine.list` | from the scanner, unless `--no-fallback` | `hang: over the watchdog limit in an earlier run (quarantine.list)` |

`hang` is the one status that can depend on the machine, and it should never occur: the budgets bound all the
work. A page that runs past the watchdog limit stops the run (its `source_file`, `row` and `column` go to
`<out>/quarantine.list`), and `--resume` then gives it `hang`.

## timings

| Column | Type | Meaning |
|---|---|---|
| *(keys)* | | the six key columns |
| `parse_ms` | float64 | wall time of the parse, in milliseconds |
| `walk_ms` | float64 | wall time of the walk, in milliseconds; 0 when there was none (`redirect`, a failed parse) |

One row per page version, keyed like `pages`. Both are 0 for `null_input`, `empty` and `hang`. The fallback
scan is not timed. This is the one table that differs between runs, and it is excluded from every
reproducibility check.

## sections

The lead (id 0) plus one section per heading (`walk/sections.rs`). Every heading opens a section wherever the
parser makes one: in running text and in table cells (the vendored parser makes headings nowhere else).

| Column | Type | Meaning |
|---|---|---|
| `level` | int32 | the heading's level, the number of `=` (1-6); 0 for the lead |
| `title_text` | string | the heading's text under the paragraph text rules (templates, refs and the rest excluded); `""` for the lead |
| `title_raw` | string | the source between the opening and the closing `=` runs, trimmed; `""` for the lead |
| `title_norm` | string | `norm(title_text)` |
| `path` | list\<string\> | the `title_norm`s from the outermost ancestor below the lead down to the section itself; empty for the lead |
| `parent_section_id` | int32 | the nearest earlier section with a smaller `level`; 0 (the lead) for a top-level section and for the lead itself |
| `heading_start`, `heading_end` | int32, null | the heading's byte span; null for the lead |
| `subtree_end` | int32 | the byte where its subtree ends: the start of the next heading whose `level` is the same or smaller, else the end of the text |
| `role` | string | the section registry's role of `title_norm` (`config/registry/sections.toml`): `notes` (`примечания`, `комментарии`), `bibliography` (`литература`, `источники`), `external_links` (`ссылки`), `see_also` (`см. также`); else `content` |

A section's `start..end` runs from its heading's start to the next heading's start (the lead from 0), or to the
end of the text. `fp` hashes the path. Pages with status `redirect`, `null_input` or `empty` have no sections,
not even a lead.

## paragraphs

The page's own text, built while walking the tree (`walk/text.rs`). Not the legacy text.

| Column | Type | Meaning |
|---|---|---|
| `kind` | string | `prose`, `list_item`, `definition_term`, `definition` or `preformatted` |
| `list_id` | int32, null | the list the item belongs to; the lists that yield at least one paragraph row, counted densely from 0 in document order; null for `prose` and `preformatted` |
| `item_depth` | int32, null | the item's list nesting depth, 1 for an outermost list; null for `prose` and `preformatted` |
| `text` | string | the paragraph's text, never empty |
| `n_chars` | int32 | code points in `text` |

**What adds text.**
- Whitespace runs (`char::is_whitespace`, so a decoded `&nbsp;` too) collapse to one space, and the text is
  trimmed. Offsets (`para_offset`, `n_chars`) count code points of the collapsed text.
- Plain text, and character entities decoded. A free URL in running text stays, as MediaWiki displays it.
- Bold and italic markers add nothing; the text between them is added.
- A link adds its label, or without one its target with any leading colon and namespace prefix removed; then its
  link trail (the site's letters: Latin and Cyrillic on both sites). An interlanguage link (a language prefix
  and no leading colon, `[[en:Moscow]]`) adds nothing; interwiki (`[[w:Москва|Москва]]`) and leading-colon links
  add text like any link.
- An external link adds its label (the text after the URL and the first whitespace), or nothing without one
  (`[https://example.org]`).
- HTML tags (`<span>`, `<sup>`, ...) add nothing but the text between them; `<br>` adds one space.
- Extension tags: `nowiki` adds its raw text, `poem` its content. `ref`, `references`, `math`, `chem`, `ce`,
  `gallery`, `templatestyles`, `score`, `timeline`, `graph`, `mapframe`, `maplink`, `syntaxhighlight`, `source`,
  `templatedata`, `imagemap`, `inputbox`, `categorytree`, `hiero`, `indicator` and `includeonly` add nothing. Any
  other extension tag adds its content, `noinclude` and `onlyinclude` among them.
- Excluded (adds nothing): templates (no template has a render rule in 1.0.0), template parameters (`{{{1}}}`),
  files, categories, comments, magic words, tables and the extension tags above. Excluded content makes no
  paragraphs.
- Headings add nothing to paragraphs; their text is the section's `title_text`.

**Where paragraphs break.** At an empty line, at a heading (wherever it is), at a horizontal divider (`----`),
and at the start and end of a list, a table or a preformatted block. A preformatted block is its own paragraph.
Each list item is its own paragraph, and a nested list ends the text of the item around it. A paragraph whose
text is empty is dropped and takes no id.

**Spans.** A paragraph's `start..end` runs from its first node to its last, excluded nodes included, leading and
trailing whitespace not. A list item's span starts at its own marker (the `*`, `#`, `;` or `:` of its level) and
ends with the item, or where its first nested list begins. Spans lie inside the text and never overlap.

## templates

One row per template invocation, `{{...}}`, in document order, at any depth (`walk/templates.rs`). Templates are
recorded, never expanded.

| Column | Type | Meaning |
|---|---|---|
| `name_raw` | string | the source of the whole name part, trimmed |
| `name` | string | the normalised name (see Fingerprints and normalisations) |
| `kind` | string | `template`, `parser_function`, `module` or `variable` (below) |
| `role` | string | the registry role of `name` on the row's site (`config/registry/templates.toml`): `citation`, `footnote`, `ref_list`, `link_status` or `other` in 1.0.0; `other` for a template the registry does not list |
| `looks_like_infobox` | bool | reserved for the infobox heuristic of milestone 3; always false in 1.0.0 |
| `depth` | int32 | the number of templates around it (0 at the top level) |
| `rendered_len` | int32 | code points the template adds to paragraph text; always 0 in 1.0.0 (no render rules yet) |
| `params` | list\<struct\> | the parameters, in source order, duplicates kept |

`params` elements: `name` string, `positional` bool, `value_raw` string, `value_text` string, `start` int32,
`end` int32.
- Positional parameters are named `1`, `2`, ... in order. `N=` names a parameter `N` and does not advance the
  count (as MediaWiki does). A named parameter's `name` is the trimmed source of its name, case and inner spacing
  kept; `positional` is false.
- `value_raw` is the trimmed source of the value; `value_text` is the value under the paragraph text rules (so a
  link's label, no nested templates), collapsed.
- `start..end` is the parameter's span as the parser reports it: the name and `=` included, the whitespace
  around the parameter (after its `|`, before the next `|` or the closing `}}`) not.

`kind` (`Templates::enter`), decided on the name's own text before its first colon (the *head*):
- `template` when the head names the template namespace (`{{Шаблон:PAGENAME}}` transcludes a page);
- `module` when the head is `#invoke`;
- `parser_function` when the name starts with `#` or the head is one of the site's parser functions (`lc`,
  `formatnum`, `ns`, ...);
- `variable` when the name is one of the site's variables (`PAGENAME`, `CURRENTYEAR`, `!`, `=`, ...);
- `template` otherwise.

For a module or a parser function with a colon, the text after the colon is positional parameter `1`
(`value_raw` and the span trimmed, `value_text` by the text rules), and the `|` parameters count from `2`:
`{{#invoke:Module|func|x}}` has `1` = `Module`, `2` = `func`, `3` = `x`.

## refs

One row per `<ref>` tag and per footnote template (registry role `footnote`), in document order
(`walk/refs.rs`). A footnote template is also a `templates` row: two views of one node, with the same span.

| Column | Type | Meaning |
|---|---|---|
| `kind` | string | `inline`, `named`, `reuse`, `list_defined` or `template` (below) |
| `name` | string, null | the ref's name; null when it has none or it is empty |
| `group` | string | the ref's group; `""` for the default group |
| `body_raw` | string | the body's source, trimmed; `""` for a reuse, a self-closing tag, and a body of only whitespace and comments |
| `body_text` | string | the body under the paragraph text rules; `""` when `body_raw` is |
| `def_ref_id` | int32, null | for a named ref: the `id` of the definition of its (group, name), the definition itself included; null for an unnamed ref and when no definition exists |
| `n_uses` | int32, null | on `named` and named `list_defined` refs: the refs of its (group, name), the definition and everything that points to it; null on every other row |
| `resolved` | bool | see Resolution |
| `harv_key` | string, null | for a short footnote, its `CITEREF` key; for a ref whose body holds a citation of family `short`, that citation's key; else null |
| `cites_citation_id` | int32, null | the citation a short footnote points to (see Resolution); else null |

**Tag refs.** `name` and `group` come from the open tag's attributes (`attrs::tag_attrs(attrs::open_tag(..))`):
names lowercased, values unquoted, entity-decoded, ASCII whitespace collapsed and trimmed; of a name or group
given twice the last counts, as in MediaWiki. `body_raw` is the source between the open tag's first `>` and the
closing `</ref>`.

**Kinds.**
- `list_defined`: the nearest enclosing ref or ref list is a ref list (a `<references>` tag, or a ref-list
  template, in any parameter and however deep in other templates). It takes the list's `section_id`, and the
  list's group unless it names its own.
- Otherwise a named ref with a body is `named`; a named ref without one is `reuse` (also `<ref name=a></ref>`,
  which MediaWiki reads as `<ref name=a/>`); an unnamed ref is `inline`.
- `template`: a footnote template (`{{sfn}}`, `{{sfnp}}`, `{{efn}}`, `{{комментарий}}`).
  - A short footnote (registry `family = "short"`, `{{sfn}}`): `body_raw` is the source of all its parameters,
    trimmed (after the name's `|`, before `}}`), and `body_text` their `value_text`s joined by one space in
    source order, a named one as `name=value_text`, an empty positional value skipped (a named one with an empty
    value stays as `name=`). So `{{sfn|Иванов|1990|с=5}}` and `{{sfn|Иванов|2001|с=7}}` differ, and
    `{{sfn|Иванов||1990}}` has the `body_text` of `{{sfn|Иванов|1990}}`.
  - Any other footnote template: its parameter `1` (the last so named): `body_raw` is its `value_raw`,
    `body_text` its `value_text`.
  - `name` and `group` come from its `name=` and `group=` parameters (the last of each), with one pair of
    surrounding quotes removed (`{{efn|group="К"}}` is group `К`). Without `group=`, the group is the registry
    entry's `default_group` (`К` for `{{efn}}`), else `""`.

**Resolution**, per (group, name) over the whole page, once the page is known (`Refs::finish`,
`refs::link_short`).
- A ref *has a body* when it is not a `reuse` and its `body_raw` is not empty. The first named ref with a body,
  in document order, is the definition of its (group, name); a later one keeps its kind and is not a second
  definition. A reuse may come before its definition.
- `resolved` is true for a ref with a body and for a ref whose (group, name) has a definition; false otherwise
  (a reuse of a name defined nowhere).
- For a short footnote, and for a ref whose body holds a citation of family `short`, `resolved` is instead
  whether `cites_citation_id` was found: the lowest-id citation of the page, not of family `short`, whose
  `harv_key` equals the ref's.

## ref_lists

Each `<references>` tag and each ref-list template (registry role `ref_list`: `{{примечания}}`, `{{reflist}}`,
`{{список примечаний}}`, `{{notelist}}`, `{{комментарии}}`).

| Column | Type | Meaning |
|---|---|---|
| `group` | string | a tag's `group` attribute; a template's `group=` parameter (unquoted as for refs), else its entry's `default_group` (`К` for `{{комментарии}}`), else `""` |
| `kind` | string | `tag` or `template` |
| `template_id` | int32, null | the `templates` row of a ref-list template; null for a tag |
| `n_entries` | int32 | the definitions in its group on the whole page: refs with a body that are not a later copy of an earlier definition; identical short footnotes (same group and `norm(body_text)`) count once, as MediaWiki merges them |

A template list is a ref list and a `templates` row with the same span. Refs inside a `<references>` tag have
the tag as parent (`ref_list`); refs inside a ref-list template have the template as parent.

## citations

One row per source as written in the markup (`walk/citations.rs`). Counting by source means counting
`citations`; counting by use means joining `refs` to `citations` through `ref_id`, `def_ref_id` and
`cites_citation_id`. Neither inflates the other.

| Column | Type | Meaning |
|---|---|---|
| `context` | string | `in_ref` when `ref_id` is set; outside refs `bibliography` in a section of role `bibliography` or `external_links`, else `other` |
| `ref_id` | int32, null | the ref whose body holds it; null outside refs |
| `family` | string | `web`, `news`, `book`, `journal`, `encyclopedia`, `other_template` or `short` from a citation template's registry entry (`other_template` when the entry names none); `bare_link`; `plain_text` |
| `template_id` | int32, null | the citation template's `templates` row (a standalone archive wrapper's own); null for `bare_link` and `plain_text` |
| `template_name` | string, null | that template's `name` |
| `params` | list\<struct\> | that template's parameters, as in `templates`; empty for `bare_link` and `plain_text` |
| `url` | string, null | the original URL (see URLs) |
| `archive_url` | string, null | the archive copy's URL |
| `archive_date_raw` | string, null | the archive date as written |
| `host` | string, null | the host of `url` |
| `registrable_domain` | string, null | the registrable domain of `host` |
| `dead_link` | bool | a `link_status` template (`{{недоступная ссылка}}`, `{{dead link}}`) sits in the same body, or the citation template's own dead-link parameter says the URL is dead (see Fields) |
| `title` | string, null | the title; for a `bare_link`, the link's label (null without one, and for a free URL) |
| `authors` | list\<string\> | the authors, in source order |
| `date_raw` | string, null | the date as written |
| `year` | int32, null | the year (below) |
| `publisher`, `work`, `language`, `isbn`, `doi`, `access_date_raw` | string, null | as written; `work` is the website, newspaper, journal or издание |
| `harv_key` | string, null | the `CITEREF` key (see Fingerprints and normalisations) |
| `text` | string, null | for `plain_text`, the ref's `body_text`; null otherwise and when it is empty |

**Bodies.** Rows are grouped by *body*: the inside of a ref, or outside refs the stretch of the page in one
section and paragraph (a bibliography entry, a list item).
- A short footnote (`{{sfn}}`) has no body: its arguments point at a source, they are none, and it yields no
  row; it links through `cites_citation_id`. A ref without a body (a reuse, an empty tag) yields no row: it links
  through `def_ref_id`.
- Every citation-role template that is not an archive wrapper is one row, unless it sits inside another such
  template's parameter. A citation template inside a footnote template (an `{{efn}}`) is `in_ref`, with that
  footnote's ref.
- Inside a ref, every external link that is not inside a citation template is one `bare_link` row (`url` from
  the link, `title` from its label). Outside refs, bare links are no rows.
- So is every free URL that MediaWiki would link, in a ref's body and not inside a citation template, an archive
  wrapper or an external link: `url` through the same URL analysis, so with `host` and `registrable_domain`, and
  no `title`. The rules are MediaWiki's (`Parser::handleMagicLinks`, `Parser::makeFreeExternalLink`;
  `autolink.rs`):
  - it starts at a word boundary (the character before is no letter, digit or `_`) with one of the site's URL
    protocols, in any case, but not the protocol-relative `//`; `mailto:` is one, and gives no host;
  - it runs over URL characters: anything but `[`, `]`, `<`, `>`, `"`, spaces and other space separators, control
    characters and U+FFFD. A comment inside is skipped and adds nothing to the URL. `''` and `{{` end it, and in
    a template parameter's value so do `|` and `}}`;
  - it ends before `&lt;`, `&gt;`, `&nbsp;` and their numeric forms, and leaves out trailing `,`, `;`, `.`, `:`,
    `!` and `?`, and a trailing `)` when the URL has no `(` (`(см. http://a.ru/x)` gives `http://a.ru/x`); a `;`
    that ends a character entity (`&amp;`) stays;
  - with nothing after the protocol, there is no URL.

  Text inside `<nowiki>`, `<pre>`, links, file links and tag attributes has no free URLs; a free URL in the
  parameter of a template that is no citation template (`{{lang-en|http://a.ru}}`) counts, as a bracketed link
  there does. A `<pre>` hides the free URLs up to its `</pre>` only when that closing tag comes before the end
  of the ref (or other extension tag) the `<pre>` is in, or before the end of the page outside one: as MediaWiki's
  preprocessor reads it, an unclosed `<pre>` and a self-closing `<pre/>` are text and hide nothing.
- A ref with a body and no other row yields one `plain_text` row, at the ref's span with the ref as parent. So
  "refs without an identifiable source" is countable rather than missing.
- An archive wrapper (`{{webarchive}}`, `{{wayback}}`, `{{архивировано}}`) merges into the nearest preceding row
  of its body when that row has no `url` or the wrapper's original is exactly the row's `url` (as the URL
  analysis gives both: `http://a.ru/x` and `http://a.ru/x/` differ, so a trailing slash makes two rows): it
  sets `archive_url` (the wrapper's URL when that is an archive URL, else
  `https://web.archive.org/web/<date>/<original>`, and none without a date) and `archive_date_raw`, and gives the
  row its original as `url` when it has none. A row that
  already has an archive keeps it. A wrapper inside a citation template's parameter merges into that template's
  row. With nothing to merge into, the wrapper is a row of its own (its entry's family, `web`) with its original
  as `url`.

**Fields** of a template row, through the registry entry's field map (`fill_from_template`).
- `url` and `archive_url` come from the parameter's `value_raw` through `urls::analyse`; every other field from
  `value_text`. When the `url` parameter is itself an archive URL, its original is `url` and it is
  `archive_url`. An `archive_url` parameter that is no archive URL is kept, entity-decoded and trimmed.
- A scalar field takes the first parameter name in the entry's list that has a non-empty value; of several
  parameters of one name, the last non-empty one. Names match exactly (case-sensitive, after trimming).
- `authors` collects, in source order, every parameter whose name without trailing digits is a base of the
  entry's `authors` list. A `lastN` comes with the `firstN` of its number as `"last, first"`; a `first` alone
  adds nothing.
- `year` is the first run of exactly four digits between 1000 and 2100 in the `year` field, else in `date_raw`.
- `dead_link` is also true when the entry's `url_status` field (the CS1 parameters `url-status`, `deadlink` and
  `deadurl`, read as a scalar field) has a `value_text` that, trimmed and lowercased, is `dead`
  (`url-status=dead`), or, for `deadlink` and `deadurl`, one of `yes`, `y`, `true` and `да` (`deadlink=yes`).
  Every other value leaves it as the body's link-status templates set it: `live`, `no`, `unfit`, `usurped`,
  `deviated`, and also an HTTP status code (`deadlink=404`).
- A row from a template has the template's span and parent; a `bare_link` row the link's span and parent (a free
  URL's from its protocol to its last character, a comment inside included); a `plain_text` row the ref's span. All are placed where the walk met them. Ids follow the document order of
  their sources.

## The fallback path

When the parse or the walk of a page fails, the fallback scanner (`fallback.rs`) reads the raw wikitext in one
linear pass and the page gets status `fallback`. It emits the same tables through the same builders
(`walk/collect.rs`), so the rules above hold, with these differences:
- It emits sections, templates, refs, ref lists and citations only: no paragraphs. `para_id` and `para_offset`
  are null on every row, and `n_paragraphs` is 0.
- Every row has `fallback = true`.
- Templates are found by brace matching, tags by name (classed as the parser classes them), headings line by
  line outside templates, refs and every other element.
- `value_text`, `body_text` and `title_text` are a linear strip of the source: comments, templates,
  `{{{parameters}}}`, refs, ref lists, `<math>` and the excluded tags removed; `<nowiki>` and `<pre>` keep their
  raw content; `[[a|b]]` is `b`, `[[a]]` is `a` (files, categories and interlanguage links removed); `[url label]`
  is `label`; `''` and `'''` removed; HTML tags removed, keeping their text (`<br>` a space); entities decoded;
  magic words removed; whitespace collapsed. On well-formed markup this gives the parser's values, so `fp`s agree
  between the two paths.
- Past 400 open elements nothing more opens, but closers still pair, so the outer elements are found.

## manifest.json

Written at the end of a run (`driver::Manifest`).
- `schema_version`, `crate_version`, `git_commit` (from the build: `GIT_COMMIT`, else `git rev-parse --short
  HEAD`, else `unknown`);
- `vendored_upstream` (the vendored parser's upstream commit), `vendored_patches` (the patch titles of
  `vendor/parse-wiki-text-2/VENDORED.md`), `vendored_src_sha256`;
- `settings_hashes` (sha256 of each site's settings file, by site), `registry_hash` (sha256 of `templates.toml`
  followed by `sections.toml`), `args`;
- `inputs` (path, input rows, page versions, whether `--resume` skipped it), `outputs` (path relative to
  `--out`, rows), `failed_inputs` (path, message: an input that could not be read to its end; its parts stay
  `.tmp`, the run goes on with the next input, and the process exits non-zero);
- `status_counts` (site, then version, then status: page versions);
- `pages_per_second` (page versions parsed in this run over the run's wall time), `seconds_over_budget` (wall
  time spent on pages whose parse or walk exceeded a budget).

## Not in 1.0.0

- **Categories, refs and links that only templates produce are invisible.** Templates are never expanded, so a
  ref, a citation or a category that a template adds when MediaWiki renders the page is not in the output. This
  is a documented limitation, not a bug.
- No template adds text to paragraphs yet (`rendered_len` is 0), so the text around a template has a gap where
  MediaWiki would show the template's output (a date, a number, a flag). `looks_like_infobox` is always false.
  Roles other than `citation`, `footnote`, `ref_list` and `link_status` are `other`.
- Links, categories, files, tables, table cells and lists have no tables yet. Table content is excluded from
  paragraphs, so the text of a table is in no table of 1.0.0.
- The fallback scanner makes no paragraphs.

## Known limitations

- A table written with `{{!}}` for its pipes can come out after its own content, and its rows leak into prose
  (a quirk of the vendored parser).
- The vendored parser reads `<pre>` content as wikitext: components and headings inside `<pre>` are rows on the
  parsed path only, though MediaWiki shows `<pre>` literally (the fallback path skips it).
- On the parsed path, a `value_raw` or `name_raw` that ends in a parameter with an empty default (`{{{1|}}}`) is
  cut short at `{{{1|`; the fallback path has it right.
- A redirect preceded by a byte-order mark, a tab or `\r` is not seen as a redirect: status `ok`.
- Magic variables are matched in any case (`{{pagename}}` is kind `variable`), although MediaWiki treats most of
  them as case-sensitive and would transclude a template.
- A leading colon in a template name (`{{:X}}`, `{{:Шаблон:X}}`) is kept in `name`, and a namespace after it is
  not stripped.
- A template whose title contains `:` cannot be given a registry entry (names with `:` are rejected), so its role
  is `other`; nor can a name with an HTML comment inside (`cite web<!-- -->`), which normalisation does not strip.
- Footnote templates without `group=` take the registry's `default_group`, which comes from page-layout evidence
  (on the corpus pages, `{{efn}}` notes are listed under `{{комментарии}}`, so `{{efn}}` gets group `К`), not
  from the templates' expansion, which cannot be checked offline.
- A free URL right after a link (`[[Статья]]http://a.ru/x` in a ref) is a `bare_link` on the fallback path only:
  the parsed path, as MediaWiki does, reads the Latin letters `http` as the link's trail, which leaves no protocol.
- A comment directly before a protocol (`x<!-- c -->http://a.ru/x` in a ref) does not stop the free URL on either
  path, although MediaWiki, which removes comments first, would read `xhttp://…` and link nothing.
- A bare `archive.is` (`archive.today`, `archive.ph`, `archive.li`) link gets no `archive_date_raw`, though its URL
  carries the date.
- `ref=` together with a year gives the key `CITEREF<ref><year>`, while CS1 uses `ref=` literally as the anchor;
  `{{sfn}}` calls that point at such a citation stay unresolved (10 of the 16 unresolved `{{sfn}}` calls of the
  public corpus).
- `authors` does not apply MediaWiki's last-wins rule to a repeated parameter: an author given twice appears
  twice.
- Outside refs, citations with no paragraph (in table cells) share one body per section, so a
  `{{недоступная ссылка}}` in one cell marks every such citation of that section `dead_link`.
- URL edge cases: in a `webcitation.org` `url=` query a `+` becomes a space; `&#0;` decodes to NUL and `&#x+41;` is
  accepted; an archived original `ftp:/x` or `mailto:x` gets an `http://` prefix.
- In the registry, `site` may only be `wikipedia` or `ruwiki`, so another wiki's site-scoped entries need a code
  change.
- `--resume`: stale `.tmp` parts are never removed, a stale `manifest.json` stays after a watchdog abort, and a
  resumed manifest mixes scopes (`seconds_over_budget` and `pages_per_second` count this run, `status_counts`
  the whole output directory).
- `--resume` with `--format json` reads each finished `.jsonl` whole into memory to count its statuses.
- `quarantine.list` is matched on the raw line: a line edited by hand (the row written as `07`, trailing spaces,
  a `\r`) no longer matches its page, which then runs again.
- A short footnote whose parameters are all empty (`{{sfn||}}`) has a `body_raw`, so it counts as a definition in
  its list's `n_entries`, although its `body_text` is empty.
- Reading streams one row group at a time with no column projection, so memory is bounded per row group, not
  per batch.
