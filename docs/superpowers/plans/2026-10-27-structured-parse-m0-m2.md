# Structured Parse, Milestones 0–2 (first release) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lock the v0.1.0 prose output as a tested baseline. Then build `parse_components`, a parser that streams
Parquet and emits pages, sections, paragraphs, templates, refs, ref lists and citations with their positions. The
release freezes schema 1.0.0.

**Architecture:** A new library (`src/components/`) and binary (`src/parse_components.rs`) sit next to the
untouched legacy binaries. Only `frontend.rs` and `walk/` see the vendored `parse-wiki-text-2` crate, which is patched
with a counted step budget. Everything else works on our own row structs. These rows serialise to Parquet (via
`serde_arrow`) and to JSON (via `serde_json`).

**Tech Stack:** Rust (edition 2021, toolchain 1.99), arrow/parquet 53.3, vendored parse-wiki-text-2 at `bc6bde5`, serde,
serde_arrow 0.15 (`arrow-53`), serde_json, toml 1, rayon 1, xxhash-rust 0.8 (`xxh3`), psl 2, sha2 0.11, similar 3.

**Spec:** `docs/superpowers/specs/2026-10-27-structured-parse-design.md`. Milestones 3–7 get their own plan after this
release.

## Global Constraints

- Legacy files never change: `src/parser.rs`, `src/main.rs`, `src/clean_parsed.rs`, `src/parse_single.rs`,
  `src/export_parsed.rs` and the `*.sh` scripts. The legacy path keeps `parse_wiki_text = "0.1"`.
- Ruwiki markup never enters git, and neither does anything that reproduces it: expected outputs, snapshots or
  facts. Wikipedia markup may enter git (CC BY-SA, attributed). The private corpus is the file named by
  `WIKITEXT_PRIVATE_CORPUS`, by default
  `../ruwiki_replication_package_sources/data/results/sources/test_corpus/parser_test_corpus_v1.parquet`
  (relative to the repository root).
- Offline: no build, test or run contacts a website. The one exception is the performance-sample download in
  Task 10, and only after the user approves the stated size.
- Same input, same build, same bytes, except the `timings` table. Rows are in input order, then in document order.
- Column types: no unsigned 64-bit integers, and no Arrow map type. Fingerprints are 16 lowercase hex characters.
- Every page version yields exactly one `pages` row. A failure carries a status, never silence.
- Run tests with `cargo test --release` (corpus pages up to 260,000 characters are slow in debug).
- `SCHEMA_VERSION = "1.0.0"`. It is frozen at Task 21. Until then, `tests/schema/v1.json` is regenerated whenever
  a table changes.
- Corpus labels (`--label` for the long format): `wiki_forkday=wikipedia:forkday`,
  `wiki_2025=wikipedia:2025-01-01`, `deleted_wiki_2025=wikipedia:2025-01-01`, `ruwiki_2025=ruwiki:2025-01-01`.
- Private corpus v1 sha256: `9800a5a3a11feee64af7bf2ebcaf44d281ea59619b82f81af1a67835ef263d06`.
- `pages.timestamp` is always a string: input strings pass through as they are, and Arrow timestamps become
  RFC 3339 UTC (`2024-12-28T01:38:34Z`). This keeps the schema fixed across inputs and is my refinement of the
  spec's "input type".

## Review Focus

These are the inputs most likely to bite a user that the spec implies but no task's main tests hit. Each line's
test lives in the task named.

1. **A `<ref>` inside a template parameter** (`{{Карточка|население=1<ref>Источник</ref>}}`). The ref must be found,
   with `parent_type = "template"`. Test in Task 15.
2. **Template name variants**: `{{Cite_web}}`, `{{ cite web }}`, `{{Шаблон:Cite web}}`, `{{Ш:cite web}}` and a
   registry alias all resolve to the `cite web` entry. Test in Task 14.
3. **URL forms**:
   - IDN host (`https://Президент.рф/a` gives `президент.рф`);
   - protocol-relative (`//tass.ru/x` gives `tass.ru`);
   - upper case with a port (`http://WWW.Kremlin.RU:80/a` gives `kremlin.ru`);
   - `&amp;` in the query string, decoded.

   Test in Task 11.
4. **Ref name spelling**: `name="a"`, `name=a`, `name = 'a'` and `name=" a "` are one name, so reuses resolve.
   Test in Task 12, with resolution in Task 15.
5. **Duplicate `page_id` rows in one input file** (the time-series fetches have them). Both rows are parsed and
   kept apart by `row`. Test in Task 8.

---

## File Structure

```
Cargo.toml                         add [lib], [[bin]] parse_components, deps; Cargo.lock committed
.gitignore                         drop Cargo.lock; exceptions for the corpus, expected parquet, selected.tsv; /tests/private/
vendor/parse-wiki-text-2/          vendored fork + VENDORED.md (budget patch)
config/sites/ru.wikipedia.toml     site settings (Task 5)
config/sites/ruwiki.toml           copy of the above at start
config/registry/templates.toml     template roles, families, field maps (Task 14)
config/registry/sections.toml      section roles (Task 14)
src/components/lib.rs              module declarations
src/components/corpus.rs           corpus loading, hash check, public cut, legacy reshaping (Tasks 1, 3)
src/components/settings.rs         Settings (Task 5)
src/components/frontend.rs         the only module (with walk/) that sees the vendored crate's parse call (Task 4)
src/components/model.rs            row structs, SCHEMA_VERSION (Task 6)
src/components/page.rs             process_page: budgets, catch_unwind, status, fallback switch (Task 7)
src/components/input.rs            layouts, labels, PageInput stream (Task 8)
src/components/output.rs           Arrow schemas, Parquet part writers, JSON (Tasks 6, 9)
src/components/driver.rs           streaming, rayon, ordering, watchdog, resume, manifest (Task 9)
src/components/spans.rs            CharIndex (Task 11)
src/components/fingerprint.rs      fp + normalisation (Task 11)
src/components/urls.rs             UrlInfo, archive unwrapping (Task 11)
src/components/attrs.rs            tag attribute lexer (Task 12)
src/components/registry.rs         Registry (types Task 7, loading Task 14, coverage Task 15)
src/components/walk/mod.rs         walk(): orchestration, budget/depth (Task 13)
src/components/walk/text.rs        paragraph text builder with offset map (Task 13)
src/components/walk/sections.rs    sections + paragraphs (Task 13)
src/components/walk/templates.rs   templates table (Task 13b)
src/components/walk/refs.rs        refs, ref lists (Task 15)
src/components/walk/citations.rs   citations, archive merge, sfn (Tasks 16, 17)
src/components/fallback.rs         linear scanner (Task 18)
src/components/compare.rs          symmetry + locality (Task 19)
src/parse_components.rs            CLI (Task 9)
examples/cut_public_corpus.rs      Task 1
examples/reshape_legacy.rs         Task 3
examples/collect_legacy.rs         Task 3
examples/measure.rs                Task 10
tools/gen_legacy_expected.sh       Task 3
tests/common/mod.rs                corpus access, fixtures, run_page helper
tests/corpus/wikipedia_v1.parquet, wikipedia_v1.sha256, ATTRIBUTION.md
tests/fixtures/*.wiki              hand-written snippets
tests/expected/legacy_wikipedia_v1.parquet, legacy_private.sha256
tests/expected/structured/*.json, structured.sha256, structured_private.sha256, selected.tsv
tests/facts.toml
tests/schema/v1.json
SCHEMA.md
```

---

## Milestone 0: lock the baseline

### Task 1: Lockfile, library skeleton, corpus access

**Files:**
- Modify: `.gitignore`, `Cargo.toml`
- Create: `src/components/lib.rs`, `src/components/corpus.rs`, `examples/cut_public_corpus.rs`, `tests/common/mod.rs`,
  `tests/corpus_access.rs`, `tests/corpus/wikipedia_v1.parquet`, `tests/corpus/wikipedia_v1.sha256`,
  `tests/corpus/ATTRIBUTION.md`

**Interfaces:**
- Produces, in `wikitext_components::corpus`:
  - `pub struct CorpusRow { pub page_id: String, pub page_title: String, pub version: String, pub timestamp: String, pub wikitext: String, pub n_chars: i64, pub case: Option<String> }`;
  - `pub fn load(path: &Path) -> anyhow::Result<Vec<CorpusRow>>`, which reads either the v1 file (`why`
    becomes `case`: `"largest"` when `why == "largest"`, `"slow"` when `why` contains `"slow"`, otherwise `None`)
    or the public file (`case` column);
  - `pub fn sha256_file(path: &Path) -> anyhow::Result<String>`;
  - `pub const PRIVATE_V1_SHA256: &str`;
  - `pub const LABELS: [(&str, &str, &str); 4]`, holding (corpus version, site, version label);
  - `pub fn write_public(rows: &[CorpusRow], path: &Path) -> anyhow::Result<()>`, which writes only the rows whose
    version does not start with `ruwiki`, with the columns `page_id, page_title, version, timestamp, wikitext,
    n_chars, case` and zstd compression.
- Produces, in `tests/common/mod.rs`:
  - `public_corpus() -> Vec<CorpusRow>`;
  - `private_corpus(test: &str) -> Option<Vec<CorpusRow>>`. It reads the file named in Global Constraints,
    checks `PRIVATE_V1_SHA256`, and keeps only the `ruwiki_2025` rows. When the file is absent, it prints `SKIPPED (private corpus absent): {test}` to stderr
    and returns `None`. When the hash differs, it panics.
  - `fixture(name: &str) -> String`, which reads `tests/fixtures/{name}.wiki`.

- [ ] **Step 1: Repository hygiene.**
  - In `.gitignore`, remove the `Cargo.lock` line and append `!tests/corpus/*.parquet`,
    `!tests/expected/*.parquet`, `!tests/expected/structured/selected.tsv` and `/tests/private/`.
  - In `Cargo.toml`, add `[lib] name = "wikitext_components" path = "src/components/lib.rs"`, plus the
    dependencies `sha2 = "0.11"`, `serde = { version = "1", features = ["derive"] }`, `serde_json = "1"`.
    Create `src/components/lib.rs` holding `pub mod corpus;`, and an empty `src/components/corpus.rs`.
  - Run `cargo build --release`. Expected: all four legacy binaries build. Then `git add Cargo.lock`.
- [ ] **Step 2: Write the failing tests** in `tests/corpus_access.rs`:

```rust
#[test] fn public_corpus_has_only_wikipedia_versions() {
    let rows = common::public_corpus();
    assert_eq!(rows.len(), 180);
    assert!(rows.iter().all(|r| !r.version.starts_with("ruwiki")));
    let pages: std::collections::BTreeSet<_> = rows.iter().map(|r| &r.page_id).collect();
    assert_eq!(pages.len(), 105);
    assert!(rows.iter().all(|r| matches!(r.case.as_deref(), None | Some("slow") | Some("largest"))));
}
#[test] fn public_corpus_hash_matches_committed() {
    let want = std::fs::read_to_string("tests/corpus/wikipedia_v1.sha256").unwrap();
    assert_eq!(corpus::sha256_file("tests/corpus/wikipedia_v1.parquet".as_ref()).unwrap(), want.trim());
}
#[test] fn private_corpus_is_ruwiki_only_or_skipped() {
    if let Some(rows) = common::private_corpus("private_corpus_is_ruwiki_only_or_skipped") {
        assert_eq!(rows.len(), 75);
        assert!(rows.iter().all(|r| r.version == "ruwiki_2025"));
    }
}
```

- [ ] **Step 3: Run the tests.** `cargo test --release --test corpus_access`. Expected: FAIL, because the
  public file is missing.
- [ ] **Step 4: Implement `corpus.rs` and `examples/cut_public_corpus.rs`.**
  - The example takes `<v1.parquet> <out_dir>`, checks the v1 hash, and writes `wikipedia_v1.parquet`, its sha256
    and `ATTRIBUTION.md`. The attribution has one line per public row:
    `page_id | title | version | revision timestamp | https://ru.wikipedia.org/?curid=<page_id>`, headed by the
    CC BY-SA 4.0 notice.
  - Run it with the v1 path given on the command line, not hard-coded.
- [ ] **Step 5: Run the tests.** `cargo test --release --test corpus_access`. Expected: PASS. With the private path
  configured, all three tests run. Without it, stderr shows the SKIPPED line.
- [ ] **Step 6: Commit.**
  `git add .gitignore Cargo.toml Cargo.lock src/components examples/cut_public_corpus.rs tests/common tests/corpus_access.rs tests/corpus`
  then `git commit -m "Lockfile, library skeleton, public test corpus and private corpus access"`.

### Task 2: Hand-written fixtures

**Files:**
- Create: `tests/fixtures/{refs_basic,refs_groups_listdefined,refs_reuse_order,refs_in_template,sfn,archive_deadlink,efn,sections_paragraphs,templates,malformed,redirect_ru,redirect_en}.wiki`

These are exact copy. Later tasks assert on them, so their content is fixed here.

- [ ] **Step 1: Write the fixtures** (UTF-8, with a trailing newline):

`refs_basic.wiki`
```
Текст<ref>Простой источник.</ref> и ещё<ref name="a">{{cite web |url=https://www.kremlin.ru/events/1 |title=Событие |website=Kremlin |date=2020-01-02}}</ref>. Повтор<ref name=a />.

== Примечания ==
{{примечания}}
```
`refs_groups_listdefined.wiki`
```
Факт<ref group="К">Комментарий.</ref>. Другой<ref name="b" />. Третий<ref name="c" />.

== Примечания ==
<references group="К" />
<references>
<ref name="b">{{книга |автор=Иванов И. И. |заглавие=Книга |место=М. |издательство=Наука |год=1990 |isbn=978-5-02-000000-0}}</ref>
</references>
{{примечания|refs=<ref name="c">[https://tass.ru/x ТАСС]</ref>}}
```
`refs_reuse_order.wiki`
```
Первое<ref name = 'x' />. Второе<ref name=" x ">Источник.</ref>. Третье<ref name="нет" />.
```
`refs_in_template.wiki`
```
{{Карточка города |название=Москва |население=13 000 000<ref>Росстат</ref>}}
Москва — город.
```
`sfn.wiki`
```
Утверждение{{sfn|Иванов|1990|с=12}}. Другое{{sfn|Петров|Сидоров|2001}}. Нет цели{{sfn|Никто|1900}}.

== Примечания ==
{{примечания}}

== Литература ==
* {{книга |автор=Иванов И. И. |заглавие=Книга |год=1990 |ref=Иванов}}
* {{cite book |last1=Петров |first1=П. |last2=Сидоров |first2=С. |title=Труд |year=2001}}
```
`archive_deadlink.wiki`
```
А<ref>{{cite web |url=https://web.archive.org/web/20200101000000/https://tass.ru/politika/1 |title=Т}}</ref>.
Б<ref>{{cite news |url=https://ria.ru/2020/a.html |title=Р |archive-url=https://web.archive.org/web/2021/https://ria.ru/2020/a.html |archive-date=2021-01-01}}</ref>.
В<ref>[https://www.bbc.co.uk/news/1 BBC] {{webarchive |url=https://web.archive.org/web/2019/https://www.bbc.co.uk/news/1 |date=2019}}</ref>.
Г<ref>{{статья |автор=А. Б. |заглавие=С |ссылка=http://old.example.org/x |издание=Журнал |год=2005}} {{недоступная ссылка}}</ref>.
Д<ref>{{wayback |url=http://gov.ru/a |date=20150101}}</ref>.
```
`efn.wiki`
```
Текст{{efn|Пояснение {{cite web |url=https://a.ru/p |title=A}}}}.

== Комментарии ==
{{notelist}}
```
`sections_paragraphs.wiki`
```
'''Москва''' — столица [[Россия|России]]. Город в [[Москва|Москв]]е[[Файл:Moscow.jpg|мини|200пкс|Вид]].

== История ==
Текст истории.

=== Ранние годы ===
* Пункт один
* Пункт два

== Примечания ==

[[Категория:Столицы]]
```
`templates.wiki`
```
{{Карточка города |название=Москва |население={{num|13000000}} |1=позиционный}}
Текст {{нп3|Тахмина Милани|Tahmineh Milani|en|Tahmineh Milani}} и {{#if:x|да|нет}}.
```
`malformed.wiki`
```
Начало<ref>{{cite web |url=http://a.ru |title=X</ref> текст {{незакрытый |a=b
{| class="wikitable"
| ячейка
<ref name="z">Конец
```
`redirect_ru.wiki`: `#перенаправление [[Москва]]`. `redirect_en.wiki`: `#REDIRECT [[Moscow]]`.

- [ ] **Step 2: Commit.** `git add tests/fixtures` then `git commit -m "Hand-written fixtures"`.

### Task 3: Legacy regression against v0.1.0

**Files:**
- Modify: `src/components/corpus.rs`
- Create: `examples/reshape_legacy.rs`, `examples/collect_legacy.rs`, `tools/gen_legacy_expected.sh`,
  `tests/legacy_regression.rs`, `tests/expected/legacy_wikipedia_v1.parquet`, `tests/expected/legacy_private.sha256`

**Interfaces:**
- Consumes `corpus::{load, CorpusRow}` and `common::{public_corpus, private_corpus}`.
- Produces, in `corpus`:
  - `pub fn single_fetch(rows: &[&CorpusRow]) -> RecordBatch`, with the columns
    `page_id, page_title, text, timestamp` (all Utf8);
  - `pub fn pair_fetch(pairs: &[(&CorpusRow, &CorpusRow)]) -> RecordBatch`, with the columns
    `page_id, page_title, official_text, official_timestamp, clone_page_title, clone_text, clone_timestamp`;
  - `pub fn legacy_pairs<'a>(rows: &'a [CorpusRow], official: &str, clone: &str) -> Vec<(&'a CorpusRow, &'a CorpusRow)>`,
    which returns, sorted by `page_id`, the pages that have both versions, leaving out a page when either
    version has `case == "slow"`;
  - the expected-output long format: `page_id, version, binary, stage, text`, with
    `binary ∈ {parse_single, wikitext_parser_rust, export_parsed}` and `stage ∈ {dirty, clean, file}`.

- [ ] **Step 1: Write the failing test** `tests/legacy_regression.rs`. It builds the inputs in a temporary
  directory, then runs the current binaries through `env!("CARGO_BIN_EXE_<name>")`, always with `--timeout 0`:
  - every non-slow public version through `parse_single`, then `clean_parsed`;
  - the public pairs (`wiki_forkday` as official, `wiki_2025` as clone) through `wikitext_parser_rust`, then
    `clean_parsed`, then `export_parsed`.

  It collects the outputs in the long format.

```rust
#[test] fn legacy_public_outputs_are_byte_identical_to_v010() {
    let got = run_legacy(&public_inputs());
    let want = load_expected("tests/expected/legacy_wikipedia_v1.parquet");
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(&want) { assert_eq!(g, w, "{} {} {} {}", w.page_id, w.version, w.binary, w.stage); }
}
#[test] fn legacy_private_outputs_match_committed_hashes() {
    let Some(rows) = common::private_corpus("legacy_private_outputs_match_committed_hashes") else { return };
    // pairs: wiki_2025 (public) as official, ruwiki_2025 (private) as clone; ruwiki versions also via parse_single
    let got = run_legacy(&private_inputs(&rows));
    assert_eq!(hash_lines(&got), std::fs::read_to_string("tests/expected/legacy_private.sha256").unwrap());
}
```

  `hash_lines` writes one line per output, sorted: `page_id\tversion\tbinary\tstage\t<sha256 of text>`.
- [ ] **Step 2: Run the test.** `cargo test --release --test legacy_regression`. Expected: FAIL, because the
  expected file is missing.
- [ ] **Step 3: Write the examples and the script.**
  - `examples/reshape_legacy.rs <public.parquet> [<private v1>] <out_dir>` writes `single_public.parquet`,
    `pair_public.parquet`, and, when the private file is given, `single_private.parquet` and `pair_private.parquet`.
  - `examples/collect_legacy.rs <run_dir> <out_dir>` reads the binaries' outputs. It writes the public long-format
    parquet to `tests/expected/`, the private hash lines to `tests/expected/legacy_private.sha256`, and the readable
    private copy to `tests/private/legacy_ruwiki_v1.parquet`.
  - `tools/gen_legacy_expected.sh` does, in order:
    1. `git worktree add "$TMP/v010" v0.1.0`;
    2. copy `Cargo.lock` into the worktree;
    3. `cargo build --release` there;
    4. run `reshape_legacy`;
    5. run the v0.1.0 binaries with `--timeout 0`;
    6. run `collect_legacy`;
    7. `git worktree remove "$TMP/v010"`.

    `$TMP` comes from `mktemp -d`. The private path is read the same way as in `common::private_corpus`.
- [ ] **Step 4: Generate.** Run `tools/gen_legacy_expected.sh`. Expected: it prints the row counts. The public file
  has a `parse_single` row (dirty and clean) for each of the 170 non-slow public versions, and a
  `wikitext_parser_rust` row (dirty, clean and file) for each version of each public pair.
- [ ] **Step 5: Run the tests.** `cargo test --release --test legacy_regression`. Expected: PASS for both tests, or
  PASS plus the SKIPPED line without the private corpus.
- [ ] **Step 6: Commit.**
  `git add src/components/corpus.rs examples/reshape_legacy.rs examples/collect_legacy.rs tools tests/legacy_regression.rs tests/expected`
  then `git commit -m "Legacy regression: v0.1.0 outputs as byte-identical baseline"`.

---

## Milestone 1: frontend and driver

### Task 4: Vendor the fork and patch in the step budget

**Files:**
- Create: `vendor/parse-wiki-text-2/` (the upstream tree at `bc6bde5cc463c57bfa38d6e9a27df49b4cf4a8a7`, without
  `.git` and CI files), `vendor/parse-wiki-text-2/VENDORED.md`, `src/components/frontend.rs`, `tests/frontend.rs`
- Modify: `vendor/parse-wiki-text-2/src/parse.rs`, `vendor/parse-wiki-text-2/src/configuration.rs`, `Cargo.toml`
  (`parse-wiki-text-2 = { path = "vendor/parse-wiki-text-2" }`)

**Interfaces:**
- The vendored crate gains:
  - `Configuration::parse_with_budget<'a>(&self, wiki_text: &'a str, max_steps: u64) -> Result<(Output<'a>, u64), ParseError<'a>>`.
    It counts one step per iteration of the main loop, and `max_steps == 0` means unlimited.
  - the variant `ParseError::BudgetExceeded { steps: u64, output: Output<'a> }`.
  - The existing functions keep their behaviour.
- Produces, in `frontend`:
  - `pub struct Parsed<'a> { pub output: parse_wiki_text_2::Output<'a>, pub steps: u64 }`;
  - `pub enum FrontendError { Budget { steps: u64 } }`;
  - `pub fn parse<'a>(text: &'a str, config: &parse_wiki_text_2::Configuration, budget: u64) -> Result<Parsed<'a>, FrontendError>`.

- [ ] **Step 1: Write the failing tests** in `tests/frontend.rs`, using `Configuration::default()` for now:

```rust
#[test] fn steps_are_deterministic() {
    let t = common::fixture("refs_basic");
    let a = frontend::parse(&t, &cfg(), 0).unwrap().steps;
    let b = frontend::parse(&t, &cfg(), 0).unwrap().steps;
    assert_eq!(a, b); assert!(a > 0);
}
#[test] fn budget_exceeded_is_reported_with_steps() {
    let t = common::fixture("refs_basic");
    match frontend::parse(&t, &cfg(), 10) { Err(FrontendError::Budget { steps }) => assert!(steps > 10), _ => panic!() }
}
#[test] fn budget_equal_to_need_succeeds() {
    let t = common::fixture("refs_basic");
    let need = frontend::parse(&t, &cfg(), 0).unwrap().steps;
    assert!(frontend::parse(&t, &cfg(), need).is_ok());
}
```

- [ ] **Step 2: Run the tests.** `cargo test --release --test frontend`. Expected: FAIL, because `frontend` does not
  exist yet.
- [ ] **Step 3: Copy the upstream tree and patch `parse.rs`.**
  - Generalise `parse()` to take `enum Limit { Duration(Duration), Steps(u64) }`.
  - Keep a `u64` step counter beside `loop_counter`. For `Steps(n)` with `n > 0`, return `BudgetExceeded` once the
    counter exceeds `n`, flushing first, as the timeout branch does.
  - Return the counter on success.
  - Record the upstream commit and this patch in `VENDORED.md`.
- [ ] **Step 4: Implement `frontend.rs`.** It is a thin wrapper.
- [ ] **Step 5: Run the tests.** `cargo test --release --test frontend`. Expected: PASS. Then run
  `cargo test --release --test legacy_regression` and expect PASS (the legacy path is untouched).
- [ ] **Step 6: Commit.** `git commit -m "Vendor parse-wiki-text-2 at bc6bde5 with a counted step budget"`.

### Task 5: Site settings

**Files:**
- Create: `config/sites/ru.wikipedia.toml`, `config/sites/ruwiki.toml`, `src/components/settings.rs`, `tests/settings.rs`
- Modify: `tests/common/mod.rs`

**Interfaces:**
- Consumes: `frontend::parse`.
- Produces: `pub struct Settings` with these methods:
  - `load(path: &Path) -> anyhow::Result<Settings>`;
  - `site(&self) -> &str`;
  - `configuration(&self) -> &parse_wiki_text_2::Configuration`, used only by `frontend` and `walk`;
  - `namespace(&self, prefix: &str) -> Option<i32>`, which matches names and aliases, case-insensitive on the first
    letter, with `_` as a space;
  - `is_language_prefix(&self, p: &str) -> bool` and `is_interwiki_prefix(&self, p: &str) -> bool`;
  - `is_image_option(&self, s: &str) -> bool`, true for `мини`, `thumb`, `справа`, `200px`, `200пкс`, `x120px`,
    `upright=1.2`, `альт=...` and `ссылка=...`;
  - `template_name(&self, raw: &str) -> String`, which strips the namespace prefix of ns 10, turns `_` into spaces,
    collapses and trims whitespace, and lowercases the first character only;
  - `is_variable(&self, name: &str) -> bool`, for `PAGENAME` and its Russian aliases;
  - `is_parser_function(&self, word: &str) -> bool`, for the function words before a colon (`lc`, `uc`,
    `formatnum`, `padleft`, …, and their Russian aliases);
  - `redirect_words(&self) -> &[String]`;
  - `hash(&self) -> String`, the sha256 of the file bytes.

  The TOML keys are `site`, `source`, `link_trail`, `[namespaces]` (id as a string mapped to a list of names),
  `extension_tags`, `html_tags`, `magic_words`, `redirect_magic_words`, `protocols`, `image_options`,
  `image_option_patterns`, `variables`, `parser_functions`, `language_prefixes` and `interwiki_prefixes`.
- Produces, in `tests/common/mod.rs`: `ru() -> Settings`, which loads `config/sites/ru.wikipedia.toml`.

- [ ] **Step 1: Write the failing tests** in `tests/settings.rs`. `has_node` is a test helper that walks the node
  tree recursively.

```rust
#[test] fn russian_namespaces() {
    let s = ru();
    assert_eq!(s.namespace("Файл"), Some(6)); assert_eq!(s.namespace("изображение"), Some(6));
    assert_eq!(s.namespace("К"), Some(14)); assert_eq!(s.namespace("Ш"), Some(10)); assert_eq!(s.namespace("Модуль"), Some(828));
    assert_eq!(s.namespace("Москва"), None);
}
#[test] fn file_and_category_nodes() {
    let s = ru();
    let p = frontend::parse("[[Файл:A.jpg|мини|Подпись]] [[К:Столицы]]", s.configuration(), 0).unwrap();
    assert!(has_node(&p, "Image")); assert!(has_node(&p, "Category"));
}
#[test] fn cyrillic_link_trail() { /* parse "[[Москва|Москв]]е": Link node end == byte length of the whole input */ }
#[test] fn template_name_normalisation() {
    let s = ru();
    for raw in ["Cite_web", " cite web ", "Шаблон:Cite web", "Ш:cite  web", "Template:Cite web"] { assert_eq!(s.template_name(raw), "cite web"); }
}
#[test] fn image_options() { let s = ru(); for o in ["мини","thumb","справа","200px","200пкс","x120px","upright=1.2","альт=Текст"] { assert!(s.is_image_option(o)); } assert!(!s.is_image_option("Вид на город")); }
#[test] fn language_prefixes() { let s = ru(); assert!(s.is_language_prefix("en")); assert!(s.is_interwiki_prefix("wikt")); assert!(!s.is_language_prefix("Москва")); }
#[test] fn ruwiki_markup_uses_no_unknown_prefix_or_tag() {
    let Some(rows) = common::private_corpus("ruwiki_markup_uses_no_unknown_prefix_or_tag") else { return };
    let s = Settings::load("config/sites/ruwiki.toml".as_ref()).unwrap();
    let unknown = settings::unknown_prefixes_and_tags(&s, rows.iter().map(|r| r.wikitext.as_str()));
    assert!(unknown.is_empty(), "add to ruwiki.toml: {unknown:?}");
}
```

  Also add `pub fn unknown_prefixes_and_tags<'a>(s: &Settings, texts: impl Iterator<Item = &'a str>) -> BTreeSet<String>`.
  It finds `[[X:` prefixes that are not a namespace, a language or an interwiki prefix, and `<tag` names that are
  not extension tags or HTML tags. The HTML list is in the settings file under `html_tags`.

- [ ] **Step 2: Run the tests.** `cargo test --release --test settings`. Expected: FAIL.
- [ ] **Step 3: Write `ru.wikipedia.toml`.**
  - Take the values from MediaWiki's `languages/messages/MessagesRu.php` (namespace names, magic-word and image-option
    aliases) and Russian Wikipedia's settings.
  - The namespaces, aliases, link trail and extension tags recorded in the spec's "What exists now" and D3 must be
    present. That includes `templatestyles`, `langconvert`, `phonos` and `page-collection`.
  - The language prefixes are MediaWiki's language codes, and the interwiki prefixes are the Wikimedia projects'
    (`wikt`, `commons`, `s`, `q`, `b`, `n`, `v`, `voy`, `d`, `m`, `mw`, `species`, `meta`, `foundation`, `wmf`,
    `phab`).
  - Copy the file to `ruwiki.toml`, changing only `site = "ruwiki"`.
- [ ] **Step 4: Implement `settings.rs`.** Build `parse_wiki_text_2::ConfigurationSource` from the file. Its
  `category_namespaces` and `file_namespaces` hold all names and aliases of ns 14 and ns 6.
- [ ] **Step 5: Run the tests.** `cargo test --release --test settings`. Expected: PASS. If the private test lists
  prefixes or tags, add each to `ruwiki.toml` with a comment, and re-run.
- [ ] **Step 6: Commit.** `git commit -m "Site settings for Russian Wikipedia and Ruwiki"`.

### Task 6: Row model and output schema

**Files:**
- Create: `src/components/model.rs`, `src/components/output.rs` (schemas only), `tests/schema.rs`, `tests/schema/v1.json`
- Modify: `Cargo.toml` (`serde_arrow = { version = "0.15", features = ["arrow-53"] }`)

**Interfaces:**
- Produces, in `model`:
  - `pub const SCHEMA_VERSION: &str = "1.0.0";`
  - `pub struct Key { pub source_file: String, pub row: i32, pub page_id: String, pub site: String, pub version: String, pub column: String }`;
  - `pub struct PageRow`, holding the `Key` fields, then `title: Option<String>, timestamp: Option<String>,
    status: String, status_detail: Option<String>, parse_steps: i64, n_bytes: i32, n_chars: i32, n_warnings: i32,
    warning_counts: Vec<WarningCount>, warnings: Vec<WarningRow>, redirect_target: Option<String>,
    n_sections: i32, n_paragraphs: i32, n_templates: i32, n_refs: i32, n_ref_lists: i32, n_citations: i32`;
  - `pub struct WarningRow { pub kind: String, pub start: i32, pub end: i32 }` and
    `pub struct WarningCount { pub kind: String, pub n: i32 }` (D8 asks for counts by kind; a list, since maps are
    ruled out);
  - `pub struct TimingRow`, holding the `Key` fields, then `parse_ms: f64, walk_ms: f64`;
  - the component rows `SectionRow`, `ParagraphRow`, `TemplateRow`, `RefRow`, `RefListRow` and `CitationRow`.
    Each begins with these common fields, in this order: `id: i32, start: i32, end: i32, start_char: i32,
    end_char: i32, section_id: i32, para_id: Option<i32>, para_offset: Option<i32>, parent_type: Option<String>,
    parent_id: Option<i32>, fallback: bool, fp: String`. The specific fields follow, exactly as named in the spec's
    output schema and in D7. `ParamRow { name, positional: bool, value_raw, value_text, start, end }` is shared.
  - `pub struct Components { pub sections: Vec<SectionRow>, pub paragraphs: Vec<ParagraphRow>, pub templates: Vec<TemplateRow>, pub refs: Vec<RefRow>, pub ref_lists: Vec<RefListRow>, pub citations: Vec<CitationRow> }`;
  - `pub struct PageOutput { pub page: PageRow, pub timing: TimingRow, pub components: Components }`.

  All rows derive `Serialize, Deserialize, Clone, Debug, PartialEq`.
- Produces, in `output`:
  - `pub const TABLES: [&str; 8] = ["pages","timings","sections","paragraphs","templates","refs","ref_lists","citations"];`
  - `pub fn schema(table: &str) -> arrow::datatypes::SchemaRef`. Component tables get the six `Key` columns
    prepended (`source_file`, `site`, `version` and `column` as `Dictionary(Int32, Utf8)`). Every schema carries
    the metadata `wikitext_parser.schema_version = SCHEMA_VERSION`.

- [ ] **Step 1: Write the failing test** in `tests/schema.rs`:

```rust
#[test] fn schemas_match_frozen_snapshot() {
    let got: serde_json::Value = output::TABLES.iter().map(|t| (t.to_string(), schema_json(&output::schema(t)))).collect();
    if std::env::var("UPDATE_EXPECTED").is_ok() { write_pretty("tests/schema/v1.json", &got); }
    let want: serde_json::Value = read_json("tests/schema/v1.json");
    assert_eq!(got, want, "schema changed: bump SCHEMA_VERSION (minor: added column/table; major: rename/remove/meaning)");
}
#[test] fn no_unsigned_64_or_map_types() { /* walk every field of every table: no UInt64, no Map */ }
```

- [ ] **Step 2: Run the tests.** `cargo test --release --test schema`. Expected: FAIL.
- [ ] **Step 3: Implement `model.rs` and `output::schema`.** Derive the field lists with `serde_arrow`
  (`SchemaLike::from_type::<Row>`), then override the dictionary columns.
- [ ] **Step 4: Write the snapshot and test it.** Run `UPDATE_EXPECTED=1 cargo test --release --test schema`, read
  `tests/schema/v1.json`, then run `cargo test --release --test schema`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Row model and output schema 1.0.0 (draft until release)"`.

### Task 7: Page processing and status

**Files:**
- Create: `src/components/page.rs`, `src/components/registry.rs`, `tests/page.rs`
- Modify: `tests/common/mod.rs`

**Interfaces:**
- Consumes: `frontend::parse`, `Settings`, `model`.
- Produces:
  - `pub struct Limits { pub parse_budget: u64, pub walk_budget: u64, pub depth_cap: usize, pub fallback: bool }`;
  - `pub const DEFAULT_PARSE_BUDGET: u64` (a placeholder of `2_000_000_000` until Task 10);
  - `pub const DEFAULT_WALK_BUDGET: u64 = 50_000_000;`
  - `pub const DEFAULT_DEPTH_CAP: usize = 400;`
  - `pub struct PageInput { pub key: Key, pub title: Option<String>, pub timestamp: Option<String>, pub text: Option<String> }`;
  - `pub fn process_page(input: &PageInput, settings: &Settings, registry: &Registry, limits: &Limits) -> PageOutput`.
    Until Task 13 and Task 18 exist, the walk and the fallback are stubs that return an empty `Components`.
  - In `registry.rs`, the types that later tasks fill: `#[derive(Default)] pub struct Registry` (the default has
    no entries), `pub struct TemplateEntry { pub role: String, pub family: Option<String>, pub archive_wrapper: bool, pub fields: BTreeMap<String, Vec<String>>, pub site: Option<String> }`,
    `Registry::template(&self, name: &str, site: &str) -> Option<&TemplateEntry>` and
    `Registry::section_role(&self, title_norm: &str) -> &str`, which returns `"content"` when nothing matches.
  - In `tests/common/mod.rs`: `defaults() -> Limits` (the four defaults, `fallback: true`),
    `reg() -> Registry` (`Registry::default()` until Task 14 makes it load `config/registry/`),
    `run_page(text: &str, site: &str) -> PageOutput` and `run_with(text: &str, limits: Limits) -> PageOutput`
    (site `wikipedia`). Both use `ru()` or the Ruwiki settings, `reg()`, and a fixed `Key`.
  - The status rules, in order:
    1. `text == None` gives `null_input`;
    2. a text that is only whitespace gives `empty`;
    3. a Redirect node as the first node gives `redirect`, with `redirect_target`;
    4. a parse or walk failure (`budget`, `depth_limit` or `panic`) gives that status when
       `limits.fallback == false`. Otherwise it gives `fallback`, with
       `status_detail = "<original status>: <message>"`;
    5. everything else is `ok`.
  - `warnings` keeps the first 100 parser warnings. `kind` is the `WarningMessage` variant name, `n_warnings`
    counts all of them, and `warning_counts` has one entry per kind present, sorted by kind.

- [ ] **Step 1: Write the failing tests** in `tests/page.rs`:

```rust
#[test] fn statuses() {
    assert_eq!(run(None).page.status, "null_input");
    assert_eq!(run(Some("  \n")).page.status, "empty");
    let r = run(Some(&common::fixture("redirect_ru"))); assert_eq!(r.page.status, "redirect"); assert_eq!(r.page.redirect_target.as_deref(), Some("Москва"));
    let r = run(Some(&common::fixture("redirect_en"))); assert_eq!(r.page.redirect_target.as_deref(), Some("Moscow"));
    assert_eq!(run(Some(&common::fixture("refs_basic"))).page.status, "ok");
}
#[test] fn budget_failure_with_and_without_fallback() {
    let t = common::fixture("refs_basic");
    let a = run_with(&t, Limits { parse_budget: 10, fallback: false, ..defaults() }); assert_eq!(a.page.status, "budget");
    let b = run_with(&t, Limits { parse_budget: 10, fallback: true, ..defaults() }); assert_eq!(b.page.status, "fallback");
    assert!(b.page.status_detail.unwrap().starts_with("budget: "));
}
#[test] fn counts_and_sizes() { let r = run(Some("Привет")); assert_eq!(r.page.n_bytes, 12); assert_eq!(r.page.n_chars, 6); assert!(r.page.parse_steps > 0); }
```

  Add a unit test in `page.rs` (`#[cfg(test)]`): `guarded(|| panic!("x"))` must yield status `panic` with the message
  `"x"`. `guarded` is the private wrapper around `catch_unwind` that `process_page` uses.
- [ ] **Step 2: Run the tests.** `cargo test --release --test page`. Expected: FAIL.
- [ ] **Step 3: Implement `page.rs`.** Wrap the parse and the walk in `std::panic::catch_unwind(AssertUnwindSafe(..))`.
  Measure `parse_ms` and `walk_ms` into `timing` only.
- [ ] **Step 4: Run the tests.** `cargo test --release --test page`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Page processing: statuses, budgets, catch_unwind"`.

### Task 8: Input layouts and labels

**Files:**
- Create: `src/components/input.rs`, `tests/input.rs`

**Interfaces:**
- Consumes: `PageInput`, `Key`.
- Produces:
  - `pub enum Layout { Pair, Single, Long }`;
  - `pub struct Label { pub key: String, pub site: String, pub version: String }`;
  - `pub fn parse_label(s: &str) -> anyhow::Result<Label>`, for `key=site:version`, or `site:version` for a `.wiki`
    file;
  - `pub fn detect_layout(schema: &Schema) -> anyhow::Result<Layout>`. `Long` needs a `version` column and a
    `wikitext` column. `Pair` needs `official_text` and `clone_text`. Otherwise `Single`, with the column found the
    way `parse_single.rs` finds it, by copying its functions, not moving them.
  - `pub struct InputFile { ... }`, with `pub fn open(path: &Path, layout: Option<Layout>, text_column: Option<&str>, labels: &[Label]) -> anyhow::Result<InputFile>`
    and `pub fn batches(&mut self, rows: usize) -> impl Iterator<Item = anyhow::Result<Vec<PageInput>>>`.
    - Pair layout gives two `PageInput`s per row, official first.
    - `row` is the 0-based row index in the file.
    - `source_file` is the file stem.
    - `column` is the text column name.
    - Arrow timestamps become RFC 3339 UTC strings.
  - `pub fn wiki_file(path: &Path, label: &Label) -> anyhow::Result<PageInput>`. Its `page_id` is the file stem,
    `row` is 0, and `column` is `"wikitext"`.
  - Errors:
    - a text column or version value without a label: `no --label for '<key>'`;
    - a label that matches nothing: `--label '<key>' matches no column or version`;
    - an unknown site: `unknown site '<site>' (no config/sites file)`.

- [ ] **Step 1: Write the failing tests** in `tests/input.rs`. Build small parquet files in a temporary directory
  for each layout.

```rust
#[test] fn pair_layout_yields_two_versions_per_row_in_order() { /* 2 rows -> 4 PageInputs: (row0 official),(row0 clone),(row1 official),(row1 clone); site/version from labels */ }
#[test] fn long_layout_maps_version_values() { /* corpus-shaped file with wiki_forkday + wiki_2025 -> versions "forkday","2025-01-01", site "wikipedia" */ }
#[test] fn duplicate_page_ids_are_kept_apart_by_row() { /* two rows page_id "7": both returned, rows 0 and 1 */ }
#[test] fn missing_and_unmatched_labels_are_errors() { /* exact messages above */ }
#[test] fn arrow_timestamp_becomes_rfc3339() { /* TimestampSecond 1735689600 -> "2025-01-01T00:00:00Z" */ }
```

- [ ] **Step 2: Run the tests.** `cargo test --release --test input`. Expected: FAIL.
- [ ] **Step 3: Implement `input.rs`.**
- [ ] **Step 4: Run the tests.** `cargo test --release --test input`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Input layouts pair/single/long/.wiki with required labels"`.

### Task 9: Driver, writers, CLI, manifest, resume, watchdog

**Files:**
- Create: `src/components/driver.rs`, `src/parse_components.rs`, `tests/driver.rs`
- Modify: `src/components/output.rs` (writers, JSON), `Cargo.toml` (`[[bin]] name = "parse_components" path =
  "src/parse_components.rs"`, `rayon = "1"`, feature `test-hooks = []`)

**Interfaces:**
- Consumes: `input`, `page::process_page`, `output::schema`.
- Produces:
  - `pub struct RunArgs { pub inputs: Vec<PathBuf>, pub out: PathBuf, pub labels: Vec<Label>, pub layout: Option<Layout>, pub text_column: Option<String>, pub format: Format, pub tables: Option<Vec<String>>, pub jobs: usize, pub limits: Limits, pub resume: bool }`;
  - `pub enum Format { Parquet, Json }`;
  - `pub fn run(args: &RunArgs) -> anyhow::Result<Manifest>`.
  - `output::PartWriter::create(out: &Path, table: &str, stem: &str)`, with `append(&[PageOutput])` and
    `finish()`. `finish` writes `<out>/<table>/<stem>.parquet.tmp`, then renames it, and renames `pages` last.
    It uses zstd.
  - `output::page_json(p: &PageOutput) -> String`: pretty JSON with the shape
    `{"page": PageRow, "sections": [...], "paragraphs": [...], "templates": [...], "refs": [...], "ref_lists": [...], "citations": [...]}`,
    without `timing`.
  - Settings are loaded from `config/sites/<site>.toml` per label site. The directory is resolved against the
    binary's `CARGO_MANIFEST_DIR` at build time, and `--config-dir` overrides it. The registry comes from
    `config/registry/`.
  - Manifest (`<out>/manifest.json`) fields:
    - `schema_version`, `crate_version`, `git_commit` (from `GIT_COMMIT` at build time via `build.rs`, else
      `"unknown"`), `vendored_upstream` (`bc6bde5`);
    - `settings_hashes` (site to hash), `registry_hash`, `args`;
    - `inputs` (path, rows), `outputs` (path, rows);
    - `status_counts` (site to version to status to n);
    - `pages_per_second`, `seconds_over_budget`.
  - `--resume` skips an input whose `pages/<stem>.parquet` exists.
  - The watchdog, a thread polling every second:
    - a page running more than `WATCHDOG_SECS = 600` gets `source_file\trow\tcolumn` appended to
      `<out>/quarantine.list`, then `std::process::abort()`;
    - on resume, quarantined entries skip the parse and get `status = "hang"`, plus fallback rows;
    - under `test-hooks`, the env `WIKITEXT_WATCHDOG_SECS` overrides the limit and `WIKITEXT_TEST_SLEEP_PAGE=<page_id>`
      makes that page sleep.

  CLI flags exactly as in the spec's Command line section, except `--report-templates`, which comes in milestone 3.

- [ ] **Step 1: Write the failing tests** in `tests/driver.rs`:

```rust
#[test] fn output_is_identical_for_one_and_four_jobs() { /* run the public corpus (labels from Global Constraints) with --jobs 1 and 4; every table except timings byte-identical */ }
#[test] fn every_input_row_yields_one_pages_row_per_text() { /* public corpus: pages rows == 180 */ }
#[test] fn resume_skips_completed_inputs() { /* run, delete sections/<stem>.parquet, run --resume: pages file mtime unchanged and sections not recreated */ }
#[test] fn wiki_file_to_json() { /* parse_components --input tests/fixtures/refs_basic.wiki --label wikipedia:adhoc --format json --out x.json -> page.status "ok", page.site "wikipedia" */ }
#[test] fn manifest_records_hashes_and_counts() { /* settings_hashes has "wikipedia", status_counts.wikipedia.forkday.ok > 0 */ }
#[test] fn tables_subset_always_writes_pages() { /* --tables refs -> only pages/ and refs/ dirs */ }
```

  In `tests/watchdog.rs` (`required-features = ["test-hooks"]`), test that a sleeping page with
  `WIKITEXT_WATCHDOG_SECS=1` aborts the process and writes `quarantine.list`. The following `--resume` run then
  reports `status = "hang"` for that page and `ok` for the rest.
- [ ] **Step 2: Run the tests.** `cargo test --release --test driver`. Expected: FAIL.
- [ ] **Step 3: Implement the driver and the writers.**
  - Read batches of 256 rows.
  - Process them with `rayon` `par_iter().map(process_page).collect::<Vec<_>>()`, which keeps order.
  - Append each batch to every `PartWriter`.
  - Build each component table's key columns by repeating each page's `Key` once per row.
  - A `.wiki` input with `Format::Json` writes one pretty document to `--out`. A Parquet input with `Format::Json`
    writes `<out>/<stem>.jsonl`.
- [ ] **Step 4: Run the tests.** `cargo test --release --test driver` and
  `cargo test --release --features test-hooks --test watchdog`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "parse_components: streaming parallel driver, part writers, manifest, resume, watchdog"`.

### Task 10: Calibrate the budget; measure on a sample

**Files:**
- Create: `examples/measure.rs`
- Modify: `src/components/page.rs` (`DEFAULT_PARSE_BUDGET`), `docs/superpowers/specs/2026-10-27-structured-parse-design.md`
  (append a dated "Calibration" note under D8)

**Interfaces:**
- Consumes: `frontend::parse`, `Settings`, `corpus::load`, `input::InputFile`.
- `examples/measure.rs <parquet> --label ... [--budget N]` prints one TSV line per page version:
  `page_id version n_chars steps parse_ms status`. With `--budget 0`, it parses without a limit, but it skips rows
  whose `case` is `slow`, since those may never end.

- [ ] **Step 1: Measure on the corpus.**
  - Run `cargo run --release --example measure -- tests/corpus/wikipedia_v1.parquet <labels> --budget 0`.
  - Compute `rate` = the median of `steps / parse_ms × 1000` over the 20 largest non-slow versions.
  - Set `DEFAULT_PARSE_BUDGET = round(30 × rate, two significant digits)`.
- [ ] **Step 2: Check the slow and largest pages.** Run `measure` with `--budget DEFAULT_PARSE_BUDGET` on the
  rows whose `case` is `slow` or `largest`. Expected: every one finishes, either `ok` or `budget`, within roughly
  30 s each. Record each one's steps and time (D9 times them separately) and how many exceed the budget.
- [ ] **Step 3: Get the performance sample.** **Ask the user first**, stating: one row group of the 2025
  cross-section, about 14,000 page pairs, about 0.5 GB, into the scratch directory, deleted after Task 21.
  - Only after a yes, read row group 0 of one cross-section file with pyarrow (`AWS_PROFILE` and the bucket path
    as the replication package documents), and write it locally.
  - The command lives in the session, not in the repository.
  - If the user says no, use the corpus alone, and say so in the calibration note.
- [ ] **Step 4: Measure the sample.** Run `measure` on it with
  `--label official_text=wikipedia:2025-01-01 --label clone_text=ruwiki:2025-01-01 --budget DEFAULT_PARSE_BUDGET`.
  Report the share of pages over budget. **If it is above 0.05%, stop and tell the user** before going on, as the
  spec requires.
- [ ] **Step 5: Write the calibration note** in the spec (rate, budget, counts). Run `cargo test --release`.
  Expected: PASS.
- [ ] **Step 6: Commit.** `git commit -m "Calibrate the parse budget on the corpus and a performance sample"`.

---

## Milestone 2: references first, complete

### Task 11: Spans, fingerprints, URLs

**Files:**
- Create: `src/components/spans.rs`, `src/components/fingerprint.rs`, `src/components/urls.rs`, `tests/urls.rs`
- Modify: `Cargo.toml` (`xxhash-rust = { version = "0.8", features = ["xxh3"] }`, `psl = "2"`, `url = "2"`)

**Interfaces:**
- `spans::CharIndex::new(text: &str) -> CharIndex`, with `char_of(&self, byte: usize) -> i32`, which takes
  O(log n) after O(n) setup.
- `fingerprint::fp(normalised: &str) -> String`, which is `format!("{:016x}", xxh3_64(normalised.as_bytes()))`.
- `fingerprint::norm(s: &str) -> String`, which lowercases, collapses Unicode whitespace to one space, and trims.
- `urls::UrlInfo { pub url: String, pub archive_url: Option<String>, pub host: Option<String>, pub registrable_domain: Option<String> }`.
- `urls::analyse(raw: &str) -> UrlInfo` does, in order:
  1. decode HTML entities (`&amp;`) and trim;
  2. prefix `https:` to a protocol-relative URL;
  3. if `original_from_archive` finds an original, set `url` to it and `archive_url` to the raw URL;
  4. take `host` from `url`, lowercase it, decode IDN punycode to Unicode, and strip the port and a leading `www.`;
  5. take `registrable_domain` from `psl::domain_str(host)`.
- `urls::original_from_archive(url: &str) -> Option<String>` recognises:
  - `web.archive.org/web/<digits>[a-z_]*/<orig>`;
  - `archive.org/web/...`;
  - `archive.today`, `archive.ph`, `archive.is` and `archive.li` in the form `/<timestamp>/<orig>`;
  - `webcitation.org` with `url=` in the query.

  If `<orig>` has no scheme, prefix `http://`.
- `urls::wayback_url(orig: &str, date: &str) -> String` builds `https://web.archive.org/web/{date}/{orig}`.

- [ ] **Step 1: Write the failing tests** in `tests/urls.rs`:

```rust
#[test] fn hosts() {
    assert_eq!(analyse("https://Президент.рф/a").host.as_deref(), Some("президент.рф"));
    assert_eq!(analyse("//tass.ru/x").host.as_deref(), Some("tass.ru"));
    assert_eq!(analyse("http://WWW.Kremlin.RU:80/a").host.as_deref(), Some("kremlin.ru"));
    assert_eq!(analyse("https://news.bbc.co.uk/x").registrable_domain.as_deref(), Some("bbc.co.uk"));
    assert_eq!(analyse("https://a.ru/?x=1&amp;y=2").url, "https://a.ru/?x=1&y=2");
}
#[test] fn archive_unwrapping() {
    let u = analyse("https://web.archive.org/web/20200101000000/https://tass.ru/politika/1");
    assert_eq!(u.url, "https://tass.ru/politika/1");
    assert_eq!(u.archive_url.as_deref(), Some("https://web.archive.org/web/20200101000000/https://tass.ru/politika/1"));
    assert_eq!(u.host.as_deref(), Some("tass.ru"));
    assert_eq!(analyse("https://archive.ph/2021.01.01-000000/http://gov.ru/a").url, "http://gov.ru/a");
    assert_eq!(analyse("https://web.archive.org/web/2019id_/www.bbc.co.uk/news/1").url, "http://www.bbc.co.uk/news/1");
}
#[test] fn fingerprint_shape() { let f = fp(&norm("  Привет   МИР ")); assert_eq!(f.len(), 16); assert_eq!(f, fp("привет мир")); }
#[test] fn char_index() { let t = "аб c"; let ci = CharIndex::new(t); assert_eq!(ci.char_of(0), 0); assert_eq!(ci.char_of(4), 2); assert_eq!(ci.char_of(t.len()), 4); }
```

- [ ] **Step 2: Run the tests.** `cargo test --release --test urls`. Expected: FAIL.
- [ ] **Step 3: Implement the three modules.**
- [ ] **Step 4: Run the tests.** `cargo test --release --test urls`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Spans, fingerprints, URL analysis with archive unwrapping"`.

### Task 12: Tag attributes

**Files:**
- Create: `src/components/attrs.rs`, `tests/attrs.rs`

**Interfaces:**
- `attrs::open_tag(source: &str) -> &str` returns the slice from `<` to the first `>` that is not inside quotes.
- `attrs::tag_attrs(open_tag: &str) -> Vec<(String, String)>`. Names are lowercased. Values have their quotes
  stripped, entities decoded, whitespace collapsed, and are trimmed.
- `attrs::self_closing(open_tag: &str) -> bool`.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn ref_name_variants_are_one_name() {
    for t in [r#"<ref name="a">"#, "<ref name=a>", "<ref name = 'a'>", r#"<ref name=" a ">"#, r#"<ref NAME="a"/>"#] {
        assert_eq!(get(tag_attrs(open_tag(t)), "name"), Some("a".into()), "{t}");
    }
}
#[test] fn group_and_self_closing() {
    let t = r#"<ref group="К" name=b />"#;
    assert_eq!(get(tag_attrs(open_tag(t)), "group"), Some("К".into())); assert!(self_closing(open_tag(t)));
    assert!(!self_closing(open_tag(r#"<ref name="a>b">x</ref>"#)));
}
```

- [ ] **Step 2: Run the tests.** `cargo test --release --test attrs`. Expected: FAIL.
- [ ] **Step 3: Implement it** as a hand-written single-pass lexer. No regex.
- [ ] **Step 4: Run the tests.** `cargo test --release --test attrs`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Tag attribute lexer"`.

### Task 13: Walk: sections, paragraphs, text with anchors

**Files:**
- Create: `src/components/walk/{mod,text,sections}.rs`, `tests/walk_sections.rs`
- Modify: `src/components/page.rs` (call `walk::walk`)

**Interfaces:**
- Consumes: `frontend::Parsed`, `Settings`, `Registry`, `CharIndex`, `fingerprint`.
- Produces:
  - `pub struct WalkLimits { pub budget: u64, pub depth_cap: usize }`;
  - `pub enum WalkError { Budget { nodes: u64 }, DepthLimit { depth: usize } }`;
  - `pub fn walk(text: &str, parsed: &Parsed, settings: &Settings, registry: &Registry, limits: &WalkLimits) -> Result<Components, WalkError>`.
  - Internally: `text::TextBuilder`, with `push_text(&str, source_start)`, `anchor() -> i32` (the current
    code-point offset), `break_paragraph(kind)` and `finish() -> Vec<ParagraphRow>`.

- **Rules pinned here:**
  - **Sections.** The lead is id 0, level 0, title `""`, `path []`. A Heading node opens a section. Its `end` is
    the next heading's `start` (or the end of the text). `subtree_end` is the next heading of the same or a higher
    level. `parent_section_id` is the nearest earlier section with a lower level, or 0. `title_norm` is
    `norm(title_text)`. `path` is the ancestors' `title_norm` plus its own. `role` comes from
    `registry.section_role(title_norm)`, with `content` as the default. `fp = fp(path joined by "\u{1f}")`.
  - **Paragraphs.** They break at ParagraphBreak, at Heading, and at the start and end of a list, a table or a
    Preformatted block. A Preformatted block is its own paragraph, of kind `preformatted`. Each list item is its own paragraph (`kind = "list_item"`, `list_id` counting the lists of the
    page from 0, `item_depth` from the list's nesting starting at 1). A definition list gives `definition_term` and
    `definition`. Paragraphs whose text is empty after trimming are dropped and do not consume an id.
    `fp = fp(norm(text))`.
  - **Text.**
    - Runs of whitespace collapse to one space, and the text is trimmed. Offsets are counted in the collapsed text.
    - Character entities are decoded. Bold and italic markers add nothing.
    - A Link adds its label nodes' text, or the target title without namespace when there is no label, plus the
      link trail.
    - An ExternalLink adds its label, or nothing when it is bare.
    - The contents of `nowiki` are added.
    - Template, Tag `ref`, Image, Category, Comment, Table and MagicWord add nothing.
    - A component inside a paragraph gets `para_id` and `para_offset = anchor()` at the moment it is met. Nested
      components inside an excluded component take their outermost excluded ancestor's offset.
  - **Limits.** Every node visited counts 1 against `limits.budget`. The depth is the template, tag and link
    nesting depth. Exceeding either returns the error.
  - **Constants.** `DEFAULT_WALK_BUDGET` and `DEFAULT_DEPTH_CAP` must stay at least 10 times and 4 times the
    public corpus's maximum node count and maximum depth. A test checks this.

- [ ] **Step 1: Write the failing tests** in `tests/walk_sections.rs`:

```rust
#[test] fn sections_and_paths() {
    let c = run_page(&common::fixture("sections_paragraphs"), "wikipedia").components;
    let s: Vec<_> = c.sections.iter().map(|s| (s.id, s.level, s.title_text.as_str(), s.parent_section_id)).collect();
    assert_eq!(s, vec![(0,0,"",0),(1,2,"История",0),(2,3,"Ранние годы",1),(3,2,"Примечания",0)]);
    assert_eq!(c.sections[2].path, vec!["история","ранние годы"]);
    assert!(c.sections.iter().all(|s| s.role == "content"));   // roles arrive with the registry (Task 14)
}
#[test] fn paragraph_text_link_trail_files_excluded() {
    let c = run_page(&common::fixture("sections_paragraphs"), "wikipedia").components;
    let p: Vec<_> = c.paragraphs.iter().map(|p| (p.section_id, p.kind.as_str(), p.text.as_str())).collect();
    assert_eq!(p, vec![(0,"prose","Москва — столица России. Город в Москве."),(1,"prose","Текст истории."),(2,"list_item","Пункт один"),(2,"list_item","Пункт два")]);
}
#[test] fn spans_round_trip() { /* for every section and paragraph: text[start..end] is valid UTF-8, and start_char == CharIndex::char_of(start) */ }
#[test] fn depth_limit_and_budget() {
    let deep = "{{a|".repeat(500) + &"}}".repeat(500);
    assert_eq!(run_with(&deep, Limits { fallback: false, ..defaults() }).page.status, "depth_limit");
}
#[test] fn walk_constants_have_headroom_on_corpus() { /* max nodes x 10 <= DEFAULT_WALK_BUDGET; max depth x 4 <= DEFAULT_DEPTH_CAP */ }
```

- [ ] **Step 2: Run the tests.** `cargo test --release --test walk_sections`. Expected: FAIL.
- [ ] **Step 3: Implement `walk/mod.rs`, `text.rs` and `sections.rs`.** The walk is iterative, with an explicit
  stack, so the stack cannot overflow.
- [ ] **Step 4: Run the tests.** `cargo test --release --test walk_sections`. Expected: PASS. Then run
  `UPDATE_EXPECTED=1 cargo test --release --test schema` if a row changed, followed by `cargo test --release`.
- [ ] **Step 5: Commit.** `git commit -m "Walk: sections, paragraphs, text with anchors"`.

### Task 13b: Walk: the templates table

**Files:**
- Create: `src/components/walk/templates.rs`, `tests/walk_templates.rs`

**Interfaces:**
- `TemplateRow` fields: `name_raw` (the source slice of the name), `name` (`settings.template_name`), `kind`, `role`
  (`registry.template(name, site).map(role)`, or else `"other"`), `looks_like_infobox` (`false` until milestone 3),
  `depth` (0 at the top), `rendered_len` (0 until milestone 3) and `params: Vec<ParamRow>`.
- `kind`:
  - `module` for a name starting with `#invoke:`;
  - `parser_function` for any other name starting with `#`, or containing a `:` before which stands a word for
    which `settings.is_parser_function` holds;
  - `variable` when `settings.is_variable(name)`;
  - `template` otherwise.
- Positional numbering follows MediaWiki: unnamed parameters count from 1, and an explicit `N=` names it `N` with
  `positional = false`. Parameter names and `value_raw` are source slices trimmed of surrounding whitespace.
  `value_text` is the parameter's text under Task 13's text rules.
- For a `module` or `parser_function`, `name` is the text up to the first `:`. The text after the colon becomes
  positional parameter `1`, so the `|` parameters count from `2`.
- Components found inside a parameter value get `parent_type = "template"`, `parent_id`, and `depth + 1`.
- `fp = fp(name + "\u{1f}" + params as "name=norm(value_text)", sorted by name and joined by "\u{1f}")`.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn templates_params_nesting_kinds() {
    let t = run_page(&common::fixture("templates"), "wikipedia").components.templates;
    let v: Vec<_> = t.iter().map(|t| (t.name.as_str(), t.kind.as_str(), t.depth, t.parent_id)).collect();
    assert_eq!(v, vec![("карточка города","template",0,None),("num","template",1,Some(0)),("нп3","template",0,None),("#if","parser_function",0,None)]);
    let p: Vec<_> = t[0].params.iter().map(|p| (p.name.as_str(), p.positional, p.value_raw.as_str())).collect();
    assert_eq!(p, vec![("название",false,"Москва"),("население",false,"{{num|13000000}}"),("1",false,"позиционный")]);
    assert_eq!(t[2].params.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(), vec!["1","2","3","4"]);
    assert!(t[2].params.iter().all(|p| p.positional));
    let f: Vec<_> = t[3].params.iter().map(|p| (p.name.as_str(), p.value_raw.as_str())).collect();
    assert_eq!(f, vec![("1","x"),("2","да"),("3","нет")]);
}
```
- [ ] **Step 2: Run the test.** `cargo test --release --test walk_templates`. Expected: FAIL.
- [ ] **Step 3: Implement `templates.rs`.**
- [ ] **Step 4: Run the tests.** `cargo test --release --test walk_templates`, then `cargo test --release`.
  Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Walk: templates table with parameters and nesting"`.

### Task 14: Registry

**Files:**
- Create: `config/registry/templates.toml`, `config/registry/sections.toml`, `tests/registry.rs`
- Modify: `src/components/registry.rs` (types from Task 7), `tests/common/mod.rs` (`reg()` loads
  `config/registry/`), `Cargo.toml` (`toml = "1"`)

**Interfaces:**
- `Registry::load(dir: &Path) -> anyhow::Result<Registry>` reads both files, and `Registry::hash(&self) -> String`
  is the sha256 of the two files' bytes concatenated.
- `templates.toml` holds one `[[template]]` table per entry: `name`, `aliases` (list), `role`, optional `family`,
  `archive_wrapper` (default false), optional `site`, and a `[template.fields]` table mapping each canonical key
  to the parameter names that fill it, in order of preference.
- `Registry::template` now finds entries by name or alias. Both sides are normalised with the rule of
  `Settings::template_name`, and a `site`-specific entry wins over a general one.
- `role` is one of `infobox`, `citation`, `footnote`, `ref_list`, `link_status`, `inline_text`, `interlanguage`,
  `navigation`, `maintenance` or `other`.
- The canonical citation field keys are `url`, `archive_url`, `archive_date_raw`, `title`, `authors`, `date_raw`,
  `year`, `publisher`, `work`, `language`, `isbn`, `doi`, `access_date_raw`, `harv_ref` and `harv_names`.
- **Entries this milestone must contain:**
  - **citation**: `cite web` (web), `cite news` (news), `cite book` (book), `cite journal` (journal),
    `cite encyclopedia` (encyclopedia), `книга` (book), `статья` (journal), `публикация` (other_template),
    `webarchive` and `wayback` (web, `archive_wrapper = true`), each with the Russian and English parameter names;
  - **footnote**: `sfn`, `efn`, `комментарий`;
  - **ref_list**: `примечания`, `reflist`, `notelist`, `список примечаний`, `комментарии`;
  - **link_status**: `недоступная ссылка`, `dead link`.

  The coverage test of Task 15 adds the aliases and entries the corpus needs.
- `sections.toml` maps `примечания` and `комментарии` to `notes`; `литература` and `источники` to `bibliography`;
  `ссылки` to `external_links`; and `см. также` to `see_also`.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn name_variants_resolve() {
    let (s, r) = (ru(), reg());
    for raw in ["Cite_web", " cite web ", "Шаблон:Cite web", "Ш:cite web"] { assert_eq!(r.template(&s.template_name(raw), "wikipedia").unwrap().family.as_deref(), Some("web")); }
}
#[test] fn roles() { let r = reg(); assert_eq!(r.template("sfn","wikipedia").unwrap().role, "footnote"); assert_eq!(r.template("примечания","ruwiki").unwrap().role, "ref_list"); assert!(r.template("wayback","wikipedia").unwrap().archive_wrapper); }
#[test] fn aliases_resolve_to_their_entry() {
    let (s, r) = (ru(), reg());
    for (name, alias) in r.all_aliases() { assert_eq!(r.template(&s.template_name(&alias), "wikipedia").map(|e| e as *const _), r.template(&name, "wikipedia").map(|e| e as *const _), "{alias}"); }
}
#[test] fn section_roles() {
    let r = reg(); assert_eq!(r.section_role("литература"), "bibliography"); assert_eq!(r.section_role("история"), "content");
    let c = run_page(&common::fixture("sections_paragraphs"), "wikipedia").components;
    assert_eq!(c.sections[3].role, "notes");
}
```

  This needs `Registry::all_aliases(&self) -> Vec<(String, String)>` (entry name, alias as written in the file).
- [ ] **Step 2: Run the tests.** `cargo test --release --test registry`. Expected: FAIL.
- [ ] **Step 3: Implement loading and write both TOML files.** Then `role` in `TemplateRow` (Task 13b) and in
  sections (Task 13) comes from the loaded registry.
- [ ] **Step 4: Run the tests.** `cargo test --release --test registry`, then `cargo test --release`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Template and section registry"`.

### Task 15: Refs and ref lists

**Files:**
- Create: `src/components/walk/refs.rs`, `tests/walk_refs.rs`
- Modify: `src/components/registry.rs`, `config/registry/templates.toml`, `tests/registry.rs`

**Interfaces:**
- `RefRow` specific fields: `kind` (`inline`, `named`, `reuse`, `list_defined` or `template`), `name`, `group`
  (`""` when absent), `body_raw`, `body_text`, `def_ref_id: Option<i32>`, `n_uses: Option<i32>` (on `named` and
  named `list_defined` refs, else `None`), `resolved: bool`, `harv_key: Option<String>` and
  `cites_citation_id: Option<i32>`. The last two are set in Task 17.
- `registry::in_ref_coverage(outputs: &[PageOutput], site: &str) -> Vec<(String, usize, bool)>` returns each
  template name found inside refs (a template whose span lies inside a ref's span), its count, and whether it has
  a registry entry, sorted by count descending, then by name.
- `RefListRow` specific fields: `group`, `kind` (`tag` or `template`), `template_id: Option<i32>` and `n_entries`.
  `n_entries` is the number of definitions in that group.
- **Rules:**
  - Attributes come from `attrs::tag_attrs(attrs::open_tag(&text[start..end]))`.
  - Resolution is per (group, name) over the whole page, so a reuse may come before its definition.
  - An unresolved reuse has `def_ref_id = None` and `resolved = false`.
  - `n_uses` counts the definition itself plus its reuses.
  - A ref inside a `<references>` tag, or inside a parameter of a `ref_list` template, is `list_defined`, and its
    `section_id` is that of the list.
  - Every role-`footnote` template is a `template` ref, and its `body_raw` is its first positional parameter.
  - `fp = fp(kind + "\u{1f}" + group + "\u{1f}" + name + "\u{1f}" + norm(body_text))`.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn basic_named_reuse() {
    let c = run_page(&common::fixture("refs_basic"), "wikipedia").components;
    let v: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.name.as_deref(), r.def_ref_id, r.n_uses, r.resolved)).collect();
    assert_eq!(v, vec![("inline",None,None,None,true),("named",Some("a"),Some(1),Some(2),true),("reuse",Some("a"),Some(1),None,true)]);
    assert_eq!(c.refs[0].body_text, "Простой источник.");
    assert_eq!(c.ref_lists.len(), 1); assert_eq!(c.ref_lists[0].kind, "template");
    assert!(c.refs.iter().all(|r| r.para_id == Some(0)));
}
#[test] fn groups_and_list_defined() {
    let c = run_page(&common::fixture("refs_groups_listdefined"), "wikipedia").components;
    let kinds: Vec<_> = c.refs.iter().map(|r| (r.kind.as_str(), r.group.as_str(), r.name.as_deref())).collect();
    assert_eq!(kinds, vec![("inline","К",None),("reuse","",Some("b")),("reuse","",Some("c")),("list_defined","",Some("b")),("list_defined","",Some("c"))]);
    assert!(c.refs[1].resolved && c.refs[2].resolved);
    assert_eq!(c.ref_lists.iter().map(|l| (l.kind.as_str(), l.group.as_str())).collect::<Vec<_>>(), vec![("tag","К"),("tag",""),("template","")]);
}
#[test] fn reuse_before_definition_and_unresolved() {
    let c = run_page(&common::fixture("refs_reuse_order"), "wikipedia").components;
    assert_eq!(c.refs[0].def_ref_id, Some(1)); assert_eq!(c.refs[1].n_uses, Some(2));
    assert!(!c.refs[2].resolved); assert_eq!(c.refs[2].def_ref_id, None);
}
#[test] fn ref_inside_template_parameter() {
    let c = run_page(&common::fixture("refs_in_template"), "wikipedia").components;
    assert_eq!(c.refs.len(), 1); assert_eq!(c.refs[0].parent_type.as_deref(), Some("template")); assert_eq!(c.refs[0].parent_id, Some(0));
}
```

  Append to `tests/registry.rs`:

```rust
#[test] fn in_ref_templates_are_classified_public() { assert_coverage(common::public_corpus(), "wikipedia"); }
#[test] fn in_ref_templates_are_classified_private() { let Some(rows) = common::private_corpus("in_ref_templates_are_classified_private") else { return }; assert_coverage(rows, "ruwiki"); }
// assert_coverage: parse every version with its site; unclassified in-ref occurrences / all in-ref occurrences <= 0.01;
// on failure print the 30 most frequent unclassified names
```

- [ ] **Step 2: Run the tests.** `cargo test --release --test walk_refs --test registry`. Expected: FAIL.
- [ ] **Step 3: Implement `refs.rs` and `in_ref_coverage`.**
- [ ] **Step 4: Run the ref tests.** `cargo test --release --test walk_refs`. Expected: PASS.
- [ ] **Step 5: Complete the registry.** Run `cargo test --release --test registry`. While a coverage test fails,
  add the names it prints to `templates.toml` and run it again. A name is an alias of an existing entry when the
  markup shows it used the same way; otherwise it gets its own entry. A template that is common inside refs but is
  not a citation (`{{ref-en}}`, `{{lang-en}}`, `{{nobr}}`) gets an explicit `role = "other"` entry, which counts
  as classified. Expected: PASS, then `cargo test --release` passes too.
- [ ] **Step 6: Commit.** `git commit -m "Walk: refs and ref lists; registry complete for in-ref templates on both sites"`.

### Task 16: Citations

**Files:**
- Create: `src/components/walk/citations.rs`, `tests/walk_citations.rs`

**Interfaces:**
- `CitationRow` specific fields:
  - `context` (`in_ref`, `bibliography` or `other`), `ref_id: Option<i32>`, `family`, `template_id: Option<i32>`,
    `template_name: Option<String>`, `params: Vec<ParamRow>`;
  - the URL fields `url`, `archive_url`, `archive_date_raw`, `host`, `registrable_domain`, `dead_link: bool`;
  - the bibliographic fields `title`, `authors: Vec<String>`, `date_raw`, `year: Option<i32>`, `publisher`, `work`,
    `language`, `isbn`, `doi`, `access_date_raw`, `harv_key`, `text`.

  All of them are optional strings, unless typed otherwise.
- **Rules:**
  - **Where rows come from.** Citations come from the body of every `inline`, `named` or `list_defined` ref, from
    the body of every `template` ref other than `sfn`, and from citation templates outside refs. A citation
    template outside refs has `context = "bibliography"` if its section's role is `bibliography` or
    `external_links`, otherwise `"other"`.
  - **Within one body:**
    - every citation-role template that is not an archive wrapper is one row;
    - every ExternalLink not inside such a template is one `bare_link` row, with `title` taken from its label;
    - an archive-wrapper template merges into the nearest preceding row in the same body. It sets that row's
      `archive_date_raw` and its `archive_url`: the wrapper's `url` parameter as written when that is an archive
      URL (`original_from_archive` finds an original in it), else `urls::wayback_url(url, date)`. With no
      preceding row, it becomes a standalone `web` row whose `url` is the original;
    - a `link_status` template in the body sets `dead_link = true` on every row of that body;
    - a body with no row yields one `plain_text` row, with `text = body_text`;
    - a template ref (`efn`) whose body holds no citation template yields `plain_text` too.
  - **Field mapping.** The fields come from the registry's per-template map.
    - `authors` collects `author`, `authorN`, `автор` and `lastN`+`firstN` (as `"last, first"`), in order.
    - `year` is the `year`/`год` field, else the first 4-digit run between 1000 and 2100 in `date_raw`.
    - The URL fields go through `urls::analyse`, so `url` is the original and `host` comes from it.
  - `fp = fp(family + "\u{1f}" + (url if any, else norm(title + " " + authors joined) if any, else norm(text)))`.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn web_and_plain() {
    let c = run_page(&common::fixture("refs_basic"), "wikipedia").components;
    assert_eq!(c.citations.iter().map(|x| (x.family.as_str(), x.ref_id)).collect::<Vec<_>>(), vec![("plain_text",Some(0)),("web",Some(1))]);
    let w = &c.citations[1];
    assert_eq!((w.url.as_deref(), w.host.as_deref(), w.title.as_deref(), w.work.as_deref(), w.year), (Some("https://www.kremlin.ru/events/1"), Some("kremlin.ru"), Some("Событие"), Some("Kremlin"), Some(2020)));
}
#[test] fn book_from_list_defined() {
    let c = run_page(&common::fixture("refs_groups_listdefined"), "wikipedia").components;
    let b = c.citations.iter().find(|x| x.family == "book").unwrap();
    assert_eq!((b.authors.clone(), b.title.as_deref(), b.publisher.as_deref(), b.year, b.isbn.as_deref()), (vec!["Иванов И. И.".to_string()], Some("Книга"), Some("Наука"), Some(1990), Some("978-5-02-000000-0")));
    let t = c.citations.iter().find(|x| x.family == "bare_link").unwrap(); assert_eq!(t.host.as_deref(), Some("tass.ru")); assert_eq!(t.title.as_deref(), Some("ТАСС"));
}
#[test] fn archives_and_dead_links() {
    let c = run_page(&common::fixture("archive_deadlink"), "wikipedia").components;
    let v: Vec<_> = c.citations.iter().map(|x| (x.family.as_str(), x.url.as_deref().unwrap(), x.host.as_deref().unwrap(), x.archive_url.is_some(), x.dead_link)).collect();
    assert_eq!(v, vec![
        ("web","https://tass.ru/politika/1","tass.ru",true,false),
        ("news","https://ria.ru/2020/a.html","ria.ru",true,false),
        ("bare_link","https://www.bbc.co.uk/news/1","bbc.co.uk",true,false),
        ("journal","http://old.example.org/x","old.example.org",false,true),
        ("web","http://gov.ru/a","gov.ru",true,false)]);
    assert_eq!(c.citations[4].archive_url.as_deref(), Some("https://web.archive.org/web/20150101/http://gov.ru/a"));
    assert_eq!(c.citations[1].archive_date_raw.as_deref(), Some("2021-01-01"));
}
#[test] fn efn_body_citation() {
    let c = run_page(&common::fixture("efn"), "wikipedia").components;
    assert_eq!(c.refs[0].kind, "template"); assert_eq!(c.citations.len(), 1);
    assert_eq!((c.citations[0].ref_id, c.citations[0].host.as_deref()), (Some(0), Some("a.ru")));
}
#[test] fn every_ref_with_body_has_a_citation() { /* over the public corpus: every ref of kind inline/named/list_defined has >= 1 citation row with that ref_id */ }
```

- [ ] **Step 2: Run the tests.** `cargo test --release --test walk_citations`. Expected: FAIL.
- [ ] **Step 3: Implement `citations.rs`.**
- [ ] **Step 4: Run the tests.** `cargo test --release --test walk_citations`, then `cargo test --release`. Expected:
  PASS.
- [ ] **Step 5: Commit.** `git commit -m "Walk: citations with archive unwrapping and dead links"`.

### Task 17: Short footnotes

**Files:**
- Modify: `src/components/walk/citations.rs`, `src/components/walk/refs.rs`
- Create: `tests/walk_sfn.rs`

**Interfaces:**
- `pub fn citeref(names: &[&str], year: &str) -> String` returns
  `"CITEREF" + names (each trimmed, inner whitespace replaced by "_") concatenated + year`.
- **Citation `harv_key`:**
  - with a `harv_ref` parameter whose value is not `harv`: `citeref(&[ref], year)`;
  - else with `harv_names` (`last1`..`last4`, or the surnames of `authors`, meaning the text before the first space
    or comma, up to four): `citeref(names, year)`;
  - else `None`.
- **Sfn ref `harv_key`.** Positional parameters up to the first one that matches `^\d{4}[a-zа-я]?$` are the names,
  and that one is the year. Named parameters (`с=`, `p=`) are ignored.
- **Resolution.** `cites_citation_id` points to the first citation on the page with an equal `harv_key`, else
  `resolved = false`. Sfn refs yield no citation rows.

- [ ] **Step 1: Write the failing test:**

```rust
#[test] fn sfn_resolution() {
    let c = run_page(&common::fixture("sfn"), "wikipedia").components;
    let r: Vec<_> = c.refs.iter().filter(|r| r.kind == "template").map(|r| (r.harv_key.as_deref(), r.cites_citation_id, r.resolved)).collect();
    assert_eq!(r, vec![(Some("CITEREFИванов1990"),Some(0),true),(Some("CITEREFПетровСидоров2001"),Some(1),true),(Some("CITEREFНикто1900"),None,false)]);
    assert_eq!(c.citations.len(), 2); assert!(c.citations.iter().all(|x| x.context == "bibliography" && x.ref_id.is_none()));
}
```

- [ ] **Step 2: Run the test.** `cargo test --release --test walk_sfn`. Expected: FAIL.
- [ ] **Step 3: Implement it.**
- [ ] **Step 4: Check against real pages.** Run `cargo test --release`. Then report the resolution rate of `{{sfn}}`
  over the public corpus, printed by an `#[ignore]` test `sfn_resolution_rate`. Read the unresolved cases in the
  markup. When a failure comes from a key rule (not from the page's own error), fix the rule in the registry's
  `harv_*` fields and add an assertion for it.
- [ ] **Step 5: Commit.** `git commit -m "Short footnotes: CITEREF keys and resolution"`.

### Task 18: Fallback scanner (refs, citations, templates, sections)

**Files:**
- Create: `src/components/fallback.rs`, `tests/fallback.rs`
- Modify: `src/components/page.rs` (call `fallback::scan` when the parse or the walk fails and `limits.fallback`)

**Interfaces:**
- `pub fn scan(text: &str, settings: &Settings, registry: &Registry) -> Components`. It sets `fallback = true` on
  every row and emits no paragraphs, so `para_id` and `para_offset` are `None`.
- It is a single linear pass, with no regex backtracking and no recursion. It recognises:
  - `^=+ … =+\s*$` heading lines, which become sections built the same way as in Task 13;
  - `<ref …>…</ref>`, `<ref …/>`, `<references …>…</references>` and `<references …/>`, case-insensitive, with
    `<nowiki>`, `<!-- -->` and `<pre>` regions skipped;
  - `{{ … }}` by brace counting, with nesting capped at `DEFAULT_DEPTH_CAP`, where parameters split on `|` at
    depth 0 and `[[ ]]`, `{{ }}` are respected inside values;
  - `[url label]` external links inside ref bodies.

  Refs, ref lists, templates and citations then follow Tasks 15–17's rules, sharing those modules' row builders.
  This is why `refs.rs` and `citations.rs` expose builders that take plain spans and strings, not AST nodes.

- [ ] **Step 1: Write the failing tests:**

```rust
#[test] fn parity_with_parser_on_well_formed_fixtures() {
    for f in ["refs_basic","refs_groups_listdefined","refs_reuse_order","sfn","archive_deadlink","efn"] {
        let t = common::fixture(f);
        let a = run_page(&t, "wikipedia").components;
        let b = fallback::scan(&t, &ru(), &reg());
        assert_eq!(summ_refs(&a), summ_refs(&b), "{f}");          // (kind, group, name, def_ref_id, resolved)
        assert_eq!(summ_cites(&a), summ_cites(&b), "{f}");        // (family, url, archive_url, host, dead_link, harv_key)
    }
}
#[test] fn budget_failure_falls_back_with_refs() {
    let t = common::fixture("refs_basic");
    let o = run_with(&t, Limits { parse_budget: 10, fallback: true, ..defaults() });
    assert_eq!(o.page.status, "fallback"); assert_eq!(o.components.refs.len(), 3); assert!(o.components.refs.iter().all(|r| r.fallback));
}
#[test] fn linear_time_on_adversarial_input() {
    let t = "{{a|".repeat(200_000) + &"[[".repeat(200_000) + &"<ref>".repeat(100_000);
    let s = std::time::Instant::now(); let _ = fallback::scan(&t, &ru(), &reg()); assert!(s.elapsed().as_secs_f64() < 2.0);
}
#[test] fn slow_corpus_pages_yield_refs() { /* for each public row with case "slow": run_with(budget = DEFAULT_PARSE_BUDGET); status is "ok" or "fallback"; if fallback, refs.len() > 0 whenever the markup contains "<ref" */ }
#[test] fn malformed_never_panics() { let o = run_page(&common::fixture("malformed"), "wikipedia"); assert!(matches!(o.page.status.as_str(), "ok" | "fallback")); }
```

- [ ] **Step 2: Run the tests.** `cargo test --release --test fallback`. Expected: FAIL.
- [ ] **Step 3: Implement `fallback.rs`.** Refactor `refs.rs` and `citations.rs` so that both paths share the row
  builders.
- [ ] **Step 4: Run the tests.** `cargo test --release --test fallback`, then `cargo test --release`. Expected: PASS.
- [ ] **Step 5: Commit.** `git commit -m "Fallback scanner for refs, citations, templates and sections"`.

### Task 19: Facts, symmetry and locality

**Files:**
- Create: `src/components/compare.rs`, `tests/facts.toml`, `tests/facts.rs`, `tests/symmetry.rs`,
  `tests/locality.rs`, `tests/expected/locality_allow.toml`
- Modify: `Cargo.toml` (`similar = "3"`)

**Interfaces:**
- `compare::identical_except_labels(a: &PageOutput, b: &PageOutput) -> Result<(), String>` compares every
  component table with the key and the timings ignored.
- `compare::changed_ranges(old: &str, new: &str) -> (Vec<Range<usize>>, Vec<Range<usize>>)` returns the byte
  ranges that a line diff (`similar::TextDiff::from_lines`) marks as changed in each text. An inserted line in one
  text marks the zero-width position in the other, widened to the neighbouring line.
- `compare::locality(old_text, new_text, old: &PageOutput, new: &PageOutput) -> Vec<Violation>`.
  - A component "differs" when no component in the same table of the other version has the same `fp`.
  - It is a violation when its span overlaps no changed range of its own text.
  - `Violation { table, id, fp, start, end }`.
- `tests/facts.toml`: one `[[fact]]` per page version (`page_id`, `version`), with the keys `refs_inline`,
  `refs_named`, `refs_reuse`, `refs_list_defined`, `refs_template`, `ref_lists`, `citations_by_family` (a table),
  `hosts` (a sorted list), `reuse_targets` (the sorted names that resolved reuses point to, one per reuse),
  `reuses_unresolved`, `sfn_keys` (the sorted `CITEREF` keys of resolved `{{sfn}}`s, one per use),
  `sfn_unresolved`, `sections` (a list of titles), and optionally `archived_originals` (a list of URLs). Keys
  that are absent are not checked. Infobox field counts wait for milestone 3, which brings infoboxes.
- Private facts live at `<dir of the private corpus>/parser_facts_ruwiki.toml`, in the same format, and are never
  committed.

- [ ] **Step 1: Write the facts.** Pick 9 public page versions (all three Wikipedia labels; small, medium and large;
  at least one with `{{sfn}}` and one with archive URLs) and 3 Ruwiki versions. **Read each version's markup**
  (`examples/measure.rs` is not involved; use a plain dump of the `wikitext` column), count by hand, and write the
  facts. Tell the user that the Ruwiki facts file goes next to the private corpus before writing it there.
- [ ] **Step 2: Write the failing tests.**
  - `facts.rs`: parse each listed version, and assert every present key, with the private facts behind
    `private_corpus`.
  - `symmetry.rs` (public): parse every public version with `site = "wikipedia"` and again with `"ruwiki"`, then
    `identical_except_labels`.
  - `symmetry.rs` (private): for each Ruwiki version byte-identical to the same page's `wiki_forkday` (which is
    public), the outputs are identical apart from the labels.
  - `locality.rs`:
    - public: `wiki_forkday` against `wiki_2025`, every page;
    - private: `wiki_2025` against `ruwiki_2025`;
    - over the tables `sections`, `paragraphs`, `templates`, `refs` and `citations`;
    - every violation not listed in `locality_allow.toml` (`page_id`, `versions`, `table`, `fp`, `reason`) fails.
- [ ] **Step 3: Run the tests.** `cargo test --release --test facts --test symmetry --test locality`. Expected: the
  facts tests FAIL on any mismatch, and the others FAIL until `compare.rs` exists.
- [ ] **Step 4: Implement `compare.rs` and fix what the tests find.**
  - A fact mismatch is a parser bug, unless the hand count was wrong. Re-read the markup to decide, and note which
    in the commit message.
  - A locality violation is either a parser bug, which you fix, or a real long-range effect of an edit (such as an
    unclosed tag). The latter goes in the allow file with a one-line reason. The private allow entries go in
    `tests/private/locality_allow.toml`.
- [ ] **Step 5: Run the tests.** `cargo test --release`. Expected: PASS.
- [ ] **Step 6: Commit.** `git commit -m "Facts read from markup; symmetry and locality tests"`.

### Task 20: Structured snapshots

**Files:**
- Create: `tests/snapshots.rs`, `tests/expected/structured/selected.tsv`, `tests/expected/structured/*.json`,
  `tests/expected/structured.sha256`, `tests/expected/structured_private.sha256`

**Interfaces:**
- Consumes `output::page_json`.
- `selected.tsv` lists 40 public `(page_id, version)` pairs, chosen as follows: sort the non-`largest` public
  versions by `(version, n_chars)` and take every k-th, so that each version label gets about 13 of them, plus the
  first 3 `slow` ones. The selection is generated once by an `#[ignore]` test and committed.
- Snapshot files are named `<page_id>__<version>.json`, containing `page_json` with `source_file` set to
  `"corpus"`.
- The hash files hold one line per version: `page_id\tversion\t<sha256 of page_json>`.

- [ ] **Step 1: Write the test.**
  - Selected versions are compared with their JSON file, and every other public version with its line in
    `structured.sha256`.
  - Private versions are compared by hash against `structured_private.sha256`, with a readable copy in
    `tests/private/structured/`.
  - On a mismatch, write the new output to `target/snapshots-new/` and fail with its path.
  - `UPDATE_EXPECTED=1` rewrites the files instead.
- [ ] **Step 2: Generate.** Run `UPDATE_EXPECTED=1 cargo test --release --test snapshots`. Read 5 of the JSON files
  end to end, and check them against the markup: sections, the first paragraph, the refs and the citations.
- [ ] **Step 3: Run the test.** `cargo test --release --test snapshots`. Expected: PASS.
- [ ] **Step 4: Commit.** `git commit -m "Structured snapshots: readable for 40 public versions, hashes for the rest"`.

### Task 21: Release 1.0.0

**Files:**
- Create: `SCHEMA.md`
- Modify: `README.md`, `CLAUDE.md`, `tests/schema.rs` (freeze), `docs/superpowers/specs/2026-10-27-structured-parse-design.md`
  (dated "Release 1.0.0" note with the measurements)

- [ ] **Step 1: Write `SCHEMA.md`.** Document every released table and column: type, meaning, null rules, and the
  exact normalisation behind every `fp`, `title_norm`, `harv_key` and `host`. Also list the statuses and the label
  conventions, and state the limitation that categories and refs added by templates are invisible.
- [ ] **Step 2: Freeze the schema.** In `tests/schema.rs`, `UPDATE_EXPECTED` no longer rewrites `v1.json`. Any
  difference fails, with the bump message.
- [ ] **Step 3: Measure the performance sample.** Run `parse_components` on the sample from Task 10 with
  `--jobs 12`. Record from the manifest: pages per second, the share of pages over budget, seconds over budget,
  and the status counts. Extrapolate to 866,484 pairs and to 1.4 and 1.1 million rows. If any family exceeds 8
  hours, **tell the user**. Then run `compare::locality` on the sample as a report (an `#[ignore]` test
  `locality_report_sample`, which reads the sample path from `WIKITEXT_SAMPLE`), and read it. Delete the sample
  file afterwards.
- [ ] **Step 4: Update the docs.** Add to `README.md` and `CLAUDE.md` a section on `parse_components`: what it emits,
  the labels, `SCHEMA.md`, the private-corpus env var, `cargo test --release`, and the fact that the legacy
  binaries are frozen.
- [ ] **Step 5: Verify everything.** Run `cargo test --release`, `cargo test --release --features test-hooks`, and
  `cargo build --release` with no warnings in the new code. Expected: all PASS. Without the private corpus, the
  SKIPPED lines are listed.
- [ ] **Step 6: Commit and tag.**
  `git commit -m "Release structured parse 1.0.0: pages, sections, paragraphs, templates, refs, ref lists, citations"`,
  then `git tag -a v0.2.0-alpha.1 -m "Structured parse, schema 1.0.0 (first release, milestones 0-2)"`.
  `v0.2.0` itself is reserved for milestone 6, as the spec says. Push only when the user asks.
