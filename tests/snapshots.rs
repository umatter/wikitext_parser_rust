//! Structured snapshots: every corpus version's `page_json` is pinned.
//!
//! * 40 public versions (`tests/expected/structured/selected.tsv`) have a readable JSON file.
//! * Every public version has a line `page_id\tversion\tsha256` in `tests/expected/structured.sha256`.
//! * Private versions have lines in `tests/expected/structured_private.sha256` and readable copies in the
//!   gitignored `tests/private/structured/`. Private failures name page id, version and path only.
//!
//! `UPDATE_EXPECTED=1` rewrites the expected files. The `#[ignore]` test `generate_selection` writes
//! `selected.tsv` once.
mod common;

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use wikitext_components::corpus::CorpusRow;
use wikitext_components::model::{Key, PageOutput};
use wikitext_components::output;
use wikitext_components::page::{self, PageInput};

const SELECTED: &str = "tests/expected/structured/selected.tsv";
const JSON_DIR: &str = "tests/expected/structured";
const HASHES: &str = "tests/expected/structured.sha256";
const PRIVATE_HASHES: &str = "tests/expected/structured_private.sha256";
const PRIVATE_DIR: &str = "tests/private/structured";
const PRIVATE_NEW: &str = "tests/private/snapshots-new";
const PUBLIC_NEW: &str = "target/snapshots-new";
const SIZE_CAP: usize = 15_000_000;

type Id = (String, String);

fn id(r: &CorpusRow) -> Id {
    (r.page_id.clone(), r.version.clone())
}

fn file_name(i: &Id) -> String {
    format!("{}__{}.json", i.0, i.1)
}

fn sha(s: &str) -> String {
    Sha256::digest(s.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

/// `page_json` of every row, in row order, with `source_file` set to `corpus`.
fn render(rows: &[CorpusRow], site: &str) -> Vec<String> {
    let (settings, registry, limits) = (common::settings_for(site), common::reg(), common::defaults());
    rows.iter()
        .enumerate()
        .map(|(i, r)| {
            let input = PageInput {
                key: Key {
                    source_file: "corpus".into(),
                    row: i as i32,
                    page_id: r.page_id.clone(),
                    site: site.into(),
                    version: r.version.clone(),
                    column: "text".into(),
                },
                title: Some(r.page_title.clone()),
                timestamp: None,
                text: Some(r.wikitext.clone()),
            };
            let out: PageOutput = page::process_page(&input, &settings, &registry, &limits);
            output::page_json(&out)
        })
        .collect()
}

fn read_selected() -> Vec<Id> {
    let text = std::fs::read_to_string(SELECTED).unwrap_or_else(|e| panic!("{SELECTED}: {e} (run generate_selection)"));
    let mut lines = text.lines();
    assert_eq!(lines.next(), Some("page_id\tversion"), "{SELECTED} header");
    lines
        .map(|l| {
            let (a, b) = l.split_once('\t').unwrap_or_else(|| panic!("bad line {l:?} in {SELECTED}"));
            (a.to_string(), b.to_string())
        })
        .collect()
}

fn read_hashes(path: &str) -> BTreeMap<Id, String> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e} (run with UPDATE_EXPECTED=1)"));
    text.lines()
        .map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            assert_eq!(f.len(), 3, "bad line in {path}");
            ((f[0].to_string(), f[1].to_string()), f[2].to_string())
        })
        .collect()
}

fn hash_text(ids: &[Id], jsons: &[String]) -> String {
    let mut lines: Vec<String> = ids.iter().zip(jsons).map(|(i, j)| format!("{}\t{}\t{}\n", i.0, i.1, sha(j))).collect();
    lines.sort();
    lines.concat()
}

fn write(path: &Path, text: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
}

fn json_files(dir: &str) -> BTreeSet<String> {
    match std::fs::read_dir(dir) {
        Ok(d) => d
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".json"))
            .collect(),
        Err(_) => BTreeSet::new(),
    }
}

fn updating() -> bool {
    std::env::var("UPDATE_EXPECTED").is_ok_and(|v| v == "1")
}

fn unique(ids: &[Id]) {
    let set: BTreeSet<&Id> = ids.iter().collect();
    assert_eq!(set.len(), ids.len(), "duplicate (page_id, version) in the corpus");
}

#[test]
fn public_structured_output_matches_snapshots() {
    let rows = common::public_corpus();
    let ids: Vec<Id> = rows.iter().map(id).collect();
    unique(&ids);
    let jsons = render(&rows, "wikipedia");
    let selected = read_selected();
    let sel: BTreeSet<Id> = selected.iter().cloned().collect();
    assert_eq!(sel.len(), selected.len(), "duplicate rows in {SELECTED}");

    if updating() {
        let known: BTreeSet<&Id> = ids.iter().collect();
        assert!(selected.iter().all(|s| known.contains(s)), "selected.tsv names a version not in the corpus");
        for n in json_files(JSON_DIR) {
            std::fs::remove_file(Path::new(JSON_DIR).join(n)).unwrap();
        }
        let mut written = 0;
        for (i, j) in ids.iter().zip(&jsons) {
            if sel.contains(i) {
                write(&Path::new(JSON_DIR).join(file_name(i)), &format!("{j}\n"));
                written += 1;
            }
        }
        write(Path::new(HASHES), &hash_text(&ids, &jsons));
        eprintln!("UPDATE_EXPECTED: rewrote {} files ({written} JSON, {HASHES})", written + 1);
        return;
    }

    let mut bad: Vec<String> = Vec::new();
    let new_dir = PathBuf::from(PUBLIC_NEW);
    let mut fail = |i: &Id, j: &str, what: &str| {
        let p = new_dir.join(file_name(i));
        write(&p, &format!("{j}\n"));
        bad.push(format!("{} {}: {what}; new output in {}", i.0, i.1, p.display()));
    };

    // Readable snapshots.
    for (i, j) in ids.iter().zip(&jsons) {
        if !sel.contains(i) {
            continue;
        }
        match std::fs::read_to_string(Path::new(JSON_DIR).join(file_name(i))) {
            Err(_) => fail(i, j, "snapshot file missing"),
            Ok(want) if want != format!("{j}\n") => fail(i, j, "differs from snapshot"),
            Ok(_) => {}
        }
    }
    // Hashes.
    let want = read_hashes(HASHES);
    let got: BTreeSet<&Id> = ids.iter().collect();
    for (i, j) in ids.iter().zip(&jsons) {
        match want.get(i) {
            None => fail(i, j, "no hash line"),
            Some(h) if *h != sha(j) => fail(i, j, "hash differs"),
            Some(_) => {}
        }
    }
    for i in want.keys() {
        if !got.contains(i) {
            bad.push(format!("{} {}: hash line for a version no longer in the corpus", i.0, i.1));
        }
    }
    // Stale or missing selection.
    for s in &selected {
        if !got.contains(s) {
            bad.push(format!("{} {}: selected but not in the corpus", s.0, s.1));
        }
    }
    let expected_files: BTreeSet<String> = selected.iter().map(file_name).collect();
    for n in json_files(JSON_DIR) {
        if !expected_files.contains(&n) {
            bad.push(format!("stale snapshot file {JSON_DIR}/{n}"));
        }
    }
    assert!(bad.is_empty(), "{} snapshot problems:\n{}", bad.len(), bad.join("\n"));
}

#[test]
fn private_structured_output_matches_hashes() {
    let Some(rows) = common::private_corpus("private_structured_output_matches_hashes") else {
        return;
    };
    let ids: Vec<Id> = rows.iter().map(id).collect();
    unique(&ids);
    let jsons = render(&rows, "ruwiki");

    if updating() {
        for n in json_files(PRIVATE_DIR) {
            std::fs::remove_file(Path::new(PRIVATE_DIR).join(n)).unwrap();
        }
        for (i, j) in ids.iter().zip(&jsons) {
            write(&Path::new(PRIVATE_DIR).join(file_name(i)), &format!("{j}\n"));
        }
        write(Path::new(PRIVATE_HASHES), &hash_text(&ids, &jsons));
        eprintln!("UPDATE_EXPECTED: rewrote {} files ({} private JSON, {PRIVATE_HASHES})", jsons.len() + 1, jsons.len());
        return;
    }

    let want = read_hashes(PRIVATE_HASHES);
    let mut bad: Vec<String> = Vec::new();
    for (i, j) in ids.iter().zip(&jsons) {
        let what = match want.get(i) {
            None => "no hash line",
            Some(h) if *h != sha(j) => "hash differs",
            Some(_) => continue,
        };
        let p = Path::new(PRIVATE_NEW).join(file_name(i));
        write(&p, &format!("{j}\n"));
        bad.push(format!("{} {}: {what}; new output in {}", i.0, i.1, p.display()));
    }
    let got: BTreeSet<&Id> = ids.iter().collect();
    for i in want.keys() {
        if !got.contains(i) {
            bad.push(format!("{} {}: hash line for a version no longer in the corpus", i.0, i.1));
        }
    }
    assert!(bad.is_empty(), "{} private snapshot problems:\n{}", bad.len(), bad.join("\n"));
}

/// Writes `selected.tsv`: per label (in label order) 13, 12 and 12 evenly spaced non-`largest` versions
/// ordered by `(version, n_chars)`, then the first three `slow` versions by `(page_id, version)` not
/// already chosen. Swaps the largest chosen version for its next smaller neighbour while the readable
/// snapshots exceed 15 MB.
#[test]
#[ignore]
fn generate_selection() {
    let rows = common::public_corpus();
    let jsons: BTreeMap<Id, String> = rows.iter().map(id).zip(render(&rows, "wikipedia")).collect();
    let size = |i: &Id| jsons[i].len() + 1;

    let mut labels: BTreeMap<String, Vec<&CorpusRow>> = BTreeMap::new();
    for r in rows.iter().filter(|r| r.case.as_deref() != Some("largest")) {
        labels.entry(r.version.clone()).or_default().push(r);
    }
    assert_eq!(labels.len(), 3, "three public labels");
    let mut ordered: BTreeMap<String, Vec<Id>> = BTreeMap::new();
    let mut chosen: Vec<(String, Id)> = Vec::new(); // (label, id)
    for ((label, list), k) in labels.iter_mut().zip([13usize, 12, 12]) {
        list.sort_by_key(|r| (r.version.clone(), r.wikitext.chars().count(), r.page_id.clone()));
        let n = list.len();
        assert!(n >= k);
        // Midpoint of each of k equal slices.
        let ids: Vec<Id> = list.iter().map(|r| id(r)).collect();
        for t in 0..k {
            chosen.push((label.clone(), ids[(2 * t + 1) * n / (2 * k)].clone()));
        }
        ordered.insert(label.clone(), ids);
    }
    assert_eq!(chosen.len(), 37);
    let mut slow: Vec<Id> = rows.iter().filter(|r| r.case.as_deref() == Some("slow")).map(id).collect();
    slow.sort();
    let mut extra = 0;
    for s in slow {
        if extra == 3 {
            break;
        }
        if !chosen.iter().any(|(_, c)| *c == s) {
            chosen.push(("slow".into(), s));
            extra += 1;
        }
    }
    assert_eq!(chosen.len(), 40);

    loop {
        let total: usize = chosen.iter().map(|(_, i)| size(i)).sum();
        eprintln!("readable snapshots: {total} bytes");
        if total <= SIZE_CAP {
            break;
        }
        let (k, _) = chosen.iter().enumerate().filter(|(_, (l, _))| l != "slow").max_by_key(|(_, (_, i))| size(i)).unwrap();
        let (label, old) = chosen[k].clone();
        let list = &ordered[&label];
        let pos = list.iter().position(|i| *i == old).unwrap();
        let next = (0..pos).rev().map(|p| &list[p]).find(|c| !chosen.iter().any(|(_, x)| x == *c)).expect("a smaller neighbour");
        eprintln!("SWAP over cap: {} {} ({} bytes) -> {} {} ({} bytes)", old.0, old.1, size(&old), next.0, next.1, size(next));
        chosen[k].1 = next.clone();
    }
    let set: BTreeSet<&Id> = chosen.iter().map(|(_, i)| i).collect();
    assert_eq!(set.len(), 40);
    let mut out = String::from("page_id\tversion\n");
    let mut all: Vec<&Id> = set.into_iter().collect();
    all.sort();
    for i in all {
        out.push_str(&format!("{}\t{}\n", i.0, i.1));
    }
    write(Path::new(SELECTED), &out);
}
