use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampSecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use wikitext_components::input::{open, parse_label, wiki_file, Label, Layout};

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("wtc_input_{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d.join(name)
}

fn strs(v: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(v.to_vec()))
}

fn write(path: &PathBuf, cols: Vec<(&str, ArrayRef)>) {
    let fields: Vec<Field> =
        cols.iter().map(|(n, a)| Field::new(*n, a.data_type().clone(), true)).collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), cols.into_iter().map(|(_, a)| a).collect()).unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn label(s: &str) -> Label {
    parse_label(s).unwrap()
}

fn all(path: &PathBuf, labels: &[Label], rows: usize) -> Vec<wikitext_components::page::PageInput> {
    let mut f = open(path, None, None, labels).unwrap();
    f.batches(rows).flat_map(|b| b.unwrap()).collect()
}

#[test]
fn pair_layout_yields_two_versions_per_row_in_order() {
    let p = tmp("pair.parquet");
    write(&p, vec![
        ("page_id", Arc::new(Int64Array::from(vec![1, 2]))),
        ("page_title", strs(&[Some("A"), Some("B")])),
        ("official_text", strs(&[Some("oa"), Some("ob")])),
        ("official_timestamp", Arc::new(TimestampSecondArray::from(vec![1_735_689_600, 1_735_689_601]))),
        ("clone_page_title", strs(&[Some("A2"), Some("B2")])),
        ("clone_text", strs(&[Some("ca"), None])),
        ("clone_timestamp", strs(&[Some("2024-12-28T01:38:34Z"), None])),
    ]);
    let labels = [label("official_text=wikipedia:2025-01-01"), label("clone_text=ruwiki:2025-01-01")];
    let v = all(&p, &labels, 1);
    let got: Vec<_> = v
        .iter()
        .map(|x| (x.key.row, x.key.page_id.as_str(), x.key.site.as_str(), x.key.column.as_str(), x.title.clone(), x.timestamp.clone(), x.text.clone()))
        .collect();
    assert_eq!(got.len(), 4);
    assert_eq!(got[0], (0, "1", "wikipedia", "official_text", Some("A".into()), Some("2025-01-01T00:00:00Z".into()), Some("oa".into())));
    assert_eq!(got[1], (0, "1", "ruwiki", "clone_text", Some("A2".into()), Some("2024-12-28T01:38:34Z".into()), Some("ca".into())));
    assert_eq!((got[2].0, got[2].3), (1, "official_text"));
    assert_eq!((got[3].0, got[3].3, got[3].6.clone()), (1, "clone_text", None));
    assert_eq!(v[0].key.source_file, "pair");
}

#[test]
fn long_layout_maps_version_values() {
    let p = tmp("long.parquet");
    write(&p, vec![
        ("page_id", strs(&[Some("5"), Some("6")])),
        ("page_title", strs(&[Some("A"), Some("B")])),
        ("version", strs(&[Some("wiki_forkday"), Some("wiki_2025")])),
        ("timestamp", strs(&[Some("t1"), Some("t2")])),
        ("wikitext", strs(&[Some("x"), Some("y")])),
    ]);
    let labels = [label("wiki_forkday=wikipedia:forkday"), label("wiki_2025=wikipedia:2025-01-01")];
    let v = all(&p, &labels, 10);
    assert_eq!(v[0].key.version, "forkday");
    assert_eq!(v[1].key.version, "2025-01-01");
    assert_eq!(v[1].key.site, "wikipedia");
    assert_eq!(v[1].key.column, "wikitext");
    assert_eq!(v[1].timestamp.as_deref(), Some("t2"));
}

#[test]
fn duplicate_page_ids_are_kept_apart_by_row() {
    let p = tmp("dup.parquet");
    write(&p, vec![
        ("page_id", strs(&[Some("7"), Some("7")])),
        ("text", strs(&[Some("a"), Some("b")])),
    ]);
    let v = all(&p, &[label("text=wikipedia:v")], 1);
    assert_eq!(v.len(), 2);
    assert_eq!((v[0].key.row, v[1].key.row), (0, 1));
    assert_eq!(v[0].key.page_id, v[1].key.page_id);
    assert_eq!(v[0].timestamp, None);
}

#[test]
fn missing_and_unmatched_labels_are_errors() {
    let p = tmp("lab.parquet");
    write(&p, vec![
        ("page_id", strs(&[Some("1"), Some("2")])),
        ("version", strs(&[Some("a"), Some("b")])),
        ("wikitext", strs(&[Some("x"), Some("y")])),
    ]);
    let err = |labels: &[Label]| open(&p, None, None, labels).err().unwrap().to_string();
    assert_eq!(err(&[label("a=s:1")]), "no --label for 'b'");
    assert_eq!(err(&[label("a=s:1"), label("b=s:2"), label("c=s:3")]), "--label 'c' matches no column or version");

    let q = tmp("lab_single.parquet");
    write(&q, vec![("page_id", strs(&[Some("1")])), ("text", strs(&[Some("x")]))]);
    assert_eq!(open(&q, None, None, &[]).err().unwrap().to_string(), "no --label for 'text'");
    assert_eq!(
        open(&q, None, None, &[label("text=s:1"), label("nope=s:1")]).err().unwrap().to_string(),
        "--label 'nope' matches no column or version"
    );
    // A key given twice is an error, even when both labels agree: which one counts would be a guess.
    for twice in [[label("text=s:1"), label("text=r:2")], [label("text=s:1"), label("text=s:1")]] {
        assert_eq!(
            open(&q, None, None, &twice).err().unwrap().to_string(),
            "--label 'text' is given twice; give each key one label"
        );
    }
}

#[test]
fn arrow_timestamp_becomes_rfc3339() {
    let p = tmp("ts.parquet");
    write(&p, vec![
        ("page_id", strs(&[Some("1")])),
        ("text", strs(&[Some("x")])),
        ("timestamp", Arc::new(TimestampSecondArray::from(vec![1_735_689_600]))),
    ]);
    let v = all(&p, &[label("text=s:1")], 10);
    assert_eq!(v[0].timestamp.as_deref(), Some("2025-01-01T00:00:00Z"));
}

#[test]
fn unsupported_column_type_is_named() {
    let p = tmp("bad.parquet");
    write(&p, vec![
        ("page_id", Arc::new(arrow::array::Float64Array::from(vec![1.0]))),
        ("text", strs(&[Some("x")])),
    ]);
    let e = open(&p, None, None, &[label("text=s:1")]).err().unwrap().to_string();
    assert!(e.contains("page_id") && e.contains("Float64"), "{e}");
    let _ = (DataType::Utf8, TimeUnit::Second);
}

#[test]
fn wiki_file_uses_the_stem() {
    let p = tmp("12345.wiki");
    std::fs::write(&p, "== H ==\n").unwrap();
    let x = wiki_file(&p, &label("wikipedia:2025-01-01")).unwrap();
    assert_eq!((x.key.page_id.as_str(), x.key.row, x.key.column.as_str()), ("12345", 0, "wikitext"));
    assert_eq!(x.key.source_file, "12345");
    assert_eq!(x.text.as_deref(), Some("== H ==\n"));
    assert!(x.title.is_none() && x.timestamp.is_none());
}

#[test]
fn corpus_is_long_layout() {
    let p = PathBuf::from("tests/corpus/wikipedia_v1.parquet");
    let labels = [
        label("wiki_forkday=wikipedia:forkday"),
        label("wiki_2025=wikipedia:2025-01-01"),
        label("deleted_wiki_2025=wikipedia:2025-01-01"),
    ];
    let mut f = open(&p, None, None, &labels).unwrap();
    assert_eq!(f.layout(), Layout::Long);
    let first = f.batches(3).next().unwrap().unwrap();
    assert_eq!(first.len(), 3);
}

fn collect(path: &PathBuf, labels: &[Label]) -> Vec<wikitext_components::page::PageInput> {
    all(path, labels, 2)
}

#[test]
fn null_page_id_flows_on_as_empty_string() {
    let p = tmp("nullid_single.parquet");
    write(&p, vec![
        ("page_id", strs(&[Some("1"), None, Some("3")])),
        ("text", strs(&[Some("a"), Some("b"), Some("c")])),
    ]);
    let v = collect(&p, &[label("text=s:1")]);
    let ids: Vec<_> = v.iter().map(|x| (x.key.row, x.key.page_id.as_str())).collect();
    assert_eq!(ids, [(0, "1"), (1, ""), (2, "3")]);

    let q = tmp("nullid_pair.parquet");
    write(&q, vec![
        ("page_id", strs(&[Some("1"), None, Some("3")])),
        ("official_text", strs(&[Some("a"), Some("b"), Some("c")])),
        ("clone_text", strs(&[Some("a"), Some("b"), Some("c")])),
    ]);
    let v = collect(&q, &[label("official_text=s:1"), label("clone_text=r:1")]);
    assert_eq!(v.len(), 6);
    assert_eq!((v[2].key.row, v[2].key.page_id.as_str()), (1, ""));
    assert_eq!((v[3].key.row, v[3].key.page_id.as_str()), (1, ""));
    assert_eq!(v[5].key.page_id, "3");

    let l = tmp("nullid_long.parquet");
    write(&l, vec![
        ("page_id", strs(&[Some("1"), None, Some("3")])),
        ("version", strs(&[Some("a"), Some("a"), Some("a")])),
        ("wikitext", strs(&[Some("x"), Some("y"), Some("z")])),
    ]);
    let v = collect(&l, &[label("a=s:1")]);
    assert_eq!(v.len(), 3);
    assert_eq!((v[1].key.row, v[1].key.page_id.as_str()), (1, ""));
    assert_eq!(v[2].key.page_id, "3");
}

#[test]
fn null_version_is_rejected_up_front() {
    let p = tmp("nullver.parquet");
    write(&p, vec![
        ("page_id", strs(&[Some("1"), Some("2"), Some("3")])),
        ("version", strs(&[Some("a"), Some("a"), None])),
        ("wikitext", strs(&[Some("x"), Some("y"), Some("z")])),
    ]);
    let e = open(&p, None, None, &[label("a=s:1")]).err().unwrap().to_string();
    assert_eq!(e, "null value in version column at row 2");
}

#[test]
fn timestamp_units_and_large_utf8() {
    use arrow::array::{
        LargeStringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray,
    };
    let base: i64 = 1_735_689_600;
    let cases: Vec<(ArrayRef, [&str; 2])> = vec![
        (
            Arc::new(TimestampMillisecondArray::from(vec![base * 1000, base * 1000 + 500])),
            ["2025-01-01T00:00:00Z", "2025-01-01T00:00:00.5Z"],
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![base * 1_000_000, base * 1_000_000 + 250])),
            ["2025-01-01T00:00:00Z", "2025-01-01T00:00:00.00025Z"],
        ),
        (
            Arc::new(TimestampNanosecondArray::from(vec![base * 1_000_000_000, base * 1_000_000_000 + 1])),
            ["2025-01-01T00:00:00Z", "2025-01-01T00:00:00.000000001Z"],
        ),
    ];
    for (i, (arr, want)) in cases.into_iter().enumerate() {
        let p = tmp(&format!("units{i}.parquet"));
        write(&p, vec![
            ("page_id", strs(&[Some("1"), Some("2")])),
            ("text", strs(&[Some("a"), Some("b")])),
            ("timestamp", arr),
        ]);
        let v = collect(&p, &[label("text=s:1")]);
        assert_eq!(v[0].timestamp.as_deref(), Some(want[0]));
        assert_eq!(v[1].timestamp.as_deref(), Some(want[1]));
    }

    let p = tmp("large.parquet");
    write(&p, vec![
        ("page_id", Arc::new(arrow::array::LargeStringArray::from(vec!["9"]))),
        ("title", Arc::new(LargeStringArray::from(vec!["T"]))),
        ("text", Arc::new(LargeStringArray::from(vec!["body"]))),
    ]);
    let v = collect(&p, &[label("text=s:1")]);
    assert_eq!(v[0].key.page_id, "9");
    assert_eq!(v[0].title.as_deref(), Some("T"));
    assert_eq!(v[0].text.as_deref(), Some("body"));
}

#[test]
fn mid_stream_error_is_yielded_not_swallowed() {
    let p = tmp("corrupt.parquet");
    write(&p, vec![
        ("page_id", strs(&[Some("1"), Some("2")])),
        ("text", strs(&[Some("QQQQQQQQQQ"), Some("RRRRRRRRRR")])),
    ]);
    let mut bytes = std::fs::read(&p).unwrap();
    let pos = bytes.windows(10).position(|w| w == b"QQQQQQQQQQ").unwrap();
    // Plain encoding: the four bytes before a value are its length. Make it absurd.
    for b in &mut bytes[pos - 4..pos] {
        *b = 0xFF;
    }
    std::fs::write(&p, bytes).unwrap();
    let mut f = open(&p, None, None, &[label("text=s:1")]).unwrap();
    let items: Vec<_> = f.batches(10).collect();
    assert!(!items.is_empty());
    assert!(items.last().unwrap().is_err(), "the error must be yielded");
}
