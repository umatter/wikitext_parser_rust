//! The watchdog (built with `--features test-hooks`): a page over the limit is quarantined and the
//! process aborts; `--resume` then gives that page `status = "hang"` and parses the rest.
use std::fs::{self, File};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

const BIN: &str = env!("CARGO_BIN_EXE_parse_components");

fn column(path: &Path, name: &str) -> Vec<String> {
    let r = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap().build().unwrap();
    let mut out = vec![];
    for b in r {
        let b = b.unwrap();
        let c = cast(b.column(b.schema().index_of(name).unwrap()), &DataType::Utf8).unwrap();
        let c = c.as_any().downcast_ref::<StringArray>().unwrap();
        out.extend(c.iter().map(|x| x.unwrap_or("").to_string()));
    }
    out
}

/// A fresh directory holding `hangs.parquet`: three pages, page_id 1 to 3, texts "a", "b" and "c".
fn three_pages(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("wtc_watchdog_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    let input = d.join("hangs.parquet");
    let cols: Vec<(&str, ArrayRef)> = vec![
        ("page_id", Arc::new(StringArray::from(vec!["1", "2", "3"]))),
        ("text", Arc::new(StringArray::from(vec!["a", "b", "c"]))),
    ];
    let schema = Arc::new(Schema::new(
        cols.iter().map(|(n, a)| Field::new(*n, a.data_type().clone(), true)).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), cols.into_iter().map(|(_, a)| a).collect()).unwrap();
    let mut w = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    d
}

#[test]
fn hanging_page_is_quarantined_then_marked_hang_on_resume() {
    let d = three_pages("hang");
    let input = d.join("hangs.parquet");
    let out = d.join("out");
    let base = [
        "--input", input.to_str().unwrap(), "--label", "text=wikipedia:v", "--out", out.to_str().unwrap(),
        "--jobs", "2",
    ];
    let first = Command::new(BIN)
        .args(base)
        .env("WIKITEXT_WATCHDOG_SECS", "1")
        .env("WIKITEXT_TEST_SLEEP_PAGE", "2")
        .output()
        .unwrap();
    assert!(!first.status.success(), "the watchdog must abort the process");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(first.status.signal(), Some(6), "aborted with SIGABRT");
    }
    assert_eq!(fs::read_to_string(out.join("quarantine.list")).unwrap(), "hangs\t1\ttext\n");
    assert!(String::from_utf8_lossy(&first.stderr).contains("watchdog"));
    assert!(!out.join("pages").join("hangs.parquet").exists(), "an aborted input has no finished part");

    let second = Command::new(BIN)
        .args(base)
        .arg("--resume")
        .env("WIKITEXT_WATCHDOG_SECS", "1")
        .env_remove("WIKITEXT_TEST_SLEEP_PAGE")
        .output()
        .unwrap();
    assert!(second.status.success(), "{}", String::from_utf8_lossy(&second.stderr));
    let pages = out.join("pages").join("hangs.parquet");
    assert_eq!(column(&pages, "status"), ["ok", "hang", "ok"]);
    assert_eq!(column(&pages, "page_id"), ["1", "2", "3"]);
    let detail = column(&out.join("pages").join("hangs.parquet"), "status_detail");
    assert!(detail[1].starts_with("hang: "), "{}", detail[1]);
    let _ = fs::remove_dir_all(&d);
}

/// A quarantined page runs watched too: the fallback scan `--resume` gives it is work the watchdog bounds. The
/// test hook makes page 2 run past the limit, here in its quarantined run.
#[test]
fn quarantined_page_runs_watched() {
    let d = three_pages("quarantined");
    let input = d.join("hangs.parquet");
    let out = d.join("out");
    fs::create_dir_all(&out).unwrap();
    fs::write(out.join("quarantine.list"), "hangs\t1\ttext\n").unwrap();
    let run = Command::new(BIN)
        .args(["--input", input.to_str().unwrap(), "--label", "text=wikipedia:v", "--out", out.to_str().unwrap()])
        .args(["--jobs", "2", "--resume"])
        .env("WIKITEXT_WATCHDOG_SECS", "1")
        .env("WIKITEXT_TEST_SLEEP_PAGE", "2")
        .output()
        .unwrap();
    assert!(!run.status.success(), "the watchdog must abort a quarantined page over the limit");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(run.status.signal(), Some(6), "aborted with SIGABRT");
    }
    assert!(String::from_utf8_lossy(&run.stderr).contains("watchdog: page hangs 1 text"));
    assert!(!out.join("pages").join("hangs.parquet").exists(), "an aborted input has no finished part");
    let _ = fs::remove_dir_all(&d);
}
