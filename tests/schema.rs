use arrow::datatypes::{DataType, Field, Schema};
use serde_json::{json, Value};
use wikitext_components::model::SCHEMA_VERSION;
use wikitext_components::output;

fn field_json(f: &Field) -> Value {
    let mut o = serde_json::Map::new();
    o.insert("name".into(), json!(f.name()));
    o.insert("nullable".into(), json!(f.is_nullable()));
    let mut meta: Vec<_> = f.metadata().iter().collect();
    meta.sort();
    let children: Vec<Value> = match f.data_type() {
        DataType::List(c) | DataType::LargeList(c) => {
            o.insert("type".into(), json!("List"));
            vec![field_json(c)]
        }
        DataType::Struct(fs) => {
            o.insert("type".into(), json!("Struct"));
            fs.iter().map(|c| field_json(c)).collect()
        }
        other => {
            o.insert("type".into(), json!(format!("{other:?}")));
            vec![]
        }
    };
    if !children.is_empty() {
        o.insert("children".into(), Value::Array(children));
    }
    o.insert("metadata".into(), json!(meta));
    Value::Object(o)
}

fn schema_json(s: &Schema) -> Value {
    let mut meta: Vec<_> = s.metadata().iter().collect();
    meta.sort();
    json!({ "metadata": meta, "fields": s.fields().iter().map(|f| field_json(f)).collect::<Vec<_>>() })
}

fn walk(f: &Field, table: &str) {
    match f.data_type() {
        DataType::UInt64 => panic!("{table}.{}: UInt64", f.name()),
        DataType::Map(..) => panic!("{table}.{}: Map", f.name()),
        DataType::List(c) | DataType::LargeList(c) => walk(c, table),
        DataType::Struct(fs) => fs.iter().for_each(|c| walk(c, table)),
        _ => {}
    }
}

/// Schema 1.0.0 is frozen (Task 21): `tests/schema/v1.json` is never rewritten, `UPDATE_EXPECTED` included,
/// and any difference fails. A later schema bumps `SCHEMA_VERSION` and is pinned in a file of its own.
#[test]
fn schemas_match_frozen_snapshot() {
    let got: Value = Value::Object(
        output::TABLES
            .iter()
            .map(|t| (t.to_string(), schema_json(&output::schema(t))))
            .collect(),
    );
    let path = "tests/schema/v1.json";
    if std::env::var("UPDATE_EXPECTED").is_ok() {
        eprintln!("UPDATE_EXPECTED is ignored here: {path} is frozen at schema 1.0.0");
    }
    let want: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(
        got, want,
        "schema changed: bump SCHEMA_VERSION (minor: added column/table; major: rename/remove/meaning); \
         {path} is frozen at 1.0.0 and is never rewritten"
    );
}

#[test]
fn no_unsigned_64_or_map_types() {
    for t in output::TABLES {
        output::schema(t).fields().iter().for_each(|f| walk(f, t));
    }
}

#[test]
fn keys_first_and_version_everywhere() {
    let keys = ["source_file", "row", "page_id", "site", "version", "column"];
    for t in output::TABLES {
        let s = output::schema(t);
        let names: Vec<_> = s.fields().iter().take(6).map(|f| f.name().as_str()).collect();
        assert_eq!(names, keys, "{t}");
        for f in s.fields().iter() {
            assert_eq!(f.metadata().get("wikitext_parser.schema_version").map(String::as_str), Some(SCHEMA_VERSION), "{t}.{}", f.name());
        }
        assert_eq!(s.metadata().get("wikitext_parser.schema_version").map(String::as_str), Some(SCHEMA_VERSION));
        for k in ["source_file", "site", "version", "column"] {
            assert!(matches!(s.field_with_name(k).unwrap().data_type(), DataType::Dictionary(..)), "{t}.{k}");
        }
        assert_eq!(s.field_with_name("row").unwrap().data_type(), &DataType::Int32);
        assert_eq!(s.field_with_name("page_id").unwrap().data_type(), &DataType::Utf8);
    }
}
