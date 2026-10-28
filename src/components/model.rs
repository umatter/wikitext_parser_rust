//! Row model for the structured parse. Field order here is column order in the output.
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: &str = "1.0.0";

/// The six key columns. `PageRow` and `TimingRow` repeat these fields directly (serde_arrow
/// cannot flatten); component tables get them prepended by `output::schema`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Key {
    pub source_file: String,
    pub row: i32,
    pub page_id: String,
    pub site: String,
    pub version: String,
    pub column: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct WarningRow {
    pub kind: String,
    pub start: i32,
    pub end: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct WarningCount {
    pub kind: String,
    pub n: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PageRow {
    pub source_file: String,
    pub row: i32,
    pub page_id: String,
    pub site: String,
    pub version: String,
    pub column: String,
    pub title: Option<String>,
    pub timestamp: Option<String>,
    pub status: String,
    pub status_detail: Option<String>,
    pub parse_steps: i64,
    pub n_bytes: i32,
    pub n_chars: i32,
    pub n_warnings: i32,
    pub warning_counts: Vec<WarningCount>,
    pub warnings: Vec<WarningRow>,
    pub redirect_target: Option<String>,
    pub n_sections: i32,
    pub n_paragraphs: i32,
    pub n_templates: i32,
    pub n_refs: i32,
    pub n_ref_lists: i32,
    pub n_citations: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TimingRow {
    pub source_file: String,
    pub row: i32,
    pub page_id: String,
    pub site: String,
    pub version: String,
    pub column: String,
    pub parse_ms: f64,
    pub walk_ms: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ParamRow {
    pub name: String,
    pub positional: bool,
    pub value_raw: String,
    pub value_text: String,
    pub start: i32,
    pub end: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SectionRow {
    pub id: i32,
    pub start: i32,
    pub end: i32,
    pub start_char: i32,
    pub end_char: i32,
    pub section_id: i32,
    pub para_id: Option<i32>,
    pub para_offset: Option<i32>,
    pub parent_type: Option<String>,
    pub parent_id: Option<i32>,
    pub fallback: bool,
    pub fp: String,
    pub level: i32,
    pub title_text: String,
    pub title_raw: String,
    pub title_norm: String,
    pub path: Vec<String>,
    pub parent_section_id: i32,
    pub heading_start: Option<i32>,
    pub heading_end: Option<i32>,
    pub subtree_end: i32,
    pub role: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ParagraphRow {
    pub id: i32,
    pub start: i32,
    pub end: i32,
    pub start_char: i32,
    pub end_char: i32,
    pub section_id: i32,
    pub para_id: Option<i32>,
    pub para_offset: Option<i32>,
    pub parent_type: Option<String>,
    pub parent_id: Option<i32>,
    pub fallback: bool,
    pub fp: String,
    pub kind: String,
    pub list_id: Option<i32>,
    pub item_depth: Option<i32>,
    pub text: String,
    pub n_chars: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TemplateRow {
    pub id: i32,
    pub start: i32,
    pub end: i32,
    pub start_char: i32,
    pub end_char: i32,
    pub section_id: i32,
    pub para_id: Option<i32>,
    pub para_offset: Option<i32>,
    pub parent_type: Option<String>,
    pub parent_id: Option<i32>,
    pub fallback: bool,
    pub fp: String,
    pub name_raw: String,
    pub name: String,
    pub kind: String,
    pub role: String,
    pub looks_like_infobox: bool,
    pub depth: i32,
    pub rendered_len: i32,
    pub params: Vec<ParamRow>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RefRow {
    pub id: i32,
    pub start: i32,
    pub end: i32,
    pub start_char: i32,
    pub end_char: i32,
    pub section_id: i32,
    pub para_id: Option<i32>,
    pub para_offset: Option<i32>,
    pub parent_type: Option<String>,
    pub parent_id: Option<i32>,
    pub fallback: bool,
    pub fp: String,
    pub kind: String,
    pub name: Option<String>,
    pub group: String,
    pub body_raw: String,
    pub body_text: String,
    pub def_ref_id: Option<i32>,
    pub n_uses: Option<i32>,
    pub resolved: bool,
    pub harv_key: Option<String>,
    pub cites_citation_id: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RefListRow {
    pub id: i32,
    pub start: i32,
    pub end: i32,
    pub start_char: i32,
    pub end_char: i32,
    pub section_id: i32,
    pub para_id: Option<i32>,
    pub para_offset: Option<i32>,
    pub parent_type: Option<String>,
    pub parent_id: Option<i32>,
    pub fallback: bool,
    pub fp: String,
    pub group: String,
    pub kind: String,
    pub template_id: Option<i32>,
    pub n_entries: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CitationRow {
    pub id: i32,
    pub start: i32,
    pub end: i32,
    pub start_char: i32,
    pub end_char: i32,
    pub section_id: i32,
    pub para_id: Option<i32>,
    pub para_offset: Option<i32>,
    pub parent_type: Option<String>,
    pub parent_id: Option<i32>,
    pub fallback: bool,
    pub fp: String,
    pub context: String,
    pub ref_id: Option<i32>,
    pub family: String,
    pub template_id: Option<i32>,
    pub template_name: Option<String>,
    pub params: Vec<ParamRow>,
    pub url: Option<String>,
    pub archive_url: Option<String>,
    pub archive_date_raw: Option<String>,
    pub host: Option<String>,
    pub registrable_domain: Option<String>,
    pub dead_link: bool,
    pub title: Option<String>,
    pub authors: Vec<String>,
    pub date_raw: Option<String>,
    pub year: Option<i32>,
    pub publisher: Option<String>,
    pub work: Option<String>,
    pub language: Option<String>,
    pub isbn: Option<String>,
    pub doi: Option<String>,
    pub access_date_raw: Option<String>,
    pub harv_key: Option<String>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Components {
    pub sections: Vec<SectionRow>,
    pub paragraphs: Vec<ParagraphRow>,
    pub templates: Vec<TemplateRow>,
    pub refs: Vec<RefRow>,
    pub ref_lists: Vec<RefListRow>,
    pub citations: Vec<CitationRow>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PageOutput {
    pub page: PageRow,
    pub timing: TimingRow,
    pub components: Components,
}
