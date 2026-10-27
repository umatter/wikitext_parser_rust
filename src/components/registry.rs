//! Template and section registry. Later tasks fill it from `config/registry/`; the default has no entries.
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub struct TemplateEntry {
    pub role: String,
    pub family: Option<String>,
    pub archive_wrapper: bool,
    pub fields: BTreeMap<String, Vec<String>>,
    pub site: Option<String>,
}

#[derive(Default, Clone, Debug)]
pub struct Registry {
    templates: BTreeMap<(String, String), TemplateEntry>,
    sections: BTreeMap<String, String>,
}

impl Registry {
    /// The entry for a normalised template name on a site, if any.
    pub fn template(&self, name: &str, site: &str) -> Option<&TemplateEntry> {
        self.templates.get(&(site.to_string(), name.to_string()))
    }

    /// The role of a normalised section title; `"content"` when nothing matches.
    pub fn section_role(&self, title_norm: &str) -> &str {
        self.sections.get(title_norm).map(String::as_str).unwrap_or("content")
    }
}
