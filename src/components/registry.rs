//! Template and section registry, loaded from `config/registry/{templates,sections}.toml`. It says which
//! templates are citations, footnotes, ref lists and link-status markers, which parameters fill each
//! citation field, and which section titles have which role. The default registry has no entries.
use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::fingerprint;
use crate::model::PageOutput;
use crate::settings::normalize_template_name;

pub const ROLES: &[&str] = &[
    "infobox", "citation", "footnote", "ref_list", "link_status", "inline_text", "interlanguage", "navigation",
    "maintenance", "other",
];
pub const FIELD_KEYS: &[&str] = &[
    "url", "archive_url", "archive_date_raw", "title", "authors", "date_raw", "year", "publisher", "work",
    "language", "isbn", "doi", "access_date_raw", "harv_ref", "harv_names",
];
pub const SECTION_ROLES: &[&str] = &["notes", "bibliography", "external_links", "see_also"];
const SITES: &[&str] = &["wikipedia", "ruwiki"];

#[derive(Clone, Debug, PartialEq)]
pub struct TemplateEntry {
    /// The entry's name, normalised.
    pub name: String,
    pub role: String,
    pub family: Option<String>,
    pub archive_wrapper: bool,
    pub fields: BTreeMap<String, Vec<String>>,
    pub site: Option<String>,
}

#[derive(Default, Clone, Debug)]
pub struct Registry {
    entries: Vec<TemplateEntry>,
    /// (site scope, normalised name or alias) -> index into `entries`. The scope is `""` for an entry
    /// that applies to both sites.
    index: BTreeMap<(String, String), usize>,
    /// (entry name, alias as written in the file), in file order.
    aliases: Vec<(String, String)>,
    sections: BTreeMap<String, String>,
    hash: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TemplatesFile {
    #[serde(default)]
    template: Vec<RawTemplate>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTemplate {
    name: String,
    #[serde(default)]
    aliases: Vec<String>,
    role: String,
    family: Option<String>,
    #[serde(default)]
    archive_wrapper: bool,
    site: Option<String>,
    #[serde(default)]
    fields: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SectionsFile {
    #[serde(default)]
    section: Vec<RawSection>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSection {
    role: String,
    titles: Vec<String>,
}

fn read(path: &Path) -> Result<(String, Vec<u8>)> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let text = std::str::from_utf8(&bytes).with_context(|| format!("{} is not UTF-8", path.display()))?.to_string();
    Ok((text, bytes))
}

/// Registry names carry no namespace prefix (`load` rejects a `:`), so only `_`, whitespace and the first letter are normalised.
fn entry_key(raw: &str) -> String {
    normalize_template_name(raw, |_| false)
}

impl Registry {
    /// Reads `templates.toml` and `sections.toml` from `dir`. Fails on a role, field key, site or section
    /// role it does not know, on unknown TOML keys, and on two entries claiming the same name or alias
    /// for overlapping sites.
    pub fn load(dir: &Path) -> Result<Registry> {
        let tpath = dir.join("templates.toml");
        let spath = dir.join("sections.toml");
        let (ttext, tbytes) = read(&tpath)?;
        let (stext, sbytes) = read(&spath)?;
        let tfile: TemplatesFile = toml::from_str(&ttext).with_context(|| format!("parsing {}", tpath.display()))?;
        let sfile: SectionsFile = toml::from_str(&stext).with_context(|| format!("parsing {}", spath.display()))?;

        let mut hasher = Sha256::new();
        hasher.update(&tbytes);
        hasher.update(&sbytes);
        let hash: String = hasher.finalize().iter().map(|b| format!("{b:02x}")).collect();

        let mut reg = Registry { hash, ..Registry::default() };
        for raw in tfile.template {
            reg.add_template(raw).with_context(|| format!("in {}", tpath.display()))?;
        }
        for raw in sfile.section {
            reg.add_section(raw).with_context(|| format!("in {}", spath.display()))?;
        }
        Ok(reg)
    }

    fn add_template(&mut self, raw: RawTemplate) -> Result<()> {
        for written in std::iter::once(&raw.name).chain(raw.aliases.iter()) {
            if written.contains(':') {
                bail!(
                    "template \"{}\": {:?} contains ':'; write names and aliases without a namespace prefix",
                    raw.name,
                    written
                );
            }
        }
        let name = entry_key(&raw.name);
        if name.is_empty() {
            bail!("a template entry has an empty name");
        }
        if !ROLES.contains(&raw.role.as_str()) {
            bail!("template \"{}\": unknown role {:?} (one of {})", raw.name, raw.role, ROLES.join(", "));
        }
        if let Some(site) = &raw.site {
            if !SITES.contains(&site.as_str()) {
                bail!("template \"{}\": unknown site {:?} (one of {})", raw.name, site, SITES.join(", "));
            }
        }
        for key in raw.fields.keys() {
            if !FIELD_KEYS.contains(&key.as_str()) {
                bail!("template \"{}\": unknown field key {:?} (one of {})", raw.name, key, FIELD_KEYS.join(", "));
            }
        }
        let scope = raw.site.clone().unwrap_or_default();
        let at = self.entries.len();
        let keys = std::iter::once(name.clone()).chain(raw.aliases.iter().map(|a| entry_key(a)));
        for key in keys {
            let Some(&other) = self.index.get(&(scope.clone(), key.clone())) else {
                // A general entry and a site entry of the same name are fine; a duplicate in one scope is not.
                self.index.insert((scope.clone(), key), at);
                continue;
            };
            if other == at {
                continue;
            }
            bail!(
                "templates \"{}\" and \"{}\" both claim the name {:?}{}",
                self.entries[other].name,
                name,
                key,
                if scope.is_empty() { String::new() } else { format!(" on site {scope}") }
            );
        }
        for a in &raw.aliases {
            self.aliases.push((name.clone(), a.clone()));
        }
        self.entries.push(TemplateEntry {
            name,
            role: raw.role,
            family: raw.family,
            archive_wrapper: raw.archive_wrapper,
            fields: raw.fields,
            site: raw.site,
        });
        Ok(())
    }

    fn add_section(&mut self, raw: RawSection) -> Result<()> {
        if !SECTION_ROLES.contains(&raw.role.as_str()) {
            bail!("section role {:?} is unknown (one of {})", raw.role, SECTION_ROLES.join(", "));
        }
        for title in &raw.titles {
            let key = fingerprint::norm(title);
            if let Some(prev) = self.sections.insert(key.clone(), raw.role.clone()) {
                bail!("section title {key:?} is listed twice (roles {prev:?} and {:?})", raw.role);
            }
        }
        Ok(())
    }

    /// The entry for a normalised template name on a site, if any. An entry limited to the site wins over
    /// one that applies to both.
    pub fn template(&self, name: &str, site: &str) -> Option<&TemplateEntry> {
        let find = |scope: &str| self.index.get(&(scope.to_string(), name.to_string()));
        find(site).or_else(|| find("")).map(|&i| &self.entries[i])
    }

    /// The role of a normalised section title; `"content"` when nothing matches.
    pub fn section_role(&self, title_norm: &str) -> &str {
        self.sections.get(title_norm).map(String::as_str).unwrap_or("content")
    }

    /// (entry name, alias as written in the file) for every alias.
    pub fn all_aliases(&self) -> Vec<(String, String)> {
        self.aliases.clone()
    }

    /// Each template name found inside refs in `outputs`, its count, and whether it has an entry on `site`;
    /// sorted by count, descending, then by name. A template is inside a ref when its span lies inside the
    /// span of a ref other than itself: a footnote template (a ref of kind `template`) counts only when it
    /// sits inside another ref, and what its body holds counts. Templates in a ref list's own markup do not.
    ///
    /// A method rather than the free function `in_ref_coverage(outputs, site)` of the plan: whether a name
    /// "has a registry entry" needs the registry, and the rows' `role` cannot tell an unlisted template from
    /// an entry with role `other`.
    pub fn in_ref_coverage(&self, outputs: &[PageOutput], site: &str) -> Vec<(String, usize, bool)> {
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for o in outputs {
            let mut spans: Vec<(i32, i32)> = o.components.refs.iter().map(|r| (r.start, r.end)).collect();
            spans.sort_unstable();
            for t in &o.components.templates {
                let before = &spans[..spans.partition_point(|&(s, _)| s <= t.start)];
                if before.iter().rev().any(|&(s, e)| t.end <= e && (s, e) != (t.start, t.end)) {
                    *counts.entry(t.name.as_str()).or_default() += 1;
                }
            }
        }
        let mut v: Vec<(String, usize, bool)> =
            counts.into_iter().map(|(name, n)| (name.to_string(), n, self.template(name, site).is_some())).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v
    }

    /// sha256 of `templates.toml` followed by `sections.toml`, as lowercase hex. Empty for `default()`.
    pub fn hash(&self) -> String {
        self.hash.clone()
    }
}
