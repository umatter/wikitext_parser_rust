use parse_wiki_text::{Configuration, Node};
use regex::Regex;

/// Parse wikitext and extract only plain paragraph text
pub fn parse_wikitext(wikitext: &str, skip_lists: bool) -> String {
    // Skip articles with complex nested structures that cause parsing issues
    let table_row_count = wikitext.matches("|-").count();
    let template_count = wikitext.matches("{{").count();
    let file_count = wikitext.matches("[[Файл:").count() + wikitext.matches("[[File:").count();

    // Detect problematic patterns: tables with many templates/images
    if table_row_count > 50 && (template_count > 200 || file_count > 50) {
        eprintln!("WARNING: Skipping article - {} table rows, {} templates, {} images (too complex)",
                  table_row_count, template_count, file_count);
        return String::from("[Article skipped: contains complex nested structures that cause parsing issues]");
    }

    let config = Configuration::default();
    let output = config.parse(wikitext);

    // Extract text and split into paragraphs by ParagraphBreak
    let text = extract_text_from_nodes(&output.nodes, wikitext, skip_lists);

    // Expand common templates for dates and numbers
    let expanded_text = expand_common_templates(&text);

    // Remove image markup fragments
    let cleaned_text = remove_image_fragments(&expanded_text);

    // Split by double newlines and clean up
    let paragraphs: Vec<String> = cleaned_text.split("\n\n")
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();

    // Remove empty sections (headings with no content after them)
    let cleaned_paragraphs = remove_empty_sections(&paragraphs);

    cleaned_paragraphs.join("\n\n")
}

/// Remove image markup fragments that leak through
fn remove_image_fragments(text: &str) -> String {
    let mut result = text.to_string();

    // Remove [[Файл:...]] and [[File:...]] markup completely
    // Use non-greedy match and limit to prevent catastrophic backtracking
    let file_re = Regex::new(r"\[\[(?:Файл|File):[^\]]{0,500}\]\]").unwrap();
    result = file_re.replace_all(&result, "").to_string();

    // Remove image size/position parameters that appear as standalone text
    // Pattern: size|position|description where size is like "130px", "150px", etc.
    // Limit line length to prevent catastrophic backtracking
    let image_params_re = Regex::new(r"(?m)^\d+px\|(?:мини|thumb|миниатюра|left|right|center|слева|справа|центр)\|.{0,200}$").unwrap();
    let lines: Vec<String> = result.lines()
        .filter(|line| !image_params_re.is_match(line.trim()))
        .map(|s| s.to_string())
        .collect();
    result = lines.join("\n");

    // Remove standalone image parameter fragments (size|position|text)
    // Common patterns: "130px|мини|слева|...", "альт=...|мини|..."
    // Use bounded quantifiers to prevent catastrophic backtracking
    let fragment_patterns = vec![
        r"(?m)^\s*\d+px\|мини\|(?:слева|справа|центр)?.{0,200}$",
        r"(?m)^\s*альт=.{0,100}\|мини\|.{0,200}$",
        r"(?m)^\s*\d+px\|мини$",
    ];

    for pattern in fragment_patterns {
        let re = Regex::new(pattern).unwrap();
        result = re.replace_all(&result, "").to_string();
    }

    // Clean up multiple consecutive newlines left by removals
    let multi_newline_re = Regex::new(r"\n{3,}").unwrap();
    result = multi_newline_re.replace_all(&result, "\n\n").to_string();

    result
}

/// Expand common Russian Wikipedia templates for dates and numbers
fn expand_common_templates(text: &str) -> String {
    let mut result = text.to_string();

    // Template {{СС3|18.1.1918}} → "18 января 1918"
    // This handles date templates with day.month.year format
    let date_re = Regex::new(r"\{\{СС3\|(\d+)\.(\d+)\.(\d+)\}\}").unwrap();
    result = date_re.replace_all(&result, |caps: &regex::Captures| {
        let day = &caps[1];
        let month_num: u32 = caps[2].parse().unwrap_or(0);
        let year = &caps[3];

        let month_name = match month_num {
            1 => "января",
            2 => "февраля",
            3 => "марта",
            4 => "апреля",
            5 => "мая",
            6 => "июня",
            7 => "июля",
            8 => "августа",
            9 => "сентября",
            10 => "октября",
            11 => "ноября",
            12 => "декабря",
            _ => "",
        };

        if month_name.is_empty() {
            format!("{}.{}.{}", day, month_num, year)
        } else {
            format!("{} {} {}", day, month_name, year)
        }
    }).to_string();

    // Template {{год|YYYY}} → "YYYY"
    let year_re = Regex::new(r"\{\{год\|(\d{3,4})\}\}").unwrap();
    result = year_re.replace_all(&result, "$1").to_string();

    // Template {{num|###}} → "###"
    let num_re = Regex::new(r"\{\{num\|(\d+)\}\}").unwrap();
    result = num_re.replace_all(&result, "$1").to_string();

    // Clean up any remaining simple templates that just contain text/numbers
    // Match {{TemplateNa me|value}} where value is simple alphanumeric
    let simple_template_re = Regex::new(r"\{\{[^|{}]+\|([^|{}]+)\}\}").unwrap();
    result = simple_template_re.replace_all(&result, "$1").to_string();

    result
}

/// Remove section headings that have no content following them
fn remove_empty_sections(paragraphs: &[String]) -> Vec<String> {
    let mut result = Vec::new();
    let empty_section_names = vec![
        "Население", "Примечания", "Литература", "Ссылки",
        "Категория", "См. также", "Источники"
    ];

    for (i, para) in paragraphs.iter().enumerate() {
        // Check if this is an empty structural heading
        let is_empty_section = empty_section_names.iter().any(|&name| {
            para == name || para.starts_with(&format!("Категория:"))
        });

        if is_empty_section {
            // Check if there's content after this heading
            let has_content_after = i + 1 < paragraphs.len() && {
                let next = &paragraphs[i + 1];
                !empty_section_names.iter().any(|&n| next == n || next.starts_with("Категория:"))
            };

            // Only include if there's actual content after
            if has_content_after {
                result.push(para.clone());
            }
        } else {
            result.push(para.clone());
        }
    }

    result
}

/// Extract plain text from nodes, using the original wikitext for Bold/Italic ranges
fn extract_text_from_nodes(nodes: &[Node], wikitext: &str, skip_lists: bool) -> String {
    let mut text = String::new();
    let mut current_paragraph = String::new();

    for node in nodes {
        match node {
            Node::Text { value, .. } => {
                current_paragraph.push_str(value);
            }
            Node::Bold { start, end, .. }
            | Node::Italic { start, end, .. }
            | Node::BoldItalic { start, end, .. } => {
                // Extract the text content from the marked range
                // The markup itself is within this range, so we need to get the inner text
                let inner_text = &wikitext[*start..*end];
                // Remove the wiki markup (''' for bold, '' for italic, ''''' for both)
                let cleaned = inner_text
                    .trim_start_matches("'''''")
                    .trim_end_matches("'''''")
                    .trim_start_matches("'''")
                    .trim_end_matches("'''")
                    .trim_start_matches("''")
                    .trim_end_matches("''");
                current_paragraph.push_str(cleaned);
            }
            Node::Link { text: link_text, .. } => {
                // Extract only the display text from links
                let link_display = extract_text_from_nodes(link_text, wikitext, skip_lists);
                // Filter out if it looks like an image description (contains "Файл:" patterns)
                if !link_display.contains("Файл:") && !link_display.contains("File:") {
                    current_paragraph.push_str(&link_display);
                }
            }
            Node::ExternalLink { nodes, .. } => {
                // Extract text from external links, but filter out bare URLs
                let link_text = extract_text_from_nodes(nodes, wikitext, skip_lists);
                // Only include if it's not just a URL
                if !link_text.starts_with("http://") && !link_text.starts_with("https://") {
                    current_paragraph.push_str(&link_text);
                }
            }
            Node::Heading { nodes, .. } => {
                // Extract text from headings but treat them as separate paragraphs
                let heading_text = extract_text_from_nodes(nodes, wikitext, skip_lists);
                if !heading_text.trim().is_empty() {
                    if !current_paragraph.is_empty() {
                        text.push_str(&current_paragraph);
                        text.push_str("\n\n");
                        current_paragraph.clear();
                    }
                    text.push_str(heading_text.trim());
                    text.push_str("\n\n");
                }
            }
            Node::ParagraphBreak { .. } => {
                // Mark paragraph boundary
                if !current_paragraph.trim().is_empty() {
                    text.push_str(current_paragraph.trim());
                    text.push_str("\n\n");
                    current_paragraph.clear();
                }
            }
            Node::UnorderedList { items, .. } | Node::OrderedList { items, .. } => {
                if skip_lists {
                    // Skip lists entirely when flag is set
                } else {
                    // Extract text from list items
                    for item in items {
                        let item_text = extract_text_from_nodes(&item.nodes, wikitext, skip_lists);
                        if !item_text.trim().is_empty() {
                            current_paragraph.push_str(item_text.trim());
                            current_paragraph.push(' ');
                        }
                    }
                }
            }
            Node::DefinitionList { items, .. } => {
                if skip_lists {
                    // Skip definition lists entirely when flag is set
                } else {
                    // Extract text from definition list items
                    for item in items {
                        let item_text = extract_text_from_nodes(&item.nodes, wikitext, skip_lists);
                        if !item_text.trim().is_empty() {
                            current_paragraph.push_str(item_text.trim());
                            current_paragraph.push(' ');
                        }
                    }
                }
            }
            Node::Preformatted { nodes, .. } => {
                current_paragraph.push_str(&extract_text_from_nodes(nodes, wikitext, skip_lists));
            }
            Node::Tag { name, nodes, .. } => {
                // Skip ref tags (citations/references)
                if name.as_ref() != "ref" {
                    current_paragraph.push_str(&extract_text_from_nodes(nodes, wikitext, skip_lists));
                }
            }
            // Skip templates, tables, images, categories, and other non-text content
            Node::Template { .. }
            | Node::Table { .. }
            | Node::Image { .. }
            | Node::Category { .. }
            | Node::StartTag { .. }
            | Node::EndTag { .. }
            | Node::Comment { .. }
            | Node::HorizontalDivider { .. }
            | Node::MagicWord { .. }
            | Node::Redirect { .. }
            | Node::Parameter { .. }
            | Node::CharacterEntity { .. } => {}
        }
    }

    // Add any remaining text
    if !current_paragraph.trim().is_empty() {
        text.push_str(current_paragraph.trim());
    }

    text
}
