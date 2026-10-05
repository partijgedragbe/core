use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crawl::client::ScrapingClient;
use crawl::paths::{cache_dir, data_dir};
use parquet::arrow::ArrowWriter;
use scraper::{Html, Selector};
use std::error::Error;
use std::fs::{File, read_to_string};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use tokio::fs;

static SEL_INDEX: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("div.linklist_0 > a, h4").unwrap());
static SEL_P: LazyLock<Selector> = LazyLock::new(|| Selector::parse("p").unwrap());
static SEL_B: LazyLock<Selector> = LazyLock::new(|| Selector::parse("b").unwrap());
static SEL_A: LazyLock<Selector> = LazyLock::new(|| Selector::parse("a").unwrap());

#[derive(Debug)]
struct ScrapedCommission {
    name: String,
    ctype: String,
    chairs: String,
    subchairs: String,
    permanent_members: String,
    replacement_members: String,
}

struct CommissionIndex {
    name: String,
    ctype: String,
    url: String,
    cache_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let client = ScrapingClient::new();

    let detail_dir = cache_dir().join("commissions/details");
    let index_cache = cache_dir().join("commissions/commissions.html");
    let parquet_path = data_dir().join("commissions.parquet");

    fs::create_dir_all(&detail_dir).await?;
    fs::create_dir_all(parquet_path.parent().unwrap()).await?;

    let index_url = "https://www.dekamer.be/kvvcr/showpage.cfm?section=/none&language=nl&cfm=/site/wwwcfm/comm/LstCom.cfm";
    if !index_cache.exists() {
        let html = client.get(index_url).await?.text().await?;
        fs::write(&index_cache, &html).await?;
    }

    let index_html = read_to_string(&index_cache)?;
    let index_doc = Html::parse_document(&index_html);
    let index = extract_index(&index_doc, &detail_dir);

    let mut all_commissions: Vec<ScrapedCommission> = Vec::new();
    let mut web_requests = 0u32;

    for entry in &index {
        if !entry.cache_path.exists() {
            let html = client.get(&entry.url).await?.text().await?;
            web_requests += 1;
            fs::write(&entry.cache_path, &html).await?;
        }

        let html = read_to_string(&entry.cache_path)?;
        let doc = Html::parse_document(&html);
        all_commissions.push(ScrapedCommission {
            name: entry.name.clone(),
            ctype: entry.ctype.clone(),
            chairs: extract_members(&doc, CommissionRole::Chair),
            subchairs: extract_members(&doc, CommissionRole::Subchair),
            permanent_members: extract_members(&doc, CommissionRole::Permanent),
            replacement_members: extract_members(&doc, CommissionRole::Replacement),
        });
    }

    if all_commissions.is_empty() {
        eprintln!("No commissions extracted — aborting Parquet write.");
        return Ok(());
    }

    write_parquet(&parquet_path, &all_commissions)?;
    println!(
        "Scraped {} commissions using {} web request(s).",
        all_commissions.len(),
        web_requests
    );
    Ok(())
}

fn write_parquet(path: &Path, rows: &[ScrapedCommission]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("chairs", DataType::Utf8, false),
        Field::new("subchairs", DataType::Utf8, false),
        Field::new("permanent_members", DataType::Utf8, false),
        Field::new("replacement_members", DataType::Utf8, false),
    ]));

    macro_rules! col {
        ($f:expr) => {
            Arc::new(StringArray::from(rows.iter().map($f).collect::<Vec<_>>())) as ArrayRef
        };
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            col!(|r| r.name.clone()),
            col!(|r| r.ctype.clone()),
            col!(|r| r.chairs.clone()),
            col!(|r| r.subchairs.clone()),
            col!(|r| r.permanent_members.clone()),
            col!(|r| r.replacement_members.clone()),
        ],
    )?;

    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn extract_index(document: &Html, detail_dir: &Path) -> Vec<CommissionIndex> {
    let mut entries = Vec::new();
    let mut current_type = String::from("unknown");

    for element in document.select(&SEL_INDEX) {
        match element.value().name() {
            "h4" => {
                let text = element.text().collect::<String>().trim().to_string();
                if !text.is_empty() {
                    current_type = text.to_lowercase();
                }
            }
            "a" => {
                let Some(href) = element.value().attr("href") else {
                    continue;
                };
                if !href.contains("/comm/com.cfm?com=") {
                    continue;
                }
                let name = element.text().collect::<String>().trim().to_lowercase();
                if name.is_empty() {
                    continue;
                }
                let url = if href.starts_with("http") {
                    href.to_string()
                } else {
                    format!("https://www.dekamer.be/kvvcr/{}", href)
                };
                let safe = name.replace(' ', "_").replace('/', "_");
                entries.push(CommissionIndex {
                    name,
                    ctype: current_type.clone(),
                    url,
                    cache_path: detail_dir.join(format!("{safe}.html")),
                });
            }
            _ => {}
        }
    }

    entries
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommissionRole {
    Chair,
    Subchair,
    Permanent,
    Replacement,
}

/// Matches the bold role label exactly: a substring match would treat
/// `Ondervoorzitters:` as `Voorzitter`.
fn parse_role_label(label: &str) -> Option<CommissionRole> {
    let normalized: String = label
        .to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect();
    // `Voorzitter(s):` splits into `voorzitter` and `s`.
    let key = normalized
        .split_whitespace()
        .filter(|token| *token != "s")
        .collect::<Vec<_>>()
        .join(" ");

    match key.as_str() {
        "voorzitter" | "voorzitters" => Some(CommissionRole::Chair),
        "ondervoorzitter" | "ondervoorzitters" => Some(CommissionRole::Subchair),
        "vaste leden" => Some(CommissionRole::Permanent),
        "plaatsvervangers" => Some(CommissionRole::Replacement),
        _ => None,
    }
}

fn extract_members(doc: &Html, role: CommissionRole) -> String {
    let mut names = Vec::new();

    for p in doc.select(&SEL_P) {
        let Some(first_b) = p.select(&SEL_B).next() else {
            continue;
        };
        if parse_role_label(&first_b.text().collect::<String>()) != Some(role) {
            continue;
        }
        for a in p.select(&SEL_A) {
            let name = a.text().collect::<String>().trim().to_string();
            if !name.is_empty() {
                names.push(name);
            }
        }
    }

    names.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Trimmed from the Justitie detail page: one chair, two subchairs.
    const JUSTITIE_HTML: &str = r#"
        <div id="story">
            <h4>JUSTITIE </h4>
            <p><b>Voorzitter(s):</b>
                <br><b>Les Engagés:</b>
                <A HREF="cvview54.cfm?key=08161">Ismaël Nuino</A>
            </p>
            <p><b>Ondervoorzitters:</b>
                <br><b>cd&v:</b>
                <A HREF="cvview54.cfm?key=07193">Steven Matheï</A>
                <br><b>N-VA:</b>
                <A HREF="cvview54.cfm?key=06230">Kristien Van Vaerenbergh</A>
            </p>
            <p><b>Vaste Leden:</b>
                <br><b>N-VA:</b>
                <A HREF="cvview54.cfm?key=06907">Christoph D'Haese</A>
            </p>
            <p><b>Plaatsvervangers:</b>
                <br><b>VB:</b>
                <A HREF="cvview54.cfm?key=07888">Werner Somers</A>
            </p>
        </div>
    "#;

    #[test]
    fn parses_role_labels_exactly() {
        assert_eq!(
            parse_role_label("Voorzitter(s):"),
            Some(CommissionRole::Chair)
        );
        assert_eq!(
            parse_role_label("Ondervoorzitters:"),
            Some(CommissionRole::Subchair)
        );
        assert_eq!(
            parse_role_label("Vaste Leden:"),
            Some(CommissionRole::Permanent)
        );
        assert_eq!(
            parse_role_label("Plaatsvervangers:"),
            Some(CommissionRole::Replacement)
        );
        assert_eq!(parse_role_label("Les Engagés:"), None);
    }

    #[test]
    fn subchairs_are_not_listed_as_chairs() {
        let doc = Html::parse_document(JUSTITIE_HTML);
        assert_eq!(extract_members(&doc, CommissionRole::Chair), "Ismaël Nuino");
        assert_eq!(
            extract_members(&doc, CommissionRole::Subchair),
            "Steven Matheï, Kristien Van Vaerenbergh"
        );
        assert_eq!(
            extract_members(&doc, CommissionRole::Permanent),
            "Christoph D'Haese"
        );
        assert_eq!(
            extract_members(&doc, CommissionRole::Replacement),
            "Werner Somers"
        );
    }
}
