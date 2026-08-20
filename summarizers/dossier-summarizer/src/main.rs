use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use crawl::paths::{cache_dir, data_dir};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::{arrow::ArrowWriter, arrow::arrow_reader::ParquetRecordBatchReaderBuilder};
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex as TokioMutex;

struct RateLimiter {
    interval_ms: u64,
    min_next_call: TokioMutex<Instant>,
}

impl RateLimiter {
    fn new(requests_per_second: f64) -> Self {
        let interval_ms = (1_000.0 / requests_per_second * 1.15) as u64;
        Self {
            interval_ms,
            min_next_call: TokioMutex::new(Instant::now()),
        }
    }
    async fn acquire(&self) {
        let mut guard = self.min_next_call.lock().await;
        let now = Instant::now();
        if now < *guard {
            tokio::time::sleep(*guard - now).await;
        }
        *guard = Instant::now() + Duration::from_millis(self.interval_ms);
    }
    async fn penalize(&self, delay_ms: u64) {
        let mut guard = self.min_next_call.lock().await;
        *guard = Instant::now() + Duration::from_millis(delay_ms);
    }
}

impl CachedAdoptedTextSummaries {
    fn is_complete(&self) -> bool {
        !self.social_summary.trim().is_empty()
    }
}

// CONSTANTS
const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF_MS: u64 = 2_000;
const MAX_BACKOFF_MS: u64 = 60_000;
const SAVE_EVERY: usize = 5;
const MODEL_ADOPTED_TEXT: &str = "mistral-large-latest";
const MODEL_REPORT: &str = "mistral-large-latest";
const SEO_TITLE_MIN: usize = 40;
const SEO_TITLE_MAX: usize = 60;
const SEO_DESCRIPTION_MIN: usize = 120;
const SEO_DESCRIPTION_MAX: usize = 158;

struct CachedAdoptedTextSummaries {
    summary_hash: String,
    summary: String,
    social_summary: String,
    title: String,
    description: String,
    model: String,
    dossier_id: String,
    source: String,
    created_at: String,
}

struct CachedReportSummaries {
    summary_hash: String,
    arguments: String,
    model: String,
    dossier_id: String,
    source: String,
    created_at: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    role: String,
    content: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Choice {
    message: Message,
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiResponse {
    choices: Vec<Choice>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct AdoptedTextSummaryJson {
    title: String,
    description: String,
    summary: String,
    social_summary: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct ReportSummaryJson {
    title: String,
    description: String,
    arguments_pro: Vec<PartyArgument>,
    arguments_contra: Vec<PartyArgument>,
    arguments_neutral: Vec<PartyArgument>,
    debate_summary: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct PartyArgument {
    parties: String,
    argument: String,
}

// PROMPTS
fn system_prompt_content() -> &'static str {
    "Je bent een objectieve samenvatter van parlementaire documenten en een ervaren SEO-copywriter. \
Je antwoordt ALTIJD uitsluitend met geldig JSON. \
Geen extra tekst, geen uitleg, geen markdown code-blokken — enkel de JSON."
}

fn user_prompt_content(content: &str, is_adopted: bool, has_original_context: bool) -> String {
    let intro = if is_adopted && has_original_context {
        "Je krijgt hieronder twee delen van hetzelfde dossier. Eerst de ORIGINELE TEKST van het \
       parlementair voorstel — deze bevat vaak een memorie van toelichting of inleidende samenvatting \
       die het onderwerp, de motivering en de context helder uitlegt. Daarna volgt de AANGENOMEN TEKST, \
       de definitieve juridische tekst zoals aangenomen door de Kamer. Deze aangenomen tekst bestaat vaak \
       uit een artikelsgewijze wijziging van bestaande wetgeving en is op zichzelf moeilijk te interpreteren. \
       Gebruik de originele tekst om het onderwerp, de motivering en de context te begrijpen, maar baseer \
       je beschrijving van de UITEINDELIJKE inhoud en de concrete gevolgen op de aangenomen tekst, aangezien \
       die de geldende wetgeving weergeeft."
    } else if is_adopted {
        "Je krijgt de volledige tekst van een aangenomen parlementaire tekst."
    } else {
        "Je krijgt de volledige tekst van een parlementair voorstel dat NIET werd aangenomen door de Kamer \
       (verworpen of zonder voorwerp verklaard). Behandel de tekst uitdrukkelijk als een voorstel, niet als geldende wetgeving, \
       en maak dit waar relevant duidelijk in de samenvatting."
    };
    format!(
        "{intro} \
    Geef je antwoord als JSON met EXACT deze structuur (geen extra velden, geen commentaar, geen markdown):\n\n\
    {{\n\
    \"title\": \"SEO-titel voor deze pagina, tussen {title_min} en {title_max} tekens\",\n\
    \"description\": \"SEO meta-omschrijving voor deze pagina, tussen {desc_min} en {desc_max} tekens\",\n\
    \"summary\": \"samenvatting van de tekst voor een gewone kiezer zonder juridische kennis\",\n\
    \"social_summary\": \"samenvatting en context van de tekst voor een instagram gebruiker zonder juridische kennis\"\n\
    }}\n\n\
    Regels voor \"title\":\n\
    - Schrijf in het Nederlands.\n\
    - Tussen {title_min} en {title_max} tekens lang (streef naar die lengte, niet korter, niet langer).\n\
    - Beknopt en concreet, benoem het hoofdonderwerp van het dossier.\n\
    - Geen aanhalingstekens, geen slotpunt, geen 'Samenvatting:' of vergelijkbare voorzetsels.\n\
    - Geen politieke interpretatie, enkel feiten.\n\n\
    Regels voor \"description\":\n\
    - Schrijf in het Nederlands.\n\
    - Tussen {desc_min} en {desc_max} tekens lang (streef naar die lengte, niet korter, niet langer).\n\
    - Eén tot twee zinnen die duidelijk maken waar het dossier over gaat en wat de concrete gevolgen zijn.\n\
    - Geschikt als meta-description voor zoekmachines: pakkend maar feitelijk, geen clickbait.\n\
    - Geen aanhalingstekens, geen politieke interpretatie, enkel feiten.\n\n\
    Regels voor \"summary\":\n\
    - Schrijf in het Nederlands.\n\
    - Gebruik maximaal 4 zinnen, hoe korter hoe beter.\n\
    - Benadruk het hoofdonderwerp en de concrete gevolgen.\n\
    - Houd het objectief — geen politieke interpretatie, alleen feiten.\n\
    - Geen extra uitleg, geen opsommingen, enkel de samenvatting.\n\
    - Geen voorzetsel zoals 'Samenvatting:' of 'Samenvatting van de tekst:'.\n\n\
    Regels voor \"social_summary\":\n\
    - Schrijf in het Nederlands.\n\
    - Maximaal 2 korte alinea's, geschikt voor een Instagram-post.\n\
    - Directe, toegankelijke taal — geen jargon, geen ambtelijke stijl.\n\
    - Leg eerst kort uit wat het voorstel/de tekst inhoudt, dan wat de concrete gevolgen zijn of zouden zijn.\n\
    - Blijf strikt feitelijk en neutraal: geen partij kiezen, geen morele lading, geen suggestie dat een van de partijen 'niet luisterde' of 'geen oplossing biedt'.\n\
    - Geen retorische stijlmiddelen: geen 'Stel je voor...', geen directe aanspreking van de lezer, geen retorische vragen, geen dramatiserende taal ('geen oplossing in zicht', 'blijft zitten met').\n\
    - Als de tekst niet werd aangenomen: benoem dat expliciet en neutraal (bv. 'Dit voorstel werd niet aangenomen door de Kamer'), zonder de premisse van het voorstel als vaststaand feit te presenteren en zonder impliciet spijt of kritiek te uiten over de verwerping.\n\
    - Toon mag toegankelijk en to-the-point zijn, maar niet activistisch of pleitend.\n\
    - Geen hashtags, geen emoji's, geen call-to-action.\n\
    - Geen voorzetsel zoals 'Samenvatting:' of 'Samenvatting van de tekst:'.\n\n\
    Enkel de JSON teruggeven, niets anders.\n\n\
    Documentinhoud:\n{content}",
        intro = intro,
        title_min = SEO_TITLE_MIN,
        title_max = SEO_TITLE_MAX,
        desc_min = SEO_DESCRIPTION_MIN,
        desc_max = SEO_DESCRIPTION_MAX,
        content = content
    )
}

fn system_prompt_arguments() -> &'static str {
    "Je bent een politiek analist van het Belgische parlement. \
Je antwoordt ALTIJD uitsluitend met geldig JSON. \
Geen extra tekst, geen uitleg, geen markdown code-blokken — enkel de JSON."
}

fn user_prompt_arguments(content: &str) -> String {
    format!(
        "Je krijgt de volledige tekst van een parlementair verslag. \
Analyseer het dossier en geef je antwoord als JSON met EXACT deze structuur \
(geen extra velden, geen commentaar, geen markdown):\n\n\
{{\n\
  \"title\": \"beknopte titel van het dossier (max 15 woorden)\",\n\
  \"description\": \"beknopte beschrijving van wat het dossier inhoudt (1-2 zinnen)\",\n\
  \"arguments_pro\": [\n\
    {{ \"parties\": \"naam van partij/partijen\", \"argument\": \"het argument (1-2 zinnen, geen markdown)\" }}\n\
  ],\n\
  \"arguments_contra\": [\n\
    {{ \"parties\": \"naam van partij/partijen\", \"argument\": \"het argument (1-2 zinnen, geen markdown)\" }}\n\
  ],\n\
  \"arguments_neutral\": [\n\
    {{ \"parties\": \"naam van partij/actor\", \"argument\": \"de neutrale opmerking (1-2 zinnen, geen markdown)\" }}\n\
  ],\n\
  \"debate_summary\": \"alinea die de grote lijnen van het debat samenvat\"\n\
}}\n\n\
Regels:\n\
- Schrijf in het Nederlands.\n\
- arguments_pro en arguments_contra: max 5 items elk, kies de meest relevante.\n\
- arguments_neutral mag een lege array [] zijn als er geen neutrale standpunten zijn.\n\
- Geen **vetgedrukt** in de argumenten of parties-velden.\n\
- Enkel de JSON teruggeven, niets anders.\n\n\
Documentinhoud:\n{content}"
    )
}

/// Strip optional ```json … ``` fences that some models add despite instructions.
fn strip_code_fences(raw: &str) -> String {
    let stripped = raw.trim();
    if stripped.starts_with("```") {
        let inner: Vec<&str> = stripped.lines().collect();
        let start = 1;
        let end = inner
            .iter()
            .rposition(|l| l.trim() == "```")
            .unwrap_or(inner.len());
        inner[start..end].join("\n")
    } else {
        stripped.to_string()
    }
}

fn parse_adopted_text_summary_response(raw: &str) -> AdoptedTextSummaryJson {
    let json_str = strip_code_fences(raw);
    serde_json::from_str(&json_str).unwrap_or_else(|e| {
        eprintln!("[summarizer] WARNING: failed to parse content JSON: {e}\nRaw:\n{raw}");
        AdoptedTextSummaryJson::default()
    })
}

fn parse_report_summary_response(raw: &str) -> ReportSummaryJson {
    let json_str = strip_code_fences(raw);
    serde_json::from_str(&json_str).unwrap_or_else(|e| {
        eprintln!("[summarizer] WARNING: failed to parse arguments JSON: {e}\nRaw:\n{raw}");
        ReportSummaryJson::default()
    })
}

/// Log a warning if the generated title/description fall well outside
/// the requested SEO length range, so it's easy to spot in logs without failing the run.
fn check_seo_lengths(dossier_id: &str, title: &str, description: &str) {
    let title_len = title.chars().count();
    let desc_len = description.chars().count();
    if title_len == 0
        || title_len < SEO_TITLE_MIN.saturating_sub(10)
        || title_len > SEO_TITLE_MAX + 15
    {
        eprintln!(
            "[summarizer] WARNING: dossier {dossier_id} title length {title_len} is outside expected SEO range ({SEO_TITLE_MIN}-{SEO_TITLE_MAX})"
        );
    }
    if desc_len == 0
        || desc_len < SEO_DESCRIPTION_MIN.saturating_sub(20)
        || desc_len > SEO_DESCRIPTION_MAX + 25
    {
        eprintln!(
            "[summarizer] WARNING: dossier {dossier_id} description length {desc_len} is outside expected SEO range ({SEO_DESCRIPTION_MIN}-{SEO_DESCRIPTION_MAX})"
        );
    }
}

fn load_existing_adopted_text_summaries(
    path: &Path,
) -> HashMap<String, CachedAdoptedTextSummaries> {
    let mut map = HashMap::new();
    if !path.exists() {
        return map;
    }
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    for batch in reader {
        let batch = batch.unwrap();
        let hash_col = col_str(&batch, "summary_hash");
        let summary_col = col_str(&batch, "summary");
        let social_summary_col = col_str_opt(&batch, "social_summary");
        let title_col = col_str(&batch, "title");
        let description_col = col_str(&batch, "description");
        let model_col = col_str(&batch, "model");
        let dossier_id_col = col_str(&batch, "dossier_id");
        let source_col = col_str(&batch, "source");
        let created_at_col = col_str(&batch, "created_at");
        for i in 0..batch.num_rows() {
            map.insert(
                hash_col.value(i).to_string(),
                CachedAdoptedTextSummaries {
                    summary_hash: hash_col.value(i).to_string(),
                    summary: summary_col.value(i).to_string(),
                    social_summary: social_summary_col
                        .map(|c| c.value(i).to_string())
                        .unwrap_or_default(),
                    title: title_col.value(i).to_string(),
                    description: description_col.value(i).to_string(),
                    model: model_col.value(i).to_string(),
                    dossier_id: dossier_id_col.value(i).to_string(),
                    source: source_col.value(i).to_string(),
                    created_at: created_at_col.value(i).to_string(),
                },
            );
        }
    }
    map
}

fn load_existing_report_summaries(path: &Path) -> HashMap<String, CachedReportSummaries> {
    let mut map = HashMap::new();
    if !path.exists() {
        return map;
    }
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    for batch in reader {
        let batch = batch.unwrap();
        let hash_col = col_str(&batch, "summary_hash");
        let args_col = col_str(&batch, "arguments");
        let model_col = col_str(&batch, "model");
        let dossier_id_col = col_str(&batch, "dossier_id");
        let source_col = col_str(&batch, "source");
        let created_at_col = col_str(&batch, "created_at");
        for i in 0..batch.num_rows() {
            map.insert(
                hash_col.value(i).to_string(),
                CachedReportSummaries {
                    summary_hash: hash_col.value(i).to_string(),
                    arguments: args_col.value(i).to_string(),
                    model: model_col.value(i).to_string(),
                    dossier_id: dossier_id_col.value(i).to_string(),
                    source: source_col.value(i).to_string(),
                    created_at: created_at_col.value(i).to_string(),
                },
            );
        }
    }
    map
}

fn save_adopted_text_summary(
    path: &Path,
    cache: &HashMap<String, CachedAdoptedTextSummaries>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (
        mut hashes,
        mut summaries,
        mut social_summaries,
        mut titles,
        mut descriptions,
        mut models,
        mut dossier_ids,
        mut sources,
        mut created_ats,
    ) = (
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![],
    );
    for row in cache.values() {
        hashes.push(row.summary_hash.clone());
        summaries.push(row.summary.clone());
        social_summaries.push(row.social_summary.clone());
        titles.push(row.title.clone());
        descriptions.push(row.description.clone());
        models.push(row.model.clone());
        dossier_ids.push(row.dossier_id.clone());
        sources.push(row.source.clone());
        created_ats.push(row.created_at.clone());
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("summary_hash", DataType::Utf8, false),
        Field::new("summary", DataType::Utf8, false),
        Field::new("social_summary", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(hashes)),
            Arc::new(StringArray::from(summaries)),
            Arc::new(StringArray::from(social_summaries)),
            Arc::new(StringArray::from(titles)),
            Arc::new(StringArray::from(descriptions)),
            Arc::new(StringArray::from(models)),
            Arc::new(StringArray::from(dossier_ids)),
            Arc::new(StringArray::from(sources)),
            Arc::new(StringArray::from(created_ats)),
        ],
    )?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn save_report_summary(
    path: &Path,
    cache: &HashMap<String, CachedReportSummaries>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut hashes, mut arguments, mut models, mut dossier_ids, mut sources, mut created_ats) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for row in cache.values() {
        hashes.push(row.summary_hash.clone());
        arguments.push(row.arguments.clone());
        models.push(row.model.clone());
        dossier_ids.push(row.dossier_id.clone());
        sources.push(row.source.clone());
        created_ats.push(row.created_at.clone());
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("summary_hash", DataType::Utf8, false),
        Field::new("arguments", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(hashes)),
            Arc::new(StringArray::from(arguments)),
            Arc::new(StringArray::from(models)),
            Arc::new(StringArray::from(dossier_ids)),
            Arc::new(StringArray::from(sources)),
            Arc::new(StringArray::from(created_ats)),
        ],
    )?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn col_str<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("Missing column: {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("Column {name} is not a StringArray"))
}

fn col_str_opt<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

/// Call Mistral API
async fn mistral_complete(
    client: &Client,
    api_key: &str,
    system: &str,
    user: &str,
    model: &str,
    call_count: &mut u32,
    rate_limiter: &RateLimiter,
) -> Option<String> {
    let payload = json!({
        "model": model,
        "messages": [
            { "role": "system", "content": system },
            { "role": "user",   "content": user }
        ]
    });
    let mut attempt = 0u32;
    let mut backoff_ms = INITIAL_BACKOFF_MS;
    loop {
        attempt += 1;
        rate_limiter.acquire().await;
        let response = client
            .post("https://api.mistral.ai/v1/chat/completions")
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Bearer {api_key}"))
            .json(&payload)
            .send()
            .await;
        match response {
            Ok(resp) if resp.status().is_success() => {
                let json_resp: ApiResponse = resp.json().await.unwrap();
                *call_count += 1;
                return Some(json_resp.choices[0].message.content.trim().to_string());
            }
            Ok(resp) if resp.status().as_u16() == 429 || resp.status().is_server_error() => {
                let status = resp.status();
                let retry_after_ms = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(|secs| secs * 1_000 + 500)
                    .unwrap_or(backoff_ms);
                let body = resp.text().await.unwrap_or_default();
                if attempt >= MAX_RETRIES {
                    eprintln!("Mistral retry failed after {attempt} attempts ({status}): {body}");
                    return None;
                }
                eprintln!(
                    "Mistral {status} (attempt {attempt}/{MAX_RETRIES}), retrying in {retry_after_ms}ms… | {body}"
                );
                rate_limiter.penalize(retry_after_ms).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            }
            Ok(resp) => {
                eprintln!(
                    "Mistral failed {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
                return None;
            }
            Err(err) => {
                if attempt >= MAX_RETRIES {
                    eprintln!("Network error after {attempt} attempts: {err}");
                    return None;
                }
                eprintln!(
                    "Network error (attempt {attempt}/{MAX_RETRIES}): {err}, retrying in {backoff_ms}ms…"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            }
        }
    }
}

fn hash_text(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Collect all dossier IDs
fn discover_dossier_ids(base: &Path) -> Vec<String> {
    let mut ids = Vec::new();
    if let Ok(entries) = std::fs::read_dir(base) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if let Some(name) = entry.file_name().to_str() {
                    ids.push(name.to_string());
                }
            }
        }
    }
    ids.sort();
    ids
}

#[tokio::main]
async fn main() {
    // Load environment variables
    dotenvy::dotenv().ok();
    let mistral_api_key = std::env::var("MISTRAL_API_TOKEN").expect("Missing MISTRAL_API_TOKEN");

    // Optional: pass a single dossier ID as a CLI argument for testing.
    let single_dossier: Option<String> = Some(String::from("1164")); //std::env::args().nth(1);
    let client = Client::new();
    let dossiers_base = cache_dir().join("sessions/56/dossiers/pdfs");
    let content_out = data_dir().join("summaries/dossier_content.parquet");
    let arguments_out = data_dir().join("summaries/dossier_arguments.parquet");

    // Shared rate limiter for mistral-large-latest (1 req/s with margin).
    let rate_limiter = Arc::new(RateLimiter::new(1.0));
    let dossier_ids: Vec<String> = if let Some(ref id) = single_dossier {
        println!("[summarizer] single-dossier mode: {id}");
        vec![id.clone()]
    } else {
        discover_dossier_ids(&dossiers_base)
    };
    println!("[summarizer] found {} dossiers", dossier_ids.len());

    // Load caches.
    let mut content_cache = load_existing_adopted_text_summaries(&content_out);
    let mut arguments_cache = load_existing_report_summaries(&arguments_out);
    println!(
        "[summarizer] loaded {} cached content summaries, {} cached argument analyses",
        content_cache.len(),
        arguments_cache.len()
    );

    let mut total_calls = 0u32;
    let mut new_content: usize = 0;
    let mut new_arguments: usize = 0;
    let pb = ProgressBar::new(dossier_ids.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[summarizer] [{elapsed_precise}] {spinner:.blue} {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {msg}",
        )
        .unwrap()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    pb.set_message(format!("api_calls={total_calls}"));

    for dossier_id in &dossier_ids {
        let dossier_dir = dossiers_base.join(dossier_id);

        // Summarize adopted text or original text.
        // If a dossier was adopted, the adopted text is often just a line-by-line
        // amendment to existing law, which is hard for the LLM to interpret on its
        // own. The original text — especially when the government is the author —
        // frequently contains a memorie van toelichting / summary that explains the
        // subject matter. So when both exist, we feed both to the model: the
        // original for context, the adopted text as the authoritative outcome.
        let adopted_path = dossier_dir.join("adopted_text.md");
        let original_path = dossier_dir.join("original_text.md");

        let adopted_content = std::fs::read_to_string(&adopted_path)
            .ok()
            .filter(|s| !s.trim().is_empty());
        let original_content = std::fs::read_to_string(&original_path)
            .ok()
            .filter(|s| !s.trim().is_empty());

        let is_adopted = adopted_content.is_some();
        let has_original_context = adopted_content.is_some() && original_content.is_some();

        let combined: Option<(String, String)> = match (&adopted_content, &original_content) {
            (Some(adopted), Some(original)) => Some((
                format!(
                    "=== ORIGINELE TEKST (context / memorie van toelichting) ===\n{original}\n\n\
                     === AANGENOMEN TEKST (definitieve, geldende juridische tekst) ===\n{adopted}"
                ),
                format!(
                    "adopted_text.md ({}) + original_text.md ({})",
                    adopted_path.display(),
                    original_path.display()
                ),
            )),
            (Some(adopted), None) => Some((
                adopted.clone(),
                format!("adopted_text.md ({})", adopted_path.display()),
            )),
            (None, Some(original)) => Some((
                original.clone(),
                format!("original_text.md ({})", original_path.display()),
            )),
            (None, None) => None,
        };

        if let Some((content, source)) = combined {
            let trimmed_content = content.trim();
            if trimmed_content.len() < 500 {
                eprintln!(
                    "[summarizer] WARNING: skipping dossier {dossier_id} content — combined length too short ({} chars < 500)",
                    trimmed_content.len()
                );
            } else {
                let hash = hash_text(&content);
                let needs_regen = match content_cache.get(&hash) {
                    Some(existing) => !existing.is_complete(),
                    None => true,
                };
                if needs_regen {
                    pb.set_message(format!(
                        "api_calls={total_calls} — summarizing {} for dossier {dossier_id}",
                        if has_original_context {
                            "adopted text + original context"
                        } else if is_adopted {
                            "adopted text"
                        } else {
                            "original (rejected) text"
                        }
                    ));
                    let user = user_prompt_content(&content, is_adopted, has_original_context);
                    if let Some(raw_response) = mistral_complete(
                        &client,
                        &mistral_api_key,
                        system_prompt_content(),
                        &user,
                        MODEL_ADOPTED_TEXT,
                        &mut total_calls,
                        &rate_limiter,
                    )
                    .await
                    {
                        let parsed = parse_adopted_text_summary_response(&raw_response);
                        check_seo_lengths(dossier_id, &parsed.title, &parsed.description);
                        content_cache.insert(
                            hash.clone(),
                            CachedAdoptedTextSummaries {
                                summary_hash: hash,
                                summary: parsed.summary,
                                social_summary: parsed.social_summary,
                                title: parsed.title,
                                description: parsed.description,
                                model: MODEL_ADOPTED_TEXT.to_string(),
                                dossier_id: dossier_id.clone(),
                                source,
                                created_at: now_rfc3339(),
                            },
                        );
                        new_content += 1;
                        if new_content % SAVE_EVERY == 0 {
                            if let Err(e) = save_adopted_text_summary(&content_out, &content_cache)
                            {
                                eprintln!(
                                    "[summarizer] WARNING: content checkpoint save failed: {e}"
                                );
                            }
                        }
                    }
                }
            }
        }

        // Summarize report
        let report_path = dossier_dir.join("report.md");
        if report_path.exists() {
            let content = std::fs::read_to_string(&report_path).unwrap_or_default();
            let trimmed_content = content.trim();
            if trimmed_content.len() < 500 {
                eprintln!(
                    "[summarizer] WARNING: skipping dossier {dossier_id} report.md — content too short ({} chars < 500)",
                    trimmed_content.len()
                );
            } else if !trimmed_content.is_empty() {
                let hash = hash_text(&content);
                if !arguments_cache.contains_key(&hash) {
                    pb.set_message(format!(
                        "api_calls={total_calls} — summarizing report for dossier {dossier_id}"
                    ));
                    let user = user_prompt_arguments(&content);
                    if let Some(raw_response) = mistral_complete(
                        &client,
                        &mistral_api_key,
                        system_prompt_arguments(),
                        &user,
                        MODEL_REPORT,
                        &mut total_calls,
                        &rate_limiter,
                    )
                    .await
                    {
                        let parsed = parse_report_summary_response(&raw_response);
                        let arguments_json =
                            serde_json::to_string(&parsed).unwrap_or_else(|_| "{}".to_string());
                        let source = format!("report.md ({})", report_path.display());
                        arguments_cache.insert(
                            hash.clone(),
                            CachedReportSummaries {
                                summary_hash: hash,
                                arguments: arguments_json,
                                model: MODEL_REPORT.to_string(),
                                dossier_id: dossier_id.clone(),
                                source,
                                created_at: now_rfc3339(),
                            },
                        );
                        new_arguments += 1;
                        if new_arguments % SAVE_EVERY == 0 {
                            if let Err(e) = save_report_summary(&arguments_out, &arguments_cache) {
                                eprintln!(
                                    "[summarizer] WARNING: arguments checkpoint save failed: {e}"
                                );
                            }
                        }
                    }
                }
            }
        }

        pb.set_message(format!("api_calls={total_calls}"));
        pb.inc(1);
    }

    pb.finish_with_message(format!("api_calls={total_calls} done"));

    save_adopted_text_summary(&content_out, &content_cache)
        .expect("Failed to write dossier_content.parquet");
    save_report_summary(&arguments_out, &arguments_cache)
        .expect("Failed to write dossier_arguments.parquet");
    println!(
        "[summarizer] finished: {} new content summaries, {} new argument analyses, {} total API calls",
        new_content, new_arguments, total_calls
    );
}
