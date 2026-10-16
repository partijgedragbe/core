use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{Local, NaiveDate};
use crawl::client::ScrapingClient;
use crawl::paths::{cache_dir, data_dir};
use encoding_rs::WINDOWS_1252;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{File, read_to_string};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::fs::read_dir;
use tokio::fs::{self, remove_file};

/// CONSTANTS
const DEKAMER_BASE: &str = "https://www.dekamer.be";
const SESSION_IDS: &[u32] = &[56];

/// REGEXES
static DATE_RE: OnceLock<Regex> = OnceLock::new();
static BR_RE: OnceLock<Regex> = OnceLock::new();
static A_TAG_RE: OnceLock<Regex> = OnceLock::new();

fn date_regex() -> &'static Regex {
    DATE_RE.get_or_init(|| Regex::new(r"(\d{2}/\d{2}/\d{4})").unwrap())
}
fn br_regex() -> &'static Regex {
    BR_RE.get_or_init(|| Regex::new(r"(?i)<br\s*/?>").unwrap())
}
fn a_tag_regex() -> &'static Regex {
    A_TAG_RE.get_or_init(|| Regex::new(r"(?is)<a[^>]*>.*?</a>").unwrap())
}

/// SELECTORS
static SELECTOR_TR: OnceLock<Selector> = OnceLock::new();
static SELECTOR_TD: OnceLock<Selector> = OnceLock::new();
static SELECTOR_TABLE: OnceLock<Selector> = OnceLock::new();
static SELECTOR_TBODY: OnceLock<Selector> = OnceLock::new();
static SELECTOR_A: OnceLock<Selector> = OnceLock::new();
static SELECTOR_FONT: OnceLock<Selector> = OnceLock::new();

fn selector_tr() -> &'static Selector {
    SELECTOR_TR.get_or_init(|| Selector::parse("tr").unwrap())
}
fn selector_td() -> &'static Selector {
    SELECTOR_TD.get_or_init(|| Selector::parse("td").unwrap())
}
fn selector_table() -> &'static Selector {
    SELECTOR_TABLE.get_or_init(|| Selector::parse("table").unwrap())
}
fn selector_tbody() -> &'static Selector {
    SELECTOR_TBODY.get_or_init(|| Selector::parse("tbody").unwrap())
}
fn selector_a() -> &'static Selector {
    SELECTOR_A.get_or_init(|| Selector::parse("a").unwrap())
}
fn selector_font() -> &'static Selector {
    SELECTOR_FONT.get_or_init(|| Selector::parse("font").unwrap())
}

/// The output of this scraper.
struct DossierScrapeOutput {
    dossiers: Vec<ScrapedDossier>,
    subdocuments: Vec<ScrapedSubdocument>,
    events: Vec<ScrapedEvent>,
}

/// A scraped dossier.
struct ScrapedDossier {
    session_id: u32,
    dossier_id: String,
    last_updated: String,
    title: String,
    authors: String,
    procedure_type: String,
    urgency_requested: bool,
    submission_date: String,
    end_date: String,
    vote_date: String,
    document_type: String,
    status: String,
    latest_adopted_text_url: Option<String>,
    latest_report_url: Option<String>,
    eurovoc_main_descriptor: String,
    eurovoc_descriptors: String,
    original_text_url: Option<String>,
}

/// A scraped subdocument.
struct ScrapedSubdocument {
    session_id: u32,
    dossier_id: u32,
    id: String,
    document_date: String,
    circulation_date: String,
    document_type: String,
    reading_phase: Option<String>,
    authors: String,
    file_url: Option<String>,
}

/// A scraped event.
struct ScrapedEvent {
    session_id: u32,
    dossier_id: u32,
    event_date: String,
    event_kind: String,
    label: String,
    chamber: Option<String>,
    committee_name: Option<String>,
    subdocument_id: Option<String>,
}

/// A dossier.
struct Dossier {
    title: String,
    authors: Vec<String>,
    procedure_type: ProcedureType,
    urgency_requested: bool,
    submission_date: String,
    end_date: String,
    vote_date: String,
    document_type: DocumentType,
    status: DocumentStatus,
    subdocuments: Vec<Subdocument>,
    events: Vec<Event>,
    eurovoc_main_descriptor: String,
    eurovoc_descriptors: String,
    original_text_url: Option<String>,
}

/// A subdocument.
struct Subdocument {
    id: String,
    dossier_id: u32,
    document_type: DocumentType,
    document_date: String,
    circulation_date: String,
    authors: Vec<String>,
    reading_phase: Option<ReadingPhase>,
    file_url: Option<String>,
}

/// A dossier event.
struct Event {
    dossier_id: u32,
    event_date: String,             // 31/03/2026
    event_kind: EventKind,          // kalender
    label: String,                  // in bespreking
    chamber: Option<Chamber>,       // commissie kamer
    committee_name: Option<String>, // mobiliteit, overheidsbedrijven en federale instellingen
    subdocument_id: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum EventKind {
    Kalender,
    SubdocumentCreated,
    SubdocumentCirculated,
    Milestone,
}

impl fmt::Display for EventKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy)]
enum Chamber {
    CommissieKamer,
    PlenaireVergaderingKamer,
}

impl fmt::Display for Chamber {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The status of a document which is linked to a dossier.
#[derive(Debug, Clone, Copy)]
enum DocumentStatus {
    Aangenomen,
    HangendKamer,
    Verworpen,
    ZonderVoorwerp,
    Onbekend,
}

impl fmt::Display for DocumentStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Type type of a calendar event.
#[derive(Debug, Clone, Copy)]
enum CalendarEventType {
    InBespreking,
    StartVergadering,
    ZonderVoorwerp,
    Onbekend,
}

impl fmt::Display for CalendarEventType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The type of a document which is linked to a dossier.
#[derive(Debug, Clone, Copy)]
enum DocumentType {
    AangenomenMotie,
    AangenomenTekst,
    AanvullendVerslag,
    Advies,
    AdviesVanDeRaadVanState,
    Amendement,
    ArtikelenAangenomenInPlenum,
    ArtikelenBijEersteStemmingAangenomen,
    ArtikelenInTweedeLezingAangenomen,
    Begroting,
    Bijlage,
    Errata,
    Kaft,
    Naturalisatielijsten,
    NietGeevoceerdOntwerp,
    OpmerkingenVanHetRekenhof,
    OvergezondenOntwerp,
    TabellenOfLijsten,
    Verantwoording,
    Verslag,
    VerslagVerwijzend,
    VoorstelOnderzoekscommissie,
    VoorstelReglement,
    VoorstelTotHerziening,
    VoorstelVanResolutie,
    VoorstelVanMotie,
    VoorstelVanNaturalisatieAkte,
    VoorstelVanVerklaring,
    WetsOntwerp,
    WetsVoorstel,
    Onbekend,
}

impl fmt::Display for DocumentType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// The procedure type with which a dossier is submitted.
#[derive(Debug, Clone, Copy)]
enum ProcedureType {
    Monocameral, // art. 74
    Onbekend,
}

impl fmt::Display for ProcedureType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Which phase the subdocument belongs to.
#[derive(Debug, Clone, Copy)]
enum ReadingPhase {
    EersteLezing,
    TweedeLezing,
    NaVerslag,
    Onbekend,
}

impl fmt::Display for ReadingPhase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

macro_rules! col {
    ($rows:expr, $f:expr) => {
        Arc::new(StringArray::from($rows.iter().map($f).collect::<Vec<_>>())) as ArrayRef
    };
}

macro_rules! col_opt {
    ($rows:expr, $f:expr) => {
        Arc::new(StringArray::from(
            $rows.iter().map($f).collect::<Vec<Option<String>>>(),
        )) as ArrayRef
    };
}

macro_rules! col_bool {
    ($rows:expr, $f:expr) => {
        Arc::new(BooleanArray::from(
            $rows.iter().map($f).collect::<Vec<bool>>(),
        )) as ArrayRef
    };
}

/// Write content to a parquet file.
fn write_parquet(
    path: &Path,
    schema: Arc<Schema>,
    columns: Vec<ArrayRef>,
) -> Result<(), Box<dyn Error>> {
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Main entry point for the dossiers scraper.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let client = ScrapingClient::new();
    let multi_progress_bar = MultiProgress::new();
    for &session_id in SESSION_IDS {
        if let Err(err) = scrape_session(&client, session_id, &multi_progress_bar).await {
            eprintln!("[dossiers] session {} failed entirely: {}", session_id, err);
        }
    }

    Ok(())
}

/// Scrape the dossiers for a single session.
async fn scrape_session(
    client: &ScrapingClient,
    session_id: u32,
    multi_progress_bar: &MultiProgress,
) -> Result<(), Box<dyn Error>> {
    // Create the dossiers directory.
    let dossiers_dir = data_dir()
        .join("sessions")
        .join(session_id.to_string())
        .join("dossiers");
    fs::create_dir_all(&dossiers_dir).await?;

    let mut web_request_count = 0u32;
    println!("[dossiers] started scraping session {}", session_id);

    // Download missing dossiers.
    download_dossiers(
        session_id,
        &client,
        &mut web_request_count,
        &multi_progress_bar,
    )
    .await?;

    // Scrape the dossier details.
    let DossierScrapeOutput {
        dossiers,
        subdocuments,
        events,
    } = scrape_dossiers(session_id, &multi_progress_bar).await?;

    // Write the dossier details to disk.
    write_dossiers(&dossiers_dir.join("dossiers.parquet"), &dossiers)?;
    write_subdocuments(&dossiers_dir.join("subdocuments.parquet"), &subdocuments)?;
    write_events(&dossiers_dir.join("events.parquet"), &events)?;

    println!(
        "[dossiers] session {}: scraped {} dossiers, {} subdocuments and {} events using {} web requests",
        session_id,
        dossiers.len(),
        subdocuments.len(),
        events.len(),
        web_request_count
    );

    Ok(())
}

/// Download dossiers.
async fn download_dossiers(
    session_id: u32,
    client: &ScrapingClient,
    web_request_count: &mut u32,
    multi_progress_bar: &MultiProgress,
) -> Result<(), Box<dyn Error>> {
    // Get the relevant dossier IDs from the plenary meetings.
    let plenary_dir = data_dir()
        .join("sessions")
        .join(session_id.to_string())
        .join("plenary");
    let ids_path = plenary_dir.join("dossier_ids.txt");

    let content = match fs::read_to_string(&ids_path).await {
        Ok(content) => content,
        Err(err) => {
            return Err(format!(
                "missing dossier_ids.txt for session {} at {}: {}",
                session_id,
                ids_path.display(),
                err
            )
            .into());
        }
    };

    let id_dates: HashMap<String, String> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| {
            let mut parts = l.splitn(2, '\t');
            let id = parts.next()?.trim().to_string();
            let date = parts.next().unwrap_or("").trim().to_string();
            Some((id, date))
        })
        .collect();

    let pb = multi_progress_bar.add(ProgressBar::new(id_dates.len() as u64));
    pb.set_style(
        ProgressStyle::with_template(
            "[dossiers-download] [{elapsed_precise}] {spinner:.blue} {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {msg}",
        )?
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );

    for (id, latest_meeting_date) in &id_dates {
        pb.set_message(format!("reqs={} id={}", web_request_count, id));
        check_and_download_dossier_file(
            id,
            latest_meeting_date,
            session_id,
            client,
            web_request_count,
        )
        .await?;
        pb.inc(1);
    }

    pb.finish_with_message("done");

    Ok(())
}

/// Checks if the dossier file is already downloaded and, if not, downloads it.
/// If fetched already today, skip.
/// If the cached file is newer than the meeting date and the meeting is > 7 days ago, skip.
async fn check_and_download_dossier_file(
    dossier_id: &str,
    latest_meeting_date: &str,
    session_id: u32,
    client: &ScrapingClient,
    web_request_count: &mut u32,
) -> Result<(), Box<dyn Error>> {
    let dossier_dir = cache_dir().join(format!("sessions/{}/dossiers", session_id));
    fs::create_dir_all(&dossier_dir).await?;

    let filename_prefix = format!("{}_{}_", session_id, dossier_id);
    let mut existing_file: Option<PathBuf> = None;

    if let Ok(mut entries) = read_dir(&dossier_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let file_name = entry.file_name().to_string_lossy().to_string();
            if !file_name.starts_with(&filename_prefix) {
                continue;
            }
            existing_file = Some(entry.path());
            if let Some(caps) = Regex::new(r"_(\d{4}-\d{2}-\d{2})")?.captures(&file_name) {
                let fetched_str = &caps[1];
                let today = Local::now().naive_local().date();

                // Already fetched today — never re-fetch within the same run
                if fetched_str == today.format("%Y-%m-%d").to_string() {
                    return Ok(());
                }

                let fetched = NaiveDate::parse_from_str(fetched_str, "%Y-%m-%d").ok();
                let meeting = NaiveDate::parse_from_str(latest_meeting_date, "%Y-%m-%d").ok();

                let within_a_week = match (fetched, meeting) {
                    (Some(f), Some(m)) => (f - m).num_days().abs() < 7,
                    _ => false,
                };

                // Skip if cached file post-dates the meeting AND the meeting
                // was more than a week ago (dossier unlikely to still change)
                if fetched_str >= latest_meeting_date && !within_a_week {
                    return Ok(());
                }
            }
            break;
        }
    }

    if let Some(old_path) = &existing_file {
        let _ = remove_file(old_path).await;
    }

    let today = Local::now().naive_local().date();
    let new_path = dossier_dir.join(format!("{}_{}_{}.html", session_id, dossier_id, today));
    let url = format!(
        "https://www.dekamer.be/kvvcr/showpage.cfm?section=/flwb&language=nl&cfm=/site/wwwcfm/flwb/flwbn.cfm?lang=N&legislat={}&dossierID={}",
        session_id, dossier_id
    );

    let response = client.get(&url).await?;
    *web_request_count += 1;
    let raw_bytes = response.bytes().await?;
    let (decoded_str, _, _) = WINDOWS_1252.decode(&raw_bytes);
    fs::write(&new_path, decoded_str.as_ref()).await?;
    Ok(())
}

/// Scrape all HTML dossier files.
async fn scrape_dossiers(
    session_id: u32,
    multi_progress_bar: &MultiProgress,
) -> Result<DossierScrapeOutput, Box<dyn Error>> {
    // Create the dossiers directory.
    let dossiers_source_dir = cache_dir()
        .join("sessions")
        .join(session_id.to_string())
        .join("dossiers");

    // Collect HTML paths up front so we know the total for the progress bar.
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut entries = read_dir(&dossiers_source_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("html") {
            paths.push(path);
        }
    }

    let progress_bar = multi_progress_bar.add(ProgressBar::new(paths.len() as u64));
    progress_bar.set_style(
        ProgressStyle::with_template(
            "[dossiers-scrape] [{elapsed_precise}] {spinner:.blue} {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {msg}",
        )?
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );

    let mut dossiers = Vec::new();
    let mut subdocuments = Vec::new();
    let mut events = Vec::new();

    for path in paths {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        let mut parts = stem.splitn(3, '_');
        let _sid = parts.next().unwrap_or("");
        let dossier_id = parts.next().unwrap_or("").to_string();

        let last_updated = parts.next().unwrap_or("").to_string();

        progress_bar.set_message(format!("id={}", dossier_id));

        let content = match read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to read {}: {}", path.display(), e);
                progress_bar.inc(1);
                continue;
            }
        };

        let document = Html::parse_document(&content);
        let dossier = match scrape_dossier(&dossier_id, &document) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Failed to scrape {}: {}", dossier_id, e);
                progress_bar.inc(1);
                continue;
            }
        };

        // Get latest adopted text URL.
        // Preference order:
        // 1. Most recent "ARTIKELEN AANGENOMEN IN PLENUM"
        // 2. Most recent "AANGENOMEN TEKST"
        let latest_adopted_text_url = dossier
            .subdocuments
            .iter()
            .filter(|s| s.file_url.is_some())
            .filter(|s| {
                matches!(
                    s.document_type,
                    DocumentType::ArtikelenAangenomenInPlenum | DocumentType::AangenomenTekst
                )
            })
            .max_by_key(|s| {
                (
                    matches!(s.document_type, DocumentType::ArtikelenAangenomenInPlenum),
                    &s.id,
                )
            })
            .and_then(|s| s.file_url.clone());

        // Get latest report URL.
        // Preference order:
        // 1. Most recent "VERSLAG"
        let latest_report_url = dossier
            .subdocuments
            .iter()
            .rev()
            .find(|s| matches!(s.document_type, DocumentType::Verslag) && s.file_url.is_some())
            .and_then(|s| s.file_url.clone());

        for subdocument in dossier.subdocuments {
            subdocuments.push(ScrapedSubdocument {
                session_id,
                dossier_id: subdocument.dossier_id,
                id: subdocument.id,
                document_date: subdocument.document_date,
                circulation_date: subdocument.circulation_date,
                document_type: subdocument.document_type.to_string(),
                reading_phase: subdocument.reading_phase.map(|p| p.to_string()),
                authors: subdocument.authors.join(","),
                file_url: subdocument.file_url,
            });
        }

        for event in dossier.events {
            events.push(ScrapedEvent {
                session_id,
                dossier_id: event.dossier_id,
                event_date: event.event_date,
                event_kind: event.event_kind.to_string(),
                label: event.label,
                chamber: event.chamber.map(|c| c.to_string()),
                committee_name: event.committee_name,
                subdocument_id: event.subdocument_id,
            });
        }

        dossiers.push(ScrapedDossier {
            session_id,
            dossier_id,
            last_updated,
            title: dossier.title,
            authors: dossier.authors.join(","),
            procedure_type: dossier.procedure_type.to_string(),
            urgency_requested: dossier.urgency_requested,
            submission_date: dossier.submission_date,
            end_date: dossier.end_date,
            vote_date: dossier.vote_date,
            document_type: dossier.document_type.to_string(),
            status: dossier.status.to_string(),
            latest_adopted_text_url,
            latest_report_url,
            eurovoc_main_descriptor: dossier.eurovoc_main_descriptor,
            eurovoc_descriptors: dossier.eurovoc_descriptors,
            original_text_url: dossier.original_text_url,
        });
        progress_bar.inc(1);
    }

    progress_bar.finish_with_message("done");
    Ok(DossierScrapeOutput {
        dossiers,
        subdocuments,
        events,
    })
}

/// Scrape a single dossier given a HTML document.
fn scrape_dossier(dossier_id: &str, document: &Html) -> Result<Dossier, Box<dyn Error>> {
    // Extract the title.
    let title_selector = Selector::parse("#story h4 center").unwrap();
    let title = document
        .select(&title_selector)
        .next()
        .and_then(|el| el.text().next())
        .unwrap_or("")
        .trim()
        .to_string();

    let mut submission_date = String::new();
    let mut vote_date = String::new();
    let mut end_date = String::new();
    let mut dossier_authors = Vec::new();
    let mut procedure_type = ProcedureType::Onbekend;
    let mut urgency_requested = false;
    let mut document_type = DocumentType::Onbekend;
    let mut status = DocumentStatus::Onbekend;
    let mut subdocuments = Vec::new();
    let mut events: Vec<Event> = Vec::new();
    let mut eurovoc_main_descriptor = String::new();
    let mut eurovoc_descriptors = String::new();
    let mut original_text_url: Option<String> = None;
    let mut original_circulation_date = String::new();

    let mut current_chamber: Option<Chamber> = None;
    let mut current_committee_name: Option<String> = None;

    let document_table = document
        .select(selector_table())
        .next()
        .ok_or_else(|| format!("no table found in dossier {}", dossier_id))?;

    if let Some(tbody) = document_table.select(selector_tbody()).next() {
        for row in document_table.select(selector_tr()) {
            if row.parent().unwrap() != *tbody {
                continue;
            }

            // Select the columns in this row
            let mut columns = row.select(selector_td());
            let (Some(col_1), Some(col_2)) = (columns.next(), columns.next()) else {
                continue;
            };
            let label = normalize_label(col_1.text().collect::<String>());
            let value = col_2.text().collect::<String>().trim().to_string();
            let value_lower = value.to_lowercase();

            if label.contains("indieningsdatum") {
                submission_date = normalize_date(&value_lower);
            } else if label.contains("datum ronddeling") {
                original_circulation_date = normalize_date(&value_lower);
            } else if label.contains("stemming kamer") {
                vote_date = normalize_date(&value_lower);
            } else if label.contains("einddatum") {
                end_date = normalize_date(&value_lower);
            } else if label.contains("artikel grondwet") {
                procedure_type = parse_procedure_type(&value_lower);
            } else if label.contains("auteur(s)") {
                for link in col_2.select(selector_a()) {
                    if let Some(name) = link.text().next() {
                        dossier_authors.push(normalize_author(name));
                    }
                }
                if dossier_authors.is_empty() {
                    for text_node in col_2.text() {
                        let name = text_node.trim();
                        if !name.is_empty() {
                            dossier_authors.push(normalize_author(name));
                        }
                    }
                }
            } else if label.contains("document type") {
                document_type = parse_document_type(&value_lower);
            } else if label.contains("status") {
                status = parse_document_status(&value_lower);
            } else if label.contains("subdocumenten") {
                subdocuments = parse_subdocuments(dossier_id, &col_2);
            } else if let Some(chamber) = parse_chamber(&label) {
                current_chamber = Some(chamber);
                current_committee_name = match chamber {
                    Chamber::CommissieKamer => {
                        let name = strip_trailing_openbaar(&value);
                        (!name.is_empty()).then_some(name)
                    }
                    // "PLENUMVERGADERING" isn't a real committee — leave this None
                    // and let `chamber` alone carry that info.
                    Chamber::PlenaireVergaderingKamer => None,
                };
            } else if label.contains("kalender") {
                // Each line in this cell (separated by <br>) is its own event,
                // e.g. "31/03/2026  IN BESPREKING". Tag it with whichever chamber
                // section (and committee, if any) header preceded this row.
                for (raw_date, line_label) in parse_event_lines(&col_2) {
                    events.push(Event {
                        dossier_id: dossier_id.to_string().parse().unwrap(),
                        event_date: normalize_date(&raw_date),
                        event_kind: EventKind::Kalender,
                        label: parse_calendar_event_type(&line_label).to_string(),
                        chamber: current_chamber,
                        committee_name: current_committee_name
                            .as_ref()
                            .map(|name| parse_commission_type(name).to_string()),
                        subdocument_id: None,
                    });
                }
            } else if label.contains("incident") {
                // e.g. urgency votes: "19/03/2026  STEMMING OVER URGENTIE ===> AANGENOMEN/ADOPTÉE"
                // No events are emitted for incidents — this cell is only consulted to
                // detect whether urgency was requested.
                urgency_requested = parse_event_lines(&col_2).iter().any(|(_, line_label)| {
                    line_label.to_lowercase().contains("stemming over urgentie")
                });
            } else if label.trim().is_empty() {
                // The 'HANGEND KAMER' row has a blank first cell.
                let candidate = parse_document_status(&value_lower);
                if !matches!(candidate, DocumentStatus::Onbekend) {
                    status = candidate;
                }
            } else if label.contains("document kamer") {
                if original_text_url.is_none() {
                    original_text_url = col_2
                        .select(selector_a())
                        .filter_map(|a| a.value().attr("href"))
                        .find(|href| href.ends_with(".pdf"))
                        .map(|href| {
                            if href.starts_with("http") {
                                href.to_string()
                            } else {
                                format!("{}{}", DEKAMER_BASE, href)
                            }
                        });
                }
            } else if label.contains("eurovoc-hoofddescriptor") {
                eurovoc_main_descriptor = value.trim().to_uppercase();
            } else if label.contains("eurovoc descriptoren")
                || label.contains("eurovoc kandidaat-descriptoren")
            {
                // Source uses " | " as separator; store as comma-separated for JS `.split(",")`
                eurovoc_descriptors = value
                    .split('|')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
                    .join(",");
            }
        }
    }

    // Add milestone submission event
    if !submission_date.is_empty() {
        events.push(Event {
            dossier_id: dossier_id.parse().unwrap(),
            event_date: submission_date.clone(),
            event_kind: EventKind::Milestone,
            label: "Ingediend".to_string(),
            chamber: None,
            committee_name: None,
            subdocument_id: None,
        });
    }

    // Add milestone vote event
    if !vote_date.is_empty() {
        events.push(Event {
            dossier_id: dossier_id.parse().unwrap(),
            event_date: vote_date.clone(),
            event_kind: EventKind::Milestone,
            label: status.to_string(),
            chamber: None,
            committee_name: None,
            subdocument_id: None,
        });
    }

    // The original submitted text is scraped separately from the subdocuments. However, it is actually subdocument 1 (the main document).
    // Make sure it shows up in `subdocuments`.
    if let Some(existing) = subdocuments.iter_mut().find(|s| s.id == "1") {
        if existing.file_url.is_none() {
            existing.file_url = original_text_url.clone();
        }
        if existing.circulation_date.is_empty() {
            existing.circulation_date = original_circulation_date.clone();
        }
    } else if original_text_url.is_some() || !submission_date.is_empty() {
        subdocuments.insert(
            0,
            Subdocument {
                dossier_id: dossier_id.to_string().parse().unwrap(),
                id: "1".to_string(),
                document_type,
                document_date: submission_date.clone(),
                circulation_date: original_circulation_date.clone(),
                authors: dossier_authors.clone(),
                reading_phase: None,
                file_url: original_text_url.clone(),
            },
        );
    }

    // Every subdocument produces up to two events: its creation and,
    // separately, its circulation.
    for sub in &subdocuments {
        if !sub.document_date.is_empty() {
            events.push(Event {
                dossier_id: sub.dossier_id,
                event_date: sub.document_date.clone(),
                event_kind: EventKind::SubdocumentCreated,
                label: sub.document_type.to_string(),
                chamber: None,
                committee_name: None,
                subdocument_id: Some(sub.id.clone()),
            });
        }
        if !sub.circulation_date.is_empty() {
            events.push(Event {
                dossier_id: sub.dossier_id,
                event_date: sub.circulation_date.clone(),
                event_kind: EventKind::SubdocumentCirculated,
                label: sub.document_type.to_string(),
                chamber: None,
                committee_name: None,
                subdocument_id: Some(sub.id.clone()),
            });
        }
    }

    Ok(Dossier {
        title,
        authors: dossier_authors,
        procedure_type,
        urgency_requested,
        submission_date,
        end_date,
        vote_date,
        document_type,
        status,
        subdocuments,
        events,
        eurovoc_main_descriptor,
        eurovoc_descriptors,
        original_text_url,
    })
}

fn normalize_label(text: String) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// Split a `<td>` cell containing one or more "DD/MM/YYYY  LABEL" lines
/// (separated by `<br>` tags) into (raw_date, label) pairs. Annotation links
/// like "(Zie opmerking in volledige fiche)" are stripped from the label.
fn parse_event_lines(cell: &ElementRef) -> Vec<(String, String)> {
    let mut lines = Vec::new();

    for segment in br_regex().split(&cell.inner_html()) {
        let cleaned = a_tag_regex().replace_all(segment, "");
        let fragment = Html::parse_fragment(&cleaned);
        let raw_text: String = fragment.root_element().text().collect();
        let normalized = raw_text
            .replace('\u{a0}', " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if normalized.is_empty() {
            continue;
        }
        if let Some(caps) = date_regex().captures(&normalized) {
            let date_str = caps[1].to_string();
            let rest = normalized[caps.get(0).unwrap().end()..].trim().to_string();
            lines.push((date_str, rest));
        }
    }

    lines
}

fn parse_subdocuments(dossier_id: &str, cell: &ElementRef) -> Vec<Subdocument> {
    let subdocument_table = match cell.select(selector_table()).next() {
        Some(t) => t,
        None => return Vec::new(),
    };

    let mut subdocuments = Vec::new();

    // Per-subdocument accumulators
    let mut document_id = String::new();
    let mut document_type = DocumentType::Onbekend;
    let mut document_date = String::new();
    let mut circulation_date = String::new();
    let mut reading_phase: Option<ReadingPhase> = None;
    let mut document_authors: Vec<String> = Vec::new();
    let mut file_url: Option<String> = None;
    let mut parsing_authors = false;
    let mut complete_subdocument = false;

    for row in subdocument_table.select(selector_tr()) {
        let mut cells = row.select(selector_td());
        let cell_1 = cells.next();
        let cell_2 = cells.next();

        // A row with only one cell (or none) acts as a separator between individual subdocuments.
        if cell_2.is_none() {
            if complete_subdocument {
                subdocuments.push(Subdocument {
                    id: document_id.clone(),
                    dossier_id: dossier_id.to_string().parse().unwrap(),
                    document_type,
                    document_date: document_date.clone(),
                    circulation_date: circulation_date.clone(),
                    authors: document_authors.clone(),
                    reading_phase,
                    file_url: file_url.clone(),
                });
                document_id.clear();
                document_date.clear();
                circulation_date.clear();
                reading_phase = None;
                document_authors.clear();
                file_url = None;
                complete_subdocument = false;
                parsing_authors = false;
            }
            continue;
        }

        if let (Some(cell_1), Some(cell_2)) = (cell_1, cell_2) {
            let label = cell_1
                .text()
                .collect::<String>()
                .to_lowercase()
                .trim()
                .to_string();
            let value = cell_2
                .text()
                .collect::<String>()
                .to_lowercase()
                .trim()
                .to_string();

            let cell_1_text = cell_1.text().collect::<String>();
            // The link cell (cell_1) usually contains an <a> whose text is the
            // sub-document number, e.g. "003", stripped down to 3.
            // Some subdocuments ("niet beschikbaar") have no <a> at all so the number is plain text instead.
            // Only attempt this on the header row of a subdocument (i.e. before
            // document_id has been set) — later rows like "Datum ronddeling" or
            // "Auteur(s)" also lack an <a> and must not clobber the id.
            if document_id.is_empty() {
                if let Some(link) = cell_1.select(selector_a()).last() {
                    if let Some(id_text) = link.text().next() {
                        document_id = id_text.trim().trim_start_matches('0').to_string();
                    }
                } else if let Some(id_text) = cell_1_text.split_whitespace().next() {
                    document_id = id_text.trim().trim_start_matches('0').to_string();
                }
            }

            // cell_2 may carry an inline <font> tag with the document type. The
            // date printed directly next to it (e.g. "11/05/2026") is this
            // subdocument's own document date.
            if let Some(font) = cell_2.select(selector_font()).next() {
                let raw_type = font.text().collect::<String>();
                document_type = parse_document_type(raw_type.trim());

                let cell_2_text = cell_2.text().collect::<String>();
                if let Some(caps) = date_regex().captures(&cell_2_text) {
                    document_date = normalize_date(&caps[1]);
                }

                // Capture the linked document URL.
                let pdf_url = cell_1
                    .select(selector_a())
                    .filter_map(|a| a.value().attr("href"))
                    .filter(|href| href.ends_with(".pdf"))
                    .last()
                    .map(|href| {
                        if href.starts_with("http") {
                            href.to_string()
                        } else {
                            format!("{}{}", DEKAMER_BASE, href)
                        }
                    });

                if pdf_url.is_some() {
                    file_url = pdf_url;
                }
            }

            // Explicitly flag documents marked as unavailable (no PDF exists).
            if cell_1_text.to_lowercase().contains("niet beschikbaar") {
                file_url = Some("NIET_BESCHIKBAAR".to_string());
            }

            if label.contains("datum ronddeling") {
                circulation_date = normalize_date(&value);
            }

            if label.contains("auteur(s)") {
                parsing_authors = true;
            }

            if parsing_authors {
                if let Some(link) = cell_2.select(selector_a()).next() {
                    if let Some(name) = link.text().next() {
                        document_authors.push(normalize_author(name));
                    }
                }
            }

            // Rows with a blank first cell and a recognized value (e.g.
            // "TWEEDE LEZING/DEUXIÈME LECTURE") indicate the reading phase.
            if label.is_empty() {
                if let Some(phase) = parse_reading_phase(&value) {
                    reading_phase = Some(phase);
                }
            }

            if !document_id.is_empty() && !document_date.is_empty() {
                complete_subdocument = true;
            }
        }
    }

    if complete_subdocument {
        subdocuments.push(Subdocument {
            id: document_id,
            dossier_id: dossier_id.to_string().parse().unwrap(),
            document_type,
            document_date,
            circulation_date,
            authors: document_authors,
            reading_phase,
            file_url,
        });
    }

    subdocuments
}

static OPENBAAR_SUFFIX_RE: OnceLock<Regex> = OnceLock::new();
fn openbaar_suffix_regex() -> &'static Regex {
    OPENBAAR_SUFFIX_RE
        .get_or_init(|| Regex::new(r"(?i)\s*\(\s*(openbaar|besloten)\s*\)\s*$").unwrap())
}

/// Parse a "N. COMMISSIE KAMER" / "N. PLENAIRE VERGADERING KAMER" header
/// cell into a `Chamber`, if the text matches.
fn parse_chamber(raw: &str) -> Option<Chamber> {
    if raw.contains("commissie kamer") {
        Some(Chamber::CommissieKamer)
    } else if raw.contains("plenaire vergadering kamer") {
        Some(Chamber::PlenaireVergaderingKamer)
    } else {
        None
    }
}

/// Strip a trailing "(OPENBAAR)"/"(BESLOTEN)" marker and collapse whitespace,
/// e.g. for the committee name in a kalender header row.
fn strip_trailing_openbaar(text: &str) -> String {
    let cleaned = text.replace('\u{a0}', " ");
    let normalized = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    openbaar_suffix_regex()
        .replace(&normalized, "")
        .trim()
        .to_string()
}

/// Parse the document status from the given raw text.
fn parse_document_status(raw: &str) -> DocumentStatus {
    let raw = raw.trim().to_lowercase();
    if raw.contains("aangenomen") {
        DocumentStatus::Aangenomen
    } else if raw.contains("verworpen") {
        DocumentStatus::Verworpen
    } else if raw.contains("zonder voorwerp") {
        DocumentStatus::ZonderVoorwerp
    } else if raw.contains("hangend") {
        DocumentStatus::HangendKamer
    } else {
        DocumentStatus::Onbekend
    }
}

/// Parse the procedure type from the given raw text.
fn parse_procedure_type(raw: &str) -> ProcedureType {
    let raw = raw.trim().to_lowercase();
    if raw.contains("monocamerale procedure") {
        ProcedureType::Monocameral
    } else {
        ProcedureType::Onbekend
    }
}

/// Parse the reading phase from the given raw text, if recognized.
fn parse_reading_phase(raw: &str) -> Option<ReadingPhase> {
    let raw = raw.trim().to_lowercase();
    if raw.contains("tweede lezing") {
        Some(ReadingPhase::TweedeLezing)
    } else if raw.contains("eerste lezing") {
        Some(ReadingPhase::EersteLezing)
    } else if raw.contains("na verslag") {
        Some(ReadingPhase::NaVerslag)
    } else {
        None
    }
}

/// Parse the calendar event type from the given raw text.
fn parse_calendar_event_type(raw: &str) -> CalendarEventType {
    let raw = raw.trim().to_lowercase();
    if raw.contains("in bespreking") {
        CalendarEventType::InBespreking
    } else if raw.contains("start vergadering") {
        CalendarEventType::StartVergadering
    } else if raw.contains("zonder voorwerp") {
        CalendarEventType::ZonderVoorwerp
    } else {
        CalendarEventType::Onbekend
    }
}

#[derive(Debug, Clone, Copy)]
enum Commission {
    Landsverdediging,
    Justitie,
    BuitenlandseBetrekkingen,
    FinancienEnBegroting,
    SocialeZakenWerkEnPensioenen,
    BinnenlandseZakenVeiligheidMigratieEnBestuurszaken,
    EconomieConsumentenBeschermingEnDigitalisering,
    EconomieConsumentenBeschermingEnDigitaleAgenda,
    MobiliteitOverheidsbedrijvenEnFederaleInstellingen,
    GezondheidEnGelijkeKansen,
    EnergieLeefmilieuEnKlimaat,
    InterparlementaireKlimaatdialoog,
    GrondwetEnInstitutioneleVernieuwing,
    InfrastructuurVerkeerOverheidsbedrijven,
    BedrijfslevenWetenschapsbeleidOnderwijs,
    NucleaireVeiligheid,
    Terrorisme2016,
    Onbekend,
}

impl fmt::Display for Commission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Commission::BinnenlandseZakenVeiligheidMigratieEnBestuurszaken => {
                write!(
                    f,
                    "binnenlandse zaken, veiligheid, migratie en bestuurszaken"
                )
            }
            Commission::Landsverdediging => write!(f, "landsverdediging"),
            Commission::Justitie => write!(f, "justitie"),
            Commission::BuitenlandseBetrekkingen => write!(f, "buitenlandse betrekkingen"),
            Commission::FinancienEnBegroting => write!(f, "financiën en begroting"),
            Commission::SocialeZakenWerkEnPensioenen => {
                write!(f, "sociale zaken, werk en pensioenen")
            }
            Commission::EconomieConsumentenBeschermingEnDigitalisering => {
                write!(f, "economie, consumentenbescherming en digitalisering")
            }
            Commission::EconomieConsumentenBeschermingEnDigitaleAgenda => {
                write!(f, "economie, consumentenbescherming en digitale agenda")
            }
            Commission::MobiliteitOverheidsbedrijvenEnFederaleInstellingen => {
                write!(f, "mobiliteit, overheidsbedrijven en federale instellingen")
            }
            Commission::GezondheidEnGelijkeKansen => write!(f, "gezondheid en gelijke kansen"),
            Commission::EnergieLeefmilieuEnKlimaat => write!(f, "energie, leefmilieu en klimaat"),
            Commission::InterparlementaireKlimaatdialoog => {
                write!(f, "interparlementaire klimaatdialoog")
            }
            Commission::GrondwetEnInstitutioneleVernieuwing => {
                write!(f, "grondwet en institutionele vernieuwing")
            }
            Commission::InfrastructuurVerkeerOverheidsbedrijven => {
                write!(f, "infrastructuur, het verkeer en de overheidsbedrijven")
            }
            Commission::BedrijfslevenWetenschapsbeleidOnderwijs => {
                write!(
                    f,
                    "het bedrijfsleven, het wetenschapsbeleid, het onderwijs, de nationale wetenschappelijke en culturele instellingen, de middenstand en de landbouw"
                )
            }
            Commission::NucleaireVeiligheid => {
                write!(f, "nucleaire veiligheid")
            }
            Commission::Terrorisme2016 => {
                write!(f, "terrorisme 2016")
            }
            Commission::Onbekend => write!(f, "onbekend"),
        }
    }
}

fn parse_commission_type(raw: &str) -> Commission {
    let raw = raw.trim().to_lowercase();
    if raw.contains("binnenlandse") || raw.contains("intérieur") {
        Commission::BinnenlandseZakenVeiligheidMigratieEnBestuurszaken
    } else if raw.contains("justitie") || raw.contains("justice") {
        Commission::Justitie
    } else if raw.contains("gezondheid") || raw.contains("santé") {
        Commission::GezondheidEnGelijkeKansen
    } else if raw.contains("digitalisering") {
        Commission::EconomieConsumentenBeschermingEnDigitalisering
    } else if raw.contains("l'agenda numérique") {
        Commission::EconomieConsumentenBeschermingEnDigitaleAgenda
    } else if raw.contains("buitenlandse") || raw.contains("relations extérieures") {
        Commission::BuitenlandseBetrekkingen
    } else if raw.contains("mobiliteit") || raw.contains("mobilité") {
        Commission::MobiliteitOverheidsbedrijvenEnFederaleInstellingen
    } else if raw.contains("landsverdediging") || raw.contains("défense") {
        Commission::Landsverdediging
    } else if raw.contains("klimaat") || raw.contains("climat") {
        Commission::EnergieLeefmilieuEnKlimaat
    } else if raw.contains("sociale") {
        Commission::SocialeZakenWerkEnPensioenen
    } else if raw.contains("begroting") || raw.contains("budget") {
        Commission::FinancienEnBegroting
    } else if raw.contains("klimaatdialoog") {
        Commission::InterparlementaireKlimaatdialoog
    } else if raw.contains("grondwet") || raw.contains("constitution") {
        Commission::GrondwetEnInstitutioneleVernieuwing
    } else if raw.contains("infrastructuur") || raw.contains("infrastructure") {
        Commission::InfrastructuurVerkeerOverheidsbedrijven
    } else if raw.contains("wetenschapsbeleid") || raw.contains("politique scientifique") {
        Commission::BedrijfslevenWetenschapsbeleidOnderwijs
    } else if raw.contains("nucleaire") {
        Commission::NucleaireVeiligheid
    } else if raw.contains("terroristische aanslagen van 22 maart 2016") {
        Commission::Terrorisme2016
    } else {
        Commission::Onbekend
    }
}

/// Parse the document type from the given raw text.
fn parse_document_type(raw: &str) -> DocumentType {
    let raw = raw.trim().to_lowercase();
    if raw.contains("voorstel van resolutie") {
        DocumentType::VoorstelVanResolutie
    } else if raw.contains("aanvullend verslag") {
        DocumentType::AanvullendVerslag
    } else if raw.contains("amendement") {
        DocumentType::Amendement
    } else if raw.contains("voorstel tot herziening") {
        DocumentType::VoorstelTotHerziening
    } else if raw.contains("wetsvoorstel") {
        DocumentType::WetsVoorstel
    } else if raw.contains("wetsontwerp") {
        DocumentType::WetsOntwerp
    } else if raw.contains("tabellen of lijsten") {
        DocumentType::TabellenOfLijsten
    } else if raw.contains("verantwoording") {
        DocumentType::Verantwoording
    } else if raw.contains("overgezonden ontwerp") {
        DocumentType::OvergezondenOntwerp
    } else if raw.contains("verslag (verwijzend)") {
        DocumentType::VerslagVerwijzend
    } else if raw.contains("bijlage") {
        DocumentType::Bijlage
    } else if raw.contains("opmerkingen van het rekenhof") {
        DocumentType::OpmerkingenVanHetRekenhof
    } else if raw.contains("verslag") {
        DocumentType::Verslag
    } else if raw.contains("advies van de raad van state") {
        DocumentType::AdviesVanDeRaadVanState
    } else if raw.contains("motie aangenomen") {
        DocumentType::AangenomenMotie
    } else if raw.contains("voorstel van motie") {
        DocumentType::VoorstelVanMotie
    } else if raw.contains("voorstel van naturalisatieakte") {
        DocumentType::VoorstelVanNaturalisatieAkte
    } else if raw.contains("voorstel van verklaring") {
        DocumentType::VoorstelVanVerklaring
    } else if raw.contains("aangenomen tekst") {
        DocumentType::AangenomenTekst
    } else if raw.contains("advies") {
        DocumentType::Advies
    } else if raw.contains("voorstel onderzoekscommissie") {
        DocumentType::VoorstelOnderzoekscommissie
    } else if raw.contains("voorstel reglement") {
        DocumentType::VoorstelReglement
    } else if raw.contains("artikelen bij 1e stemming aangenomen") {
        DocumentType::ArtikelenBijEersteStemmingAangenomen
    } else if raw.contains("artikelen aangenomen in plenum") {
        DocumentType::ArtikelenAangenomenInPlenum
    } else if raw.contains("artikelen in 2e lezing aangenomen") {
        DocumentType::ArtikelenInTweedeLezingAangenomen
    } else if raw.contains("aangenomen tekst") {
        DocumentType::AangenomenTekst
    } else if raw.contains("errata") {
        DocumentType::Errata
    } else if raw.contains("begroting") {
        DocumentType::Begroting
    } else if raw.contains("niet-geevoceerd ontwerp") {
        DocumentType::NietGeevoceerdOntwerp
    } else if raw.contains("kaft") {
        DocumentType::Kaft
    } else if raw.contains("naturalisatielijsten") {
        DocumentType::Naturalisatielijsten
    } else {
        DocumentType::Onbekend
    }
}

fn write_dossiers(path: &Path, rows: &[ScrapedDossier]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("last_updated", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("authors", DataType::Utf8, false),
        Field::new("procedure_type", DataType::Utf8, false),
        Field::new("urgency_requested", DataType::Boolean, false),
        Field::new("submission_date", DataType::Utf8, false),
        Field::new("end_date", DataType::Utf8, false),
        Field::new("vote_date", DataType::Utf8, false),
        Field::new("document_type", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("latest_adopted_text_url", DataType::Utf8, true),
        Field::new("latest_report_url", DataType::Utf8, true),
        Field::new("eurovoc_main_descriptor", DataType::Utf8, false),
        Field::new("eurovoc_descriptors", DataType::Utf8, false),
        Field::new("original_text_url", DataType::Utf8, true),
    ]));
    write_parquet(
        path,
        schema,
        vec![
            col!(rows, |d| d.session_id.to_string()),
            col!(rows, |d| d.dossier_id.clone()),
            col!(rows, |d| d.last_updated.clone()),
            col!(rows, |d| d.title.clone()),
            col!(rows, |d| d.authors.clone()),
            col!(rows, |d| d.procedure_type.clone()),
            col_bool!(rows, |d| d.urgency_requested),
            col!(rows, |d| d.submission_date.clone()),
            col!(rows, |d| d.end_date.clone()),
            col!(rows, |d| d.vote_date.clone()),
            col!(rows, |d| d.document_type.clone()),
            col!(rows, |d| d.status.clone()),
            col_opt!(rows, |d| d.latest_adopted_text_url.clone()),
            col_opt!(rows, |d| d.latest_report_url.clone()),
            col!(rows, |d| d.eurovoc_main_descriptor.clone()),
            col!(rows, |d| d.eurovoc_descriptors.clone()),
            col_opt!(rows, |d| d.original_text_url.clone()),
        ],
    )
}

fn write_subdocuments(path: &Path, rows: &[ScrapedSubdocument]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("document_date", DataType::Utf8, false),
        Field::new("circulation_date", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("reading_phase", DataType::Utf8, true),
        Field::new("authors", DataType::Utf8, false),
        Field::new("file_url", DataType::Utf8, true),
    ]));

    write_parquet(
        path,
        schema,
        vec![
            col!(rows, |d| d.session_id.to_string()),
            col!(rows, |s| s.dossier_id.to_string()),
            col!(rows, |s| s.id.clone()),
            col!(rows, |s| s.document_date.clone()),
            col!(rows, |s| s.circulation_date.clone()),
            col!(rows, |s| s.document_type.clone()),
            col_opt!(rows, |s| s.reading_phase.clone()),
            col!(rows, |s| s.authors.clone()),
            col_opt!(rows, |s| s.file_url.clone()),
        ],
    )
}

fn write_events(path: &Path, rows: &[ScrapedEvent]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("event_date", DataType::Utf8, false),
        Field::new("event_kind", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("chamber", DataType::Utf8, true),
        Field::new("committee_name", DataType::Utf8, true),
        Field::new("subdocument_id", DataType::Utf8, true),
    ]));

    write_parquet(
        path,
        schema,
        vec![
            col!(rows, |e| e.session_id.to_string()),
            col!(rows, |e| e.dossier_id.to_string()),
            col!(rows, |e| e.event_date.clone()),
            col!(rows, |e| e.event_kind.clone()),
            col!(rows, |e| e.label.clone()),
            col_opt!(rows, |e| e.chamber.clone()),
            col_opt!(rows, |e| e.committee_name.clone()),
            col_opt!(rows, |e| e.subdocument_id.clone()),
        ],
    )
}

fn normalize_author(name: &str) -> String {
    let lower = name.trim().to_lowercase();
    if lower.contains("gouvernment") || lower.contains("regering") {
        "government".to_string()
    } else {
        name.trim().replace(",", "")
    }
}

fn normalize_date(date: &str) -> String {
    NaiveDate::parse_from_str(date.trim(), "%d/%m/%Y")
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|_| date.to_string())
}
