use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use crawl::client::ScrapingClient;
use crawl::paths::{cache_dir, data_dir};
use crawl::utils::clean_text;
use encoding_rs::WINDOWS_1252;
use http::StatusCode;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use regex::Regex;
use scraper::{Html, Selector};
use serde_json::json;
use std::error::Error;
use std::fmt;
use std::fs::{File, read_to_string};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use tokio::fs;

/// REGEXES
static QUESTION_REGEX: OnceLock<Regex> = OnceLock::new();
static TIME_REGEX: OnceLock<Regex> = OnceLock::new();
static DATE_REGEX: OnceLock<Regex> = OnceLock::new();
static SPEAKER_REGEX: OnceLock<Regex> = OnceLock::new();
static SPEAKER_NAME_REGEX: OnceLock<Regex> = OnceLock::new();
static TITLES_REGEX: OnceLock<Regex> = OnceLock::new();
static CHAIR_TITLES_REGEX: OnceLock<Regex> = OnceLock::new();
static CHAIR_REGEX: OnceLock<Regex> = OnceLock::new();

fn question_regex() -> &'static Regex {
    // FIXME: Respondents like "de vice-eersteminister en minister van Werk, Economie en Landbouw"
    //        are captured as-is; normalising them is left for a future pass.
    // NOTE: Handles question IDs in the format of `(56002763C)`, `(nr. 6003263c)` and `(n° 6003263c)`
    // NOTE: Handles both ” and " quotes (which is a mistake in meeting 157 question 8)
    // NOTE: Handles missing questionee (which is a mistake in meeting 357 question 35)
    QUESTION_REGEX.get_or_init(|| Regex::new(r#"(?m)(?:(?:Vraag van|Question de)\s)?([^\n]+?)(?:\s+(?:aan|à|au)\s+([^\n]+?))?(?:\s*\(.*?\))?\s*(?:over|sur)\s*["'“”](.+?)["'“”]\s*\(?(?:n[°ro]\.?\s*)?(\d{6,8}[A-Za-z])\)?"#).unwrap())
}

fn time_regex() -> &'static Regex {
    TIME_REGEX.get_or_init(|| Regex::new(r"(\d{1,2})[.:](\d{2})\s*uur\b").unwrap())
}

fn date_regex() -> &'static Regex {
    DATE_REGEX.get_or_init(|| Regex::new(r"(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})").unwrap())
}

fn speaker_regex() -> &'static Regex {
    // NOTE: See IC311 question 1: some speaker paragraphs have multiple leading sequence numbers such as '01.02 01.03' instead of just '01.01'. (example vanessa matz)
    // We need to make sure to ignore all of them.
    SPEAKER_REGEX.get_or_init(|| Regex::new(
        r"(?m)(?:^|(?:NEWPARAGRAPH))[\n\r\s ]*(?:\d{2}\.\d{2}[\n\r\s ]+)*(\d{2}\.\d{2})[\n\r\s ]+([^:]+):|(?:Le  président|De  voorzitter)\s*:"
    ).unwrap())
}

fn speaker_name_regex() -> &'static Regex {
    SPEAKER_NAME_REGEX.get_or_init(|| Regex::new(r"^[^(,:\n\r]+").unwrap())
}

fn titles_regex() -> &'static Regex {
    TITLES_REGEX.get_or_init(|| Regex::new(r"^(Minister|De heer|Mevrouw|Le ministre|La ministre|Monsieur|Madame|Eerste minister|Staatssecretaris)\s+").unwrap())
}

fn chair_titles_regex() -> &'static Regex {
    CHAIR_TITLES_REGEX
        .get_or_init(|| Regex::new(r"(?i)\b(?:de\s+)?(?:mevrouw|heer|mevrouwen|heren)\b").unwrap())
}

fn chair_regex() -> &'static Regex {
    CHAIR_REGEX
        .get_or_init(|| Regex::new(r"(?i)voorgezeten\s+door\s+([^\.]+?)\s*(?:\.|$)").unwrap())
}

/// SELECTORS
static SELECTOR_SPAN: OnceLock<Selector> = OnceLock::new();
static SELECTOR_SPAN_P: OnceLock<Selector> = OnceLock::new();
static SELECTOR_TABLE: OnceLock<Selector> = OnceLock::new();
static SELECTOR_H2_OR_P: OnceLock<Selector> = OnceLock::new();

fn selector_span() -> &'static Selector {
    SELECTOR_SPAN.get_or_init(|| Selector::parse("span").unwrap())
}
fn selector_span_p() -> &'static Selector {
    SELECTOR_SPAN_P.get_or_init(|| Selector::parse("span, p").unwrap())
}
fn selector_table() -> &'static Selector {
    SELECTOR_TABLE.get_or_init(|| Selector::parse("table").unwrap())
}
fn selector_h2_or_p() -> &'static Selector {
    SELECTOR_H2_OR_P.get_or_init(|| Selector::parse("h2, p").unwrap())
}

struct ScrapedMeeting {
    session_id: u32,
    meeting_id: u32,
    date: String,
    time_of_day: String,
    start_time: String,
    end_time: String,
    commission: String,
    chair: String,
}

struct ScrapedQuestion {
    question_id: i32,
    session_id: u32,
    meeting_id: u32,
    questioners: String,
    questionees: String,
    respondents: String,
    topics_nl: String,
    topics_fr: String,
    discussion: String,
    internal_ids: String,
    date: String,
}

struct MeetingOutput {
    meeting: ScrapedMeeting,
    questions: Vec<ScrapedQuestion>,
}

struct QuestionData {
    questioners: Vec<String>,
    questionees: Vec<String>,
    respondents: Vec<String>,
    topics: Vec<String>,
    discussion: String,
    internal_ids: Vec<String>,
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
    MobiliteitOverheidsbedrijvenEnFederaleInstellingen,
    GezondheidEnGelijkeKansen,
    EnergieLeefmilieuEnKlimaat,
    InterparlementaireKlimaatdialoog,
    GrondwetEnInstitutioneleVernieuwing,
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
            Commission::Onbekend => write!(f, "onbekend"),
        }
    }
}

fn parse_commission_type(raw: &str) -> Commission {
    let raw = raw.trim().to_lowercase();
    if raw.contains("binnenlandse") {
        Commission::BinnenlandseZakenVeiligheidMigratieEnBestuurszaken
    } else if raw.contains("justitie") {
        Commission::Justitie
    } else if raw.contains("gezondheid") {
        Commission::GezondheidEnGelijkeKansen
    } else if raw.contains("economie") {
        Commission::EconomieConsumentenBeschermingEnDigitalisering
    } else if raw.contains("buitenlandse") {
        Commission::BuitenlandseBetrekkingen
    } else if raw.contains("mobiliteit") {
        Commission::MobiliteitOverheidsbedrijvenEnFederaleInstellingen
    } else if raw.contains("landsverdediging") {
        Commission::Landsverdediging
    } else if raw.contains("energie") {
        Commission::EnergieLeefmilieuEnKlimaat
    } else if raw.contains("sociale") {
        Commission::SocialeZakenWerkEnPensioenen
    } else if raw.contains("begroting") {
        Commission::FinancienEnBegroting
    } else if raw.contains("klimaatdialoog") {
        Commission::InterparlementaireKlimaatdialoog
    } else if raw.contains("grondwet") {
        Commission::GrondwetEnInstitutioneleVernieuwing
    } else {
        Commission::Onbekend
    }
}

macro_rules! col {
    ($rows:expr, $f:expr) => {
        Arc::new(StringArray::from($rows.iter().map($f).collect::<Vec<_>>())) as ArrayRef
    };
}

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

fn write_meetings(path: &Path, rows: &[ScrapedMeeting]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("time_of_day", DataType::Utf8, false),
        Field::new("start_time", DataType::Utf8, false),
        Field::new("end_time", DataType::Utf8, false),
        Field::new("commission", DataType::Utf8, false),
        Field::new("chair", DataType::Utf8, false),
    ]));
    write_parquet(
        path,
        schema,
        vec![
            col!(rows, |c| c.session_id.to_string()),
            col!(rows, |c| c.meeting_id.to_string()),
            col!(rows, |c| c.date.clone()),
            col!(rows, |c| c.time_of_day.clone()),
            col!(rows, |c| c.start_time.clone()),
            col!(rows, |c| c.end_time.clone()),
            col!(rows, |c| c.commission.clone()),
            col!(rows, |c| c.chair.clone()),
        ],
    )
}

fn write_questions(path: &Path, rows: &[ScrapedQuestion]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("question_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, false),
        Field::new("questioners", DataType::Utf8, false),
        Field::new("questionees", DataType::Utf8, false),
        Field::new("respondents", DataType::Utf8, false),
        Field::new("topics_nl", DataType::Utf8, false),
        Field::new("topics_fr", DataType::Utf8, false),
        Field::new("discussion", DataType::Utf8, false),
        Field::new("internal_ids", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
    ]));
    write_parquet(
        path,
        schema,
        vec![
            col!(rows, |q| q.question_id.to_string()),
            col!(rows, |q| q.session_id.to_string()),
            col!(rows, |q| q.meeting_id.to_string()),
            col!(rows, |q| q.questioners.clone()),
            col!(rows, |q| q.questionees.clone()),
            col!(rows, |q| q.respondents.clone()),
            col!(rows, |q| q.topics_nl.clone()),
            col!(rows, |q| q.topics_fr.clone()),
            col!(rows, |q| q.discussion.clone()),
            col!(rows, |q| q.internal_ids.clone()),
            col!(rows, |q| q.date.clone()),
        ],
    )
}

const SESSION_IDS: &[u32] = &[56, 55];

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let client = ScrapingClient::new();

    for &session_id in SESSION_IDS {
        if let Err(err) = scrape_session(&client, session_id).await {
            eprintln!(
                "[meetings-commission] session {} failed entirely: {}",
                session_id, err
            );
        }
    }

    Ok(())
}

/// Scrape a single session.
async fn scrape_session(client: &ScrapingClient, session_id: u32) -> Result<(), Box<dyn Error>> {
    let session_dir = data_dir()
        .join("sessions")
        .join(session_id.to_string())
        .join("commission");
    fs::create_dir_all(&session_dir).await?;

    let meeting_id_path = session_dir.join("current_commission_id.txt");
    let current_meeting_id: u32 = if meeting_id_path.exists() {
        std::fs::read_to_string(&meeting_id_path)?.trim().parse()?
    } else {
        0
    };

    let mut web_request_count = 0u32;
    let last_meeting_id = discover_last_meeting_id(
        &client,
        session_id,
        current_meeting_id,
        &mut web_request_count,
    )
    .await?;

    if last_meeting_id == current_meeting_id {
        println!("[meetings-commission] no new meeting available to download");
    } else {
        println!(
            "[meetings-commission] found new meetings up to {}",
            last_meeting_id
        );
    }

    let mut all_meetings = Vec::new();
    let mut all_questions = Vec::new();

    let mp = MultiProgress::new();
    let meetings_pb = mp.add(ProgressBar::new(last_meeting_id as u64));
    meetings_pb.set_style(
        ProgressStyle::with_template(
            "[meetings-commission] [{elapsed_precise}] {spinner:.blue} {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {msg}",
        )?
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );

    meetings_pb.set_message(web_request_count.to_string());

    for meeting_id in 1..=last_meeting_id {
        meetings_pb.set_message(format!("reqs={} meeting={}", web_request_count, meeting_id));

        match scrape_meeting(&client, session_id, meeting_id, &mut web_request_count).await {
            Ok(output) => {
                all_meetings.push(output.meeting);
                all_questions.extend(output.questions);
            }
            Err(_err) => {
                // NOTE: Some meetings are empty and so fail to scrape.
                // eprintln!(
                //     "[meetings-commission] failed meeting {}: {}",
                //     meeting_id, err
                // );
            }
        }

        meetings_pb.set_message(web_request_count.to_string());
        meetings_pb.inc(1);
    }

    meetings_pb.finish_with_message("done");
    std::fs::write(&meeting_id_path, last_meeting_id.to_string())?;

    write_meetings(&session_dir.join("meetings.parquet"), &all_meetings)?;
    write_questions(&session_dir.join("questions.parquet"), &all_questions)?;

    println!(
        "[meetings-commission] scraped {} meetings using {} web requests",
        all_meetings.len(),
        web_request_count
    );
    Ok(())
}

async fn discover_last_meeting_id(
    client: &ScrapingClient,
    session_id: u32,
    current_id: u32,
    web_request_count: &mut u32,
) -> Result<u32, Box<dyn Error>> {
    let mut last = current_id;
    let mut probe = current_id + 1;
    let mut misses = 0;

    println!(
        "[meetings-commission] discovering meetings for session {}, starting at {}",
        session_id, current_id
    );

    loop {
        println!("[meetings-commission] probing meeting {}...", probe);

        let url = format!(
            "https://www.dekamer.be/doc/CCRI/html/{}/ic{:03}x.html",
            session_id, probe
        );

        let resp = client.get(&url).await?;
        *web_request_count += 1;

        println!(
            "[meetings-commission] meeting {} -> HTTP {}",
            probe,
            resp.status()
        );

        if resp.status() == StatusCode::NOT_FOUND {
            misses += 1;

            // Allow up to 3 consecutive missing meeting IDs.
            if misses > 3 {
                break;
            }
        } else {
            last = probe;
            misses = 0;
        }

        probe += 1;
    }

    println!(
        "[meetings-commission] last meeting for session {} = {}",
        session_id, last
    );

    Ok(last)
}

async fn scrape_meeting(
    client: &ScrapingClient,
    session_id: u32,
    meeting_id: u32,
    web_request_count: &mut u32,
) -> Result<MeetingOutput, Box<dyn Error>> {
    let filepath = cache_dir().join(format!(
        "sessions/{}/meetings/commission/{}-{}.html",
        session_id, session_id, meeting_id
    ));

    if !filepath.exists() {
        let url = format!(
            "https://www.dekamer.be/doc/CCRI/html/{}/ic{:03}x.html",
            session_id, meeting_id
        );
        let response = client.get(&url).await?;
        *web_request_count += 1;
        let raw_bytes = response.bytes().await?;
        let (decoded_str, _, _) = WINDOWS_1252.decode(&raw_bytes);
        if let Some(parent) = filepath.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&filepath, decoded_str.as_ref())?;
    }

    let content = read_to_string(&filepath)?;
    let document = Html::parse_document(&content);

    let date = extract_date_from_document(&document)?;
    let time_of_day = extract_time_of_day_from_document(&document)?;
    let start_time = extract_start_time_from_document(&document)?;
    let end_time = extract_end_time_from_document(&document)?;
    let chair = extract_chair_from_document(&document)?;
    let commission = extract_commission_from_document(&document)?;

    let mut questions = extract_questions(&document, session_id, meeting_id, &date)?;
    // merge_duplicate_questions(&mut questions);

    Ok(MeetingOutput {
        meeting: ScrapedMeeting {
            session_id,
            meeting_id,
            date,
            time_of_day,
            start_time,
            end_time,
            commission,
            chair,
        },
        questions,
    })
}

fn extract_questions(
    document: &Html,
    session_id: u32,
    meeting_id: u32,
    date: &str,
) -> Result<Vec<ScrapedQuestion>, Box<dyn Error>> {
    let mut questions = Vec::new();
    let mut previous_nl = String::new();
    let mut previous_fr = String::new();
    let mut previous_discussion = String::new();
    let mut question_id: i32 = 0;

    // Commission reports always contain questions from the start; no section header needed.
    let french_indicators = ["questions jointes", "question de"];
    let dutch_indicators = ["samengevoegde vragen", "toegevoegde vragen", "vraag van"];

    let flush = |id: i32,
                 nl: &str,
                 fr: &str,
                 discussion: &str|
     -> Result<Option<ScrapedQuestion>, Box<dyn Error>> {
        if nl.is_empty() && fr.is_empty() {
            return Ok(None);
        }
        let data_nl = extract_question_data(nl, discussion)?;
        let data_fr = extract_question_data(fr, discussion)?;
        Ok(Some(ScrapedQuestion {
            question_id: id,
            session_id,
            meeting_id,
            questioners: data_nl.questioners.join(","),
            questionees: data_nl.questionees.join(","),
            respondents: data_nl.respondents.join(","),
            topics_nl: data_nl.topics.join(";"),
            topics_fr: data_fr.topics.join(";"),
            discussion: data_nl.discussion,
            internal_ids: data_nl.internal_ids.join(","),
            date: date.to_string(),
        }))
    };

    for element in document.select(selector_h2_or_p()) {
        let tag = element.value().name();

        if tag == "h2" {
            let mut found_nl: Option<String> = None;
            let mut found_fr: Option<String> = None;

            // Dutch spans: swap to FR if they look French.
            if let Some(span) = element
                .select(selector_span())
                .filter(|s| matches!(s.value().attr("lang"), Some("NL") | Some("NL-BE")))
                .last()
            {
                let text =
                    clean_text(&span.text().collect::<Vec<_>>().join(" ")).replace("\"", "'");
                let text = handle_one_off_issues(text);
                if french_indicators
                    .iter()
                    .any(|w| text.to_lowercase().contains(w))
                {
                    found_fr = Some(text);
                } else {
                    found_nl = Some(text);
                }
            }

            // French spans: swap to NL if they look Dutch.
            if let Some(span) = element
                .select(selector_span())
                .filter(|s| s.value().attr("lang") == Some("FR"))
                .last()
            {
                let text =
                    clean_text(&span.text().collect::<Vec<_>>().join(" ")).replace("\"", "'");
                let text = handle_one_off_issues(text);
                if dutch_indicators
                    .iter()
                    .any(|w| text.to_lowercase().contains(w))
                {
                    found_nl = Some(text);
                } else {
                    found_fr = Some(text);
                }
            }

            let is_hearing = found_nl
                .as_deref()
                .map_or(false, |t| t.to_lowercase().contains("hoorzitting"))
                || found_fr
                    .as_deref()
                    .map_or(false, |t| t.to_lowercase().contains("audition"));

            if is_hearing {
                // Flush any pending question that came before this hearing,
                // then reset state so the hearing's discussion doesn't bleed in.
                if !previous_nl.is_empty() && !previous_fr.is_empty() {
                    if let Some(q) = flush(
                        question_id,
                        &previous_nl,
                        &previous_fr,
                        &previous_discussion,
                    )? {
                        questions.push(q);
                        question_id += 1;
                    }
                }
                previous_nl.clear();
                previous_fr.clear();
                previous_discussion.clear();
                continue;
            }

            // NOTE: ic017x.html uses "toegevoegde vragen" instead of "Samengevoegde" for grouped questions.
            let is_group_start = found_nl.as_deref().map_or(false, |t| {
                t.starts_with("Samengevoegde") || t.contains("toegevoegde vragen")
            }) || found_fr.as_deref().map_or(false, |t| t.contains("jointes"));
            let is_subquestion = found_nl.as_deref().map_or(false, |t| t.starts_with("-"))
                || found_fr.as_deref().map_or(false, |t| t.starts_with("-"));
            let is_single = found_nl
                .as_deref()
                .map_or(false, |t| t.starts_with("Vraag van"))
                || found_fr
                    .as_deref()
                    .map_or(false, |t| t.starts_with("Question de"));

            if is_group_start || is_single {
                if !previous_nl.is_empty() && !previous_fr.is_empty() {
                    if let Some(q) = flush(
                        question_id,
                        &previous_nl,
                        &previous_fr,
                        &previous_discussion,
                    )? {
                        questions.push(q);
                        question_id += 1;
                    }
                    previous_discussion.clear();
                    previous_nl.clear();
                    previous_fr.clear();
                }
                if let Some(t) = found_nl {
                    previous_nl = t;
                }
                if let Some(t) = found_fr {
                    previous_fr = t;
                }
            } else if is_subquestion {
                if let Some(t) = found_nl {
                    previous_nl.push('\n');
                    previous_nl.push_str(&t);
                }
                if let Some(t) = found_fr {
                    previous_fr.push('\n');
                    previous_fr.push_str(&t);
                }
            }
        }

        if tag == "p" {
            let text = element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_string();
            if !text.is_empty() {
                let text = handle_one_off_issues(text);
                previous_discussion.push_str(&clean_text(&text));
                previous_discussion.push_str("NEWPARAGRAPH");
            }
        }
    }

    // Flush the last question.
    if !previous_nl.is_empty() && !previous_fr.is_empty() {
        if let Some(q) = flush(
            question_id,
            &previous_nl,
            &previous_fr,
            &previous_discussion,
        )? {
            questions.push(q);
        }
    }

    Ok(questions)
}

/// Handle one-off mistakes from the commission meeting reports.
fn handle_one_off_issues(text: String) -> String {
    // Meeting C165: question 12 has '-Kjell Vander Elst' instead of '- Kjell Vander Elst' so a missing space, handle it here
    let text = {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| Regex::new(r"^-(\S)").unwrap());
        re.replace(&text, "- $1").into_owned()
    };

    // Meeting C129: question 2 contains an additional 'Vraag van Xavier Dubois' instead of just 'Xavier Dubois', handle it here
    let text = if let Some(rest) = text.strip_prefix("- Vraag van ") {
        format!("- {}", rest)
    } else {
        text
    };

    // Meeting C393: contains 'Isabelle Hansez Je vous remercie beaucoup', with missing colon
    let text = {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE
            .get_or_init(|| Regex::new(r"^(\d{2}\.\d{2}\s+Isabelle\s+Hansez)\s+([A-Z])").unwrap());
        re.replace(&text, "$1: $2").into_owned()
    };

    // Meeting 188: contains 'Stefaan Van Hecke (Ecolo-Groen:' without closing round bracket
    let text = {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| Regex::new(r"(\([^():\n]*?):\s").unwrap());
        re.replace(&text, "$1): ").into_owned()
    };

    // Meeting 161: contains 'Minister Jan Jambon Ik denk dat het zo in de budgettaire tabel staat. Ik zou het moeten nakijken.' with missing colon
    let text = {
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| {
            Regex::new(r"^(\d{2}\.\d{2}\s+Minister\s+Jan\s+Jambon)\s+([A-ZÀ-Ý])").unwrap()
        });
        re.replace(&text, "$1: $2").into_owned()
    };

    text
}

fn extract_question_data(
    question_text: &str,
    discussion_text: &str,
) -> Result<QuestionData, Box<dyn Error>> {
    let mut questioners = Vec::new();
    let mut topics = Vec::new();
    let mut questionees = Vec::new();
    let mut internal_ids = Vec::new();

    for capture in question_regex().captures_iter(question_text) {
        let questioner = capture[1]
            .trim()
            .replace("- ", "")
            .replace("de heer ", "")
            .trim()
            .to_string();
        let questionee = capture
            .get(2)
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_else(|| "Onbekend".to_string());
        let topic = capture[3].trim().to_string();
        let dossier_id = format!("Q{}", capture[4].trim());

        questioners.push(questioner);
        if !questionees.contains(&questionee) {
            questionees.push(questionee);
        }
        internal_ids.push(dossier_id);
        topics.push(topic);
    }

    // The actual respondents may differ from the questionees so we need to extract the actual respondents from the discussion text.
    let speakers = extract_speakers_from_discussion(discussion_text);
    let respondents: Vec<String> = speakers
        .into_iter()
        .filter(|s| !questioners.contains(s))
        .collect();

    Ok(QuestionData {
        questioners,
        questionees,
        respondents,
        topics,
        discussion: get_discussion_json(discussion_text),
        internal_ids,
    })
}

/// Look at the discussion and extract the actual speakers.
fn extract_speakers_from_discussion(discussion_text: &str) -> Vec<String> {
    let mut seen = Vec::new();
    for paragraph in discussion_text.split("NEWPARAGRAPH") {
        let Some(raw) = discussion_speaker_regex()
            .captures(paragraph.trim_start())
            .map(|cap| cap[1].to_string())
        else {
            continue;
        };
        let cleaned = normalize_speaker_name(&raw);
        if !seen.contains(&cleaned) {
            seen.push(cleaned);
        }
    }
    seen
}

/// Strip title from a speaker name.
fn normalize_speaker_name(raw: &str) -> String {
    let no_parens = paren_regex().replace_all(raw, " ");
    let no_title = titles_regex().replace(no_parens.trim(), "");
    whitespace_regex()
        .replace_all(no_title.trim(), " ")
        .trim()
        .to_string()
}

fn paren_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\([^)]*\)").unwrap())
}

fn whitespace_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\s+").unwrap())
}

fn discussion_speaker_regex() -> &'static Regex {
    // NOTE: See IC311 question 1: some speaker paragraphs have multiple leading sequence numbers such as '01.02 01.03' instead of just '01.01'. (example vanessa matz)
    // We need to make sure to ignore all of them.
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^(?:\d{2}\.\d{2}\s+)*\d{2}\.\d{2}\s+([^,:\n]+?)\s*[,:]").unwrap()
    })
}

fn get_discussion_json(input: &str) -> String {
    let cleaned_input = clean_text(input);
    // NOTE: The timestamp regex anchors on NEWPARAGRAPH / line-start to avoid false-positives
    // on times that appear mid-sentence (e.g. "na 20.00 uur").

    let mut discussion = Vec::new();
    let mut current_speaker = String::new();
    let mut last_end = 0;

    for cap in speaker_regex().captures_iter(&cleaned_input) {
        let match_start = cap.get(0).unwrap().start();
        let text_segment = cleaned_input[last_end..match_start].trim();

        if !current_speaker.is_empty() && !text_segment.is_empty() {
            let clean_segment = text_segment
                .replace("Het incident is gesloten.", "")
                .replace("L'incident est clos.", "")
                .trim()
                .to_string();
            if !clean_segment.is_empty() {
                discussion
                    .push(json!({ "speaker": current_speaker.trim(), "text": clean_segment }));
            }
        }

        current_speaker = if let Some(full_speaker) = cap.get(2) {
            let stripped = titles_regex()
                .replace(full_speaker.as_str().trim(), "")
                .to_string();
            speaker_name_regex()
                .captures(&stripped)
                .and_then(|c| c.get(0))
                .map_or("Onbekend".to_string(), |m| m.as_str().to_string())
        } else {
            "Voorzitter".to_string()
        };

        last_end = cap.get(0).unwrap().end();
    }

    if !current_speaker.is_empty() && last_end < cleaned_input.len() {
        let clean_segment = cleaned_input[last_end..]
            .replace("Het incident is gesloten.", "")
            .replace("L'incident est clos.", "")
            .replace("NEWPARAGRAPH", "\n")
            .trim()
            .to_string();
        if !clean_segment.is_empty() {
            discussion.push(json!({ "speaker": current_speaker.trim(), "text": clean_segment }));
        }
    }

    serde_json::to_string_pretty(&discussion).unwrap()
}

fn extract_date_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let first_table = document
        .select(selector_table())
        .next()
        .ok_or("No table found")?;

    let text: String = first_table
        .select(selector_span())
        .flat_map(|s| s.text().map(str::to_owned))
        .collect::<Vec<_>>()
        .join(" ");

    let caps = date_regex()
        .captures(&text)
        .ok_or("Could not find date in document")?;

    let day = format!("{:02}", caps[1].parse::<u8>()?);
    let month = match &caps[2].to_lowercase()[..] {
        "januari" => "01",
        "februari" => "02",
        "maart" => "03",
        "april" => "04",
        "mei" => "05",
        "juni" => "06",
        "juli" => "07",
        "augustus" => "08",
        "september" => "09",
        "oktober" => "10",
        "november" => "11",
        "december" => "12",
        _ => return Err("Invalid month name".into()),
    };
    Ok(format!("{}-{}-{}", &caps[3], month, day))
}

fn extract_time_of_day_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    for span in document.select(selector_span()) {
        let text = span
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_lowercase();
        match text.as_str() {
            "namiddag" => return Ok("afternoon".to_string()),
            "voormiddag" => return Ok("morning".to_string()),
            "avond" => return Ok("evening".to_string()),
            _ => {}
        }
    }
    Err("Could not extract time of day from the document".into())
}

fn extract_start_time_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    // NOTE: Commission 270 used "14:13 uur" (colon) instead of the usual "14.13 uur" (dot).
    let keywords = [
        "De behandeling van de",
        "De behandeling van de vragen en de interpellatie vangt aan om",
        "De behandeling van de vragen en interpellaties vangt aan",
        "De behandeling van de vragen en van de interpellatie vangt aan om",
        "De openbare commissievergadering wordt geopend",
        "De vergadering wordt geopend",
        "De behandeling van de vragen vangt aan",
        "De gedachtewisseling vangt aan",
        "De behandeling van de interpellatie vangt",
    ];
    extract_time_from_document(document, &keywords)
        .ok_or_else(|| "Could not extract start time from the document".into())
}

fn extract_end_time_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let keywords = [
        "De openbare commissievergadering wordt gesloten",
        "De gedachtewisseling met de ministers eindigt",
        "De behandeling van de vragen eindigt",
        "De gedachtewisseling eindigt",
        "De vergadering wordt gesloten",
        "De behandeling van de interpellatie eindigt",
        "De behandeling van de interpellaties eindigt",
        "De behandeling van de vragen en interpellaties eindigt om",
    ];
    extract_time_from_document(document, &keywords)
        .ok_or_else(|| "Could not extract end time from the document".into())
}

fn extract_time_from_document(document: &Html, keywords: &[&str]) -> Option<String> {
    let mut last_time: Option<String> = None;

    for node in document.select(selector_span_p()) {
        let text = node.text().collect::<Vec<_>>().join(" ").replace('\n', " ");
        if keywords.iter().any(|&kw| text.contains(kw)) {
            if let Some(caps) = time_regex().captures(&text) {
                last_time = Some(format!("{}h{}", &caps[1], &caps[2]));
            }
        }
    }
    last_time
}

fn extract_chair_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    for node in document.select(selector_span_p()) {
        let text = node.text().collect::<Vec<_>>().join(" ").replace('\n', " ");
        if let Some(caps) = chair_regex().captures(&text) {
            let chunk = caps[1].replace('\u{00A0}', " ").trim().to_string();
            let names: Vec<String> = chunk
                .split(" en ")
                .filter_map(|part| {
                    let clean = chair_titles_regex()
                        .replace_all(part.trim(), "")
                        .trim()
                        .to_string();
                    if clean.is_empty() { None } else { Some(clean) }
                })
                .collect();
            if !names.is_empty() {
                return Ok(names.join(", "));
            }
        }
    }
    Err("Could not extract chair".into())
}

fn extract_commission_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    let first_table = document
        .select(selector_table())
        .next()
        .ok_or("No table found")?;

    let raw = first_table
        .select(selector_span())
        .next()
        .ok_or("No span in first table")?
        .text()
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    Ok(parse_commission_type(&raw).to_string())
}

fn merge_duplicate_questions(questions: &mut Vec<ScrapedQuestion>) {
    let mut merged: Vec<ScrapedQuestion> = Vec::with_capacity(questions.len());

    for question in questions.drain(..) {
        let duplicate_idx = merged.iter().position(|existing| {
            existing.session_id == question.session_id
                && existing.meeting_id == question.meeting_id
                && existing.date == question.date
                && existing.internal_ids == question.internal_ids
                && existing.questioners == question.questioners
                && existing.questionees == question.questionees
                && existing.topics_nl == question.topics_nl
                && existing.topics_fr == question.topics_fr
        });

        if let Some(idx) = duplicate_idx {
            let existing = &mut merged[idx];

            eprintln!(
                "[meetings-commission] WARNING: merging duplicate question {} into question {} \
                 (meeting {}, internal_ids={})",
                question.question_id,
                existing.question_id,
                question.meeting_id,
                question.internal_ids
            );

            existing.respondents = merge_csv_values(&existing.respondents, &question.respondents);

            existing.discussion = merge_discussion_json(&existing.discussion, &question.discussion);
        } else {
            merged.push(question);
        }
    }

    *questions = merged;

    // Re-number question IDs after merging.
    for (idx, question) in questions.iter_mut().enumerate() {
        question.question_id = idx as i32;
    }
}

fn merge_csv_values(left: &str, right: &str) -> String {
    let mut values = Vec::new();

    for value in left.split(',').chain(right.split(',')) {
        let value = value.trim();

        if !value.is_empty() && !values.iter().any(|v| v == value) {
            values.push(value.to_string());
        }
    }

    values.join(",")
}

fn merge_discussion_json(left: &str, right: &str) -> String {
    let mut left_value: Vec<serde_json::Value> = serde_json::from_str(left).unwrap_or_default();

    let right_value: Vec<serde_json::Value> = serde_json::from_str(right).unwrap_or_default();

    left_value.extend(right_value);

    serde_json::to_string_pretty(&left_value).unwrap_or_else(|_| left.to_string())
}
