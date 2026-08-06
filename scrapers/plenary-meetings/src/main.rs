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
use scraper::{ElementRef, Html, Selector};
use serde_json::json;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, read_to_string};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use tokio::fs;

/// REGEXES
static PARAGRAPH_VOTE_REGEX: OnceLock<Regex> = OnceLock::new();
static PARAGRAPH_VOTE_SECTION_REGEX: OnceLock<Regex> = OnceLock::new();
static QUESTION_REGEX: OnceLock<Regex> = OnceLock::new();
static TIME_REGEX: OnceLock<Regex> = OnceLock::new();
static DATE_REGEX: OnceLock<Regex> = OnceLock::new();
static SPEAKER_REGEX: OnceLock<Regex> = OnceLock::new();
static SPEAKER_NAME_REGEX: OnceLock<Regex> = OnceLock::new();
static TITLES_REGEX: OnceLock<Regex> = OnceLock::new();
static PROPOSITION_REGEX: OnceLock<Regex> = OnceLock::new();
static PROPOSITION_TOPIC_REGEX: OnceLock<Regex> = OnceLock::new();
static VOTE_REGEX_1: OnceLock<Regex> = OnceLock::new();
static VOTE_REGEX_2: OnceLock<Regex> = OnceLock::new();
static VOTE_REGEX_3: OnceLock<Regex> = OnceLock::new();

fn paragraph_vote_regex() -> &'static Regex {
    PARAGRAPH_VOTE_REGEX.get_or_init(|| {
        Regex::new(r"\(\s*\d{1,5}(?:\s*\/\s*\d{1,5}(?:\s*-\s*\d{1,5})?)?\s*\)\s*$").unwrap()
    })
}

fn paragraph_vote_section_regex() -> &'static Regex {
    PARAGRAPH_VOTE_SECTION_REGEX.get_or_init(|| Regex::new(r"\(Stemming\/vote (\d+)\)").unwrap())
}

fn question_regex() -> &'static Regex {
    // NOTE: Handles question IDs in the format of `(56001442P)`
    QUESTION_REGEX.get_or_init(|| {
        Regex::new(r#"(?m)(?:(?:Vraag van|Question de)\s)?([^\n]+?)\s+(?:aan|à)\s+([^\n]+?)\s*\([^)]*\)\s*(?:over|sur)\s*(.+?)(?:\s*\((\d{8}[A-Z])\))?\s*$"#).unwrap()
    })
}

fn time_regex() -> &'static Regex {
    TIME_REGEX.get_or_init(|| Regex::new(r"(\d{1,2})\.(\d{2})\s*uur").unwrap())
}

fn date_regex() -> &'static Regex {
    DATE_REGEX.get_or_init(|| Regex::new(r"(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})").unwrap())
}

fn speaker_regex() -> &'static Regex {
    SPEAKER_REGEX.get_or_init(|| Regex::new(r"(?m)(?:^|(?:NEWPARAGRAPH))[\n\r\s ]*(\d{2}\.\d{2})[\n\r\s ]+([^:]+):|(?:Le  président|De  voorzitter)\s*:").unwrap())
}

fn speaker_name_regex() -> &'static Regex {
    SPEAKER_NAME_REGEX.get_or_init(|| Regex::new(r"^[^(,:\n\r]+").unwrap())
}

fn titles_regex() -> &'static Regex {
    TITLES_REGEX.get_or_init(|| Regex::new(r"^(Minister|De heer|Mevrouw|Le ministre|La ministre|Monsieur|Madame|Eerste minister|Staatssecretaris)\s+").unwrap())
}

fn proposition_regex() -> &'static Regex {
    PROPOSITION_REGEX.get_or_init(|| Regex::new(r#"^(.+?)\s*\((\d+)\/(\d+(?:-\d+)?)\).*"#).unwrap())
    // PROPOSITION_REGEX.get_or_init(|| Regex::new(r#"^((?:Voorstel van resolutie|Proposition de résolution|Wetsvoorstel|Proposition de loi|Wetsontwerp|Projet de loi|Voorstel tot|Proposition visant|Voorstel van bijzondere wet).*)\((\d+)\/(\d+(?:-\d+)?)\).*$"#).unwrap())
}

fn proposition_topic_regex() -> &'static Regex {
    PROPOSITION_TOPIC_REGEX.get_or_init(|| Regex::new(r#"^([^(]*)"#).unwrap())
}

fn vote_regex_1() -> &'static Regex {
    VOTE_REGEX_1.get_or_init(|| Regex::new(r#"^(.*)\((\d+)/(\d+(?:-\d+)?)\)\s*$"#).unwrap())
}

fn vote_regex_2() -> &'static Regex {
    VOTE_REGEX_2.get_or_init(|| Regex::new(r#"^(.*)\s+\((?:nr\.|n°)\s*(\d+)\)\s*$"#).unwrap())
}

fn vote_regex_3() -> &'static Regex {
    VOTE_REGEX_3.get_or_init(|| Regex::new(r#"^([^(]*)"#).unwrap())
}

/// SELECTORS
static SELECTOR_SPAN: OnceLock<Selector> = OnceLock::new();
static SELECTOR_TR: OnceLock<Selector> = OnceLock::new();
static SELECTOR_TD: OnceLock<Selector> = OnceLock::new();
static SELECTOR_TABLE: OnceLock<Selector> = OnceLock::new();
static SELECTOR_H1_OR_H2: OnceLock<Selector> = OnceLock::new();
static SELECTOR_H1_OR_H2_OR_P: OnceLock<Selector> = OnceLock::new();
static SELECTOR_H1_OR_H2_OR_TABLE_OR_P: OnceLock<Selector> = OnceLock::new();

fn selector_span() -> &'static Selector {
    SELECTOR_SPAN.get_or_init(|| Selector::parse("span").unwrap())
}
fn selector_tr() -> &'static Selector {
    SELECTOR_TR.get_or_init(|| Selector::parse("tr").unwrap())
}
fn selector_td() -> &'static Selector {
    SELECTOR_TD.get_or_init(|| Selector::parse("td").unwrap())
}
fn selector_table() -> &'static Selector {
    SELECTOR_TABLE.get_or_init(|| Selector::parse("table").unwrap())
}
fn selector_h1_or_h2() -> &'static Selector {
    SELECTOR_H1_OR_H2.get_or_init(|| Selector::parse("h1, h2").unwrap())
}
fn selector_h1_or_h2_or_p() -> &'static Selector {
    SELECTOR_H1_OR_H2_OR_P.get_or_init(|| Selector::parse("h1, h2, p").unwrap())
}
fn selector_h1_or_h2_or_table_or_p() -> &'static Selector {
    SELECTOR_H1_OR_H2_OR_TABLE_OR_P.get_or_init(|| Selector::parse("h1, h2, table, p").unwrap())
}

struct ScrapedMeeting {
    session_id: u32,
    meeting_id: u32,
    date: String,
    time_of_day: String,
    start_time: String,
    end_time: String,
}

struct ScrapedVote {
    vote_id: i32,
    session_id: u32,
    meeting_id: u32,
    date: String,
    title_nl: String,
    title_fr: String,
    yes: u32,
    no: u32,
    abstain: u32,
    members_yes: String,
    members_no: String,
    members_abstain: String,
    dossier_id: String,
    document_id: String,
    motion_id: String,
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

struct ScrapedProposition {
    proposition_id: i32,
    session_id: u32,
    meeting_id: u32,
    title_nl: String,
    title_fr: String,
    dossier_id: String,
    document_id: String,
    date: String,
}

struct ScrapedNotice {
    notice_id: i32,
    session_id: u32,
    meeting_id: u32,
    title_nl: String,
    title_fr: String,
    date: String,
}

struct MeetingOutput {
    meeting: ScrapedMeeting,
    questions: Vec<ScrapedQuestion>,
    propositions: Vec<ScrapedProposition>,
    votes: Vec<ScrapedVote>,
    notices: Vec<ScrapedNotice>,
}

struct QuestionData {
    questioners: Vec<String>,
    questionees: Vec<String>,
    respondents: Vec<String>,
    topics: Vec<String>,
    discussion: String,
    internal_ids: Vec<String>,
}

impl Default for QuestionData {
    fn default() -> Self {
        Self {
            questioners: Vec::new(),
            questionees: Vec::new(),
            respondents: Vec::new(),
            topics: Vec::new(),
            discussion: String::new(),
            internal_ids: Vec::new(),
        }
    }
}

struct PropositionData {
    topic: String,
    dossier_id: Option<String>,
    document_id: Option<String>,
}

struct VoteData {
    topic: String,
    dossier_id: Option<String>,
    document_id: Option<String>,
    motion_id: Option<String>,
}

struct VoteRecord {
    vote_number: String,
    yes: u32,
    no: u32,
    abstain: u32,
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
    ]));
    write_parquet(
        path,
        schema,
        vec![
            col!(rows, |m| m.session_id.to_string()),
            col!(rows, |m| m.meeting_id.to_string()),
            col!(rows, |m| m.date.clone()),
            col!(rows, |m| m.time_of_day.clone()),
            col!(rows, |m| m.start_time.clone()),
            col!(rows, |m| m.end_time.clone()),
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

fn write_propositions(path: &Path, rows: &[ScrapedProposition]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("proposition_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, false),
        Field::new("title_nl", DataType::Utf8, false),
        Field::new("title_fr", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("document_id", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
    ]));
    write_parquet(
        path,
        schema,
        vec![
            col!(rows, |p| p.proposition_id.to_string()),
            col!(rows, |p| p.session_id.to_string()),
            col!(rows, |p| p.meeting_id.to_string()),
            col!(rows, |p| p.title_nl.clone()),
            col!(rows, |p| p.title_fr.clone()),
            col!(rows, |p| p.dossier_id.clone()),
            col!(rows, |p| p.document_id.clone()),
            col!(rows, |p| p.date.clone()),
        ],
    )
}

fn write_votes(path: &Path, rows: &[ScrapedVote]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("vote_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
        Field::new("title_nl", DataType::Utf8, false),
        Field::new("title_fr", DataType::Utf8, false),
        Field::new("yes", DataType::Utf8, false),
        Field::new("no", DataType::Utf8, false),
        Field::new("abstain", DataType::Utf8, false),
        Field::new("members_yes", DataType::Utf8, false),
        Field::new("members_no", DataType::Utf8, false),
        Field::new("members_abstain", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("document_id", DataType::Utf8, false),
        Field::new("motion_id", DataType::Utf8, false),
    ]));
    write_parquet(
        path,
        schema,
        vec![
            col!(rows, |v| v.vote_id.to_string()),
            col!(rows, |v| v.session_id.to_string()),
            col!(rows, |v| v.meeting_id.to_string()),
            col!(rows, |v| v.date.clone()),
            col!(rows, |v| v.title_nl.clone()),
            col!(rows, |v| v.title_fr.clone()),
            col!(rows, |v| v.yes.to_string()),
            col!(rows, |v| v.no.to_string()),
            col!(rows, |v| v.abstain.to_string()),
            col!(rows, |v| v.members_yes.clone()),
            col!(rows, |v| v.members_no.clone()),
            col!(rows, |v| v.members_abstain.clone()),
            col!(rows, |v| v.dossier_id.clone()),
            col!(rows, |v| v.document_id.clone()),
            col!(rows, |v| v.motion_id.clone()),
        ],
    )
}

fn write_notices(path: &Path, rows: &[ScrapedNotice]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("notice_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, false),
        Field::new("title_nl", DataType::Utf8, false),
        Field::new("title_fr", DataType::Utf8, false),
        Field::new("date", DataType::Utf8, false),
    ]));
    write_parquet(
        path,
        schema,
        vec![
            col!(rows, |n| n.notice_id.to_string()),
            col!(rows, |n| n.session_id.to_string()),
            col!(rows, |n| n.meeting_id.to_string()),
            col!(rows, |n| n.title_nl.clone()),
            col!(rows, |n| n.title_fr.clone()),
            col!(rows, |n| n.date.clone()),
        ],
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let client = ScrapingClient::new();
    let session_id: u32 = 56;

    let session_dir = data_dir()
        .join("sessions")
        .join(session_id.to_string())
        .join("plenary");
    fs::create_dir_all(&session_dir).await?;

    let meeting_id_path = data_dir().join("current_plenary_id.txt");
    let current_meeting_id: u32 = std::fs::read_to_string(&meeting_id_path)?.trim().parse()?;

    let mut web_request_count = 0u32;
    let last_meeting_id = discover_last_meeting_id(
        &client,
        session_id,
        current_meeting_id,
        &mut web_request_count,
    )
    .await?;

    if last_meeting_id == current_meeting_id {
        println!("[meetings-plenary] no new meeting available to download");
    } else {
        println!(
            "[meetings-plenary] found new meetings up to {}",
            last_meeting_id
        );
    }

    let mut all_meetings = Vec::new();
    let mut all_questions = Vec::new();
    let mut all_propositions = Vec::new();
    let mut all_notices = Vec::new();
    let mut all_votes = Vec::new();

    let mp = MultiProgress::new();
    let meetings_pb = mp.add(ProgressBar::new(last_meeting_id as u64));
    meetings_pb.set_style(
        ProgressStyle::with_template(
            "[meetings-plenary] [{elapsed_precise}] {spinner:.blue} {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {msg}",
        )?
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );

    meetings_pb.set_message(web_request_count.to_string());

    // Collect dossier ids mentioned within the meetings
    let mut encountered_dossier_ids: HashMap<String, String> = HashMap::new();

    for meeting_id in 1..=last_meeting_id {
        meetings_pb.set_message(format!("reqs={} meeting={}", web_request_count, meeting_id));

        match scrape_meeting(
            &client,
            session_id,
            meeting_id,
            &mut web_request_count,
            &mut encountered_dossier_ids,
        )
        .await
        {
            Ok(output) => {
                all_meetings.push(output.meeting);
                all_questions.extend(output.questions);
                all_propositions.extend(output.propositions);
                all_notices.extend(output.notices);
                all_votes.extend(output.votes);
            }
            Err(err) => {
                eprintln!("[meetings-plenary] failed meeting {}: {}", meeting_id, err);
            }
        };

        meetings_pb.set_message(web_request_count.to_string());
        meetings_pb.inc(1);
    }

    // Write collected dossier ids
    let ids_path = cache_dir().join(format!("sessions/{}/dossier_ids.txt", session_id));
    let mut lines: Vec<String> = encountered_dossier_ids
        .iter()
        .map(|(id, date)| format!("{}\t{}", id, date))
        .collect();
    lines.sort();
    std::fs::write(&ids_path, lines.join("\n"))?;

    meetings_pb.finish_with_message("done");

    std::fs::write(&meeting_id_path, last_meeting_id.to_string())?;

    write_meetings(&session_dir.join("meetings.parquet"), &all_meetings)?;
    write_questions(&session_dir.join("questions.parquet"), &all_questions)?;
    write_propositions(&session_dir.join("propositions.parquet"), &all_propositions)?;
    write_notices(&session_dir.join("notices.parquet"), &all_notices)?;
    write_votes(&session_dir.join("votes.parquet"), &all_votes)?;

    println!(
        "[meetings-plenary] scraped {} meetings using {} web requests",
        all_meetings.len(),
        web_request_count
    );
    Ok(())
}

fn record_dossier(map: &mut HashMap<String, String>, id: &str, date: &str) {
    let entry = map
        .entry(id.to_string())
        .or_insert_with(|| date.to_string());
    if date > entry.as_str() {
        *entry = date.to_string();
    }
}

async fn discover_last_meeting_id(
    client: &ScrapingClient,
    session_id: u32,
    current_id: u32,
    web_request_count: &mut u32,
) -> Result<u32, Box<dyn Error>> {
    let mut last = current_id;
    loop {
        let probe = last + 1;
        let url = format!(
            "https://www.dekamer.be/doc/PCRI/html/{}/ip{:03}x.html",
            session_id, probe
        );
        let resp = client.get(&url).await?;
        *web_request_count += 1;
        if resp.status() == StatusCode::NOT_FOUND {
            break;
        }
        last = probe;
    }
    Ok(last)
}

async fn scrape_meeting(
    client: &ScrapingClient,
    session_id: u32,
    meeting_id: u32,
    web_request_count: &mut u32,
    encountered_dossier_ids: &mut HashMap<String, String>,
) -> Result<MeetingOutput, Box<dyn Error>> {
    let filepath = cache_dir().join(format!(
        "sessions/{}/meetings/plenary/{}-{}.html",
        session_id, session_id, meeting_id
    ));

    if !filepath.exists() {
        let url = format!(
            "https://www.dekamer.be/doc/PCRI/html/{}/ip{:03}x.html",
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

    let typo_map: HashMap<String, String> = [
        ("Steven Coengrachts", "Steven Coenegrachts"),
        ("Ridouhane Chahid", "Ridouane Chahid"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    let questions = extract_questions(&document, session_id, meeting_id, &date, &typo_map).await?;
    let propositions = extract_propositions(
        &document,
        session_id,
        meeting_id,
        &date,
        encountered_dossier_ids,
    )
    .await?;
    let notices = extract_notices(&document, session_id, meeting_id, &date).await?;
    let votes = extract_votes(
        &document,
        session_id,
        meeting_id,
        &date,
        encountered_dossier_ids,
    )
    .await?;

    Ok(MeetingOutput {
        meeting: ScrapedMeeting {
            session_id,
            meeting_id,
            date,
            time_of_day,
            start_time,
            end_time,
        },
        questions,
        propositions,
        notices,
        votes,
    })
}

async fn extract_questions(
    document: &Html,
    session_id: u32,
    meeting_id: u32,
    date: &str,
    typo_map: &HashMap<String, String>,
) -> Result<Vec<ScrapedQuestion>, Box<dyn Error>> {
    let mut questions = Vec::new();
    let mut previous_nl = String::new();
    let mut previous_fr = String::new();
    let mut previous_discussion = String::new();
    let mut question_id: i32 = 0;
    let mut found_questions_section = false;
    let mut processing = false;

    let flush_question = |id: i32,
                          nl: &str,
                          fr: &str,
                          discussion: &str,
                          typo_map: &HashMap<String, String>|
     -> Result<Option<ScrapedQuestion>, Box<dyn Error>> {
        if nl.is_empty() && fr.is_empty() {
            return Ok(None);
        }
        let data_nl = extract_question_data(typo_map, nl, discussion)?;
        let data_fr = extract_question_data(typo_map, fr, discussion)?;
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

    // The keywords that indicate the questions section has started.
    let questions_section_keywords = [
        "mondelinge vragen",
        "vragen",
        "questions orales",
        "question orales",
        "questions",
    ];

    for element in document.select(selector_h1_or_h2_or_p()) {
        let tag = element.value().name();

        if tag == "h1" {
            let text = element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .replace("\n", " ")
                .trim()
                .to_lowercase();

            if questions_section_keywords
                .iter()
                .any(|&keyword| text.contains(keyword))
            {
                found_questions_section = true;
                processing = true;
            } else if found_questions_section {
                if let Some(q) = flush_question(
                    question_id,
                    &previous_nl,
                    &previous_fr,
                    &previous_discussion,
                    typo_map,
                )? {
                    questions.push(q);
                }
                break;
            }
            continue;
        }

        if !processing {
            continue;
        }

        if tag == "h2" {
            let (found_nl, found_fr) = extract_bilingual_spans(&element);

            let is_group_start = found_nl
                .as_deref()
                .map_or(false, |t| t.starts_with("Samengevoegde"))
                || found_fr.as_deref().map_or(false, |t| t.contains("jointes"));
            let is_subquestion = found_nl.as_deref().map_or(false, |t| t.starts_with("-"))
                || found_fr.as_deref().map_or(false, |t| t.starts_with("-"));
            let is_single = found_nl
                .as_deref()
                .map_or(false, |t| t.starts_with("Vraag van"))
                || found_fr
                    .as_deref()
                    .map_or(false, |t| t.starts_with("Question de"));

            // If it's none of the above, it's a non-question h2 — flush and stop.
            if !is_group_start && !is_subquestion && !is_single {
                if let Some(q) = flush_question(
                    question_id,
                    &previous_nl,
                    &previous_fr,
                    &previous_discussion,
                    typo_map,
                )? {
                    questions.push(q);
                }
                break;
            }

            if is_group_start || is_single {
                if !previous_nl.is_empty() && !previous_fr.is_empty() {
                    if let Some(q) = flush_question(
                        question_id,
                        &previous_nl,
                        &previous_fr,
                        &previous_discussion,
                        typo_map,
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

            if text.contains("Het incident is gesloten") || text.contains("L'incident est clos") {
                if let Some(q) = flush_question(
                    question_id,
                    &previous_nl,
                    &previous_fr,
                    &previous_discussion,
                    typo_map,
                )? {
                    questions.push(q);
                    question_id += 1;
                }
                previous_discussion.clear();
                previous_nl.clear();
                previous_fr.clear();
                continue;
            }

            if !text.is_empty() && (!previous_nl.is_empty() || !previous_fr.is_empty()) {
                previous_discussion.push_str(&clean_text(&text));
                previous_discussion.push_str("NEWPARAGRAPH");
            }
        }
    }

    // Fix duplicated internal question IDs caused by incorrect codes in the plenary report.
    // Example: IP005 contains both 56000005P and 56000005P, but the first question
    // ("Het uitstellen van de indiening van het begrotingsplan bij de EU")
    // should actually be 56000006P.
    let mut seen_internal_ids: HashMap<String, usize> = HashMap::new();

    for question in &mut questions {
        let internal_ids = question
            .internal_ids
            .split(',')
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        for internal_id in internal_ids {
            if internal_id.is_empty() {
                continue;
            }

            let count = seen_internal_ids.entry(internal_id.clone()).or_insert(0);

            *count += 1;

            if *count > 1
                && internal_id == "Q56000005P"
                && question
                    .topics_nl
                    .contains("Het uitstellen van de indiening van het begrotingsplan bij de EU")
            {
                question.internal_ids = question.internal_ids.replace("Q56000005P", "Q56000006P");
            }
        }
    }

    Ok(questions)
}

/// Extract the propositions from the plenary meeting.
/// - Propositions can be found under the <h1> tag with the name "(wets)voorstel" or "(wets)voorstellen" as <h2> tags.
/// - The notices always have a <h2> in Dutch, and another <h2> in French. The language indicators (lang="NL" for example) are often wrong
///   so we decide NL/FR based on position: NL comes first, then FR.
/// - Some notices are included within the propositions section. These are detected and stored as notices.
async fn extract_propositions(
    document: &Html,
    session_id: u32,
    meeting_id: u32,
    date: &str,
    encountered_dossier_ids: &mut HashMap<String, String>,
) -> Result<Vec<ScrapedProposition>, Box<dyn Error>> {
    let mut propositions = Vec::new();
    let mut proposition_id: i32 = 0;
    let mut found = false;
    let mut processing = false;
    let proposition_keywords_nl = ["voorstel", "wetsvoorstel"];
    let proposition_keywords_fr = ["proposition"];

    let mut all_titles: Vec<(Option<u32>, String, bool)> = Vec::new();

    for element in document.select(selector_h1_or_h2()) {
        let tag = element.value().name();

        if tag == "h1" {
            let text = element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .replace("\n", " ")
                .trim()
                .to_lowercase();
            let is_dutch_propositions_header = proposition_keywords_nl
                .iter()
                .any(|&keyword| text.contains(keyword));
            if is_dutch_propositions_header {
                found = true;
                processing = true;
            } else if found
                && !proposition_keywords_fr
                    .iter()
                    .any(|&keyword| text.contains(keyword))
            {
                break;
            }
            continue;
        }

        if !processing || tag != "h2" {
            continue;
        }

        // Extract the agenda number from pure-digit spans.
        let number: Option<u32> = element
            .select(selector_span())
            .filter_map(|span| {
                let raw = clean_text(&span.text().collect::<Vec<_>>().join(" "))
                    .trim()
                    .to_string();
                if !raw.is_empty() && raw.chars().all(|c| c.is_ascii_digit()) {
                    raw.parse().ok()
                } else {
                    None
                }
            })
            .next();

        // Collect the actual title text, skipping pure-digit spans.
        let text: String = element
            .select(selector_span())
            .filter_map(|span| {
                let raw = clean_text(&span.text().collect::<Vec<_>>().join(" "))
                    .replace("\"", "'")
                    .trim()
                    .to_string();
                if raw.is_empty() || raw.chars().all(|c| c.is_ascii_digit()) {
                    None
                } else {
                    Some(raw)
                }
            })
            .collect::<Vec<_>>()
            .last() // NOTE: We pick the last one but why? Otherwise I got duplicates.
            .unwrap()
            .trim()
            .to_string();

        if text.is_empty() {
            continue;
        }

        let is_sub = text.starts_with('-');
        let clean = text.trim_start_matches('-').trim().to_string();
        all_titles.push((number, clean, is_sub));
    }

    // Group by agenda number: each new distinct number starts a new group.
    // Within each group the first half is NL titles, second half is FR titles.
    let mut groups: Vec<Vec<String>> = Vec::new();
    let mut current_number: Option<u32> = None;

    for (number, text, _) in &all_titles {
        match number {
            Some(n) if Some(*n) != current_number => {
                current_number = Some(*n);
                groups.push(vec![text.clone()]);
            }
            _ => {
                if let Some(group) = groups.last_mut() {
                    group.push(text.clone());
                }
            }
        }
    }

    for group in groups {
        let half = group.len() / 2;
        let nl_titles = &group[..half];
        let fr_titles = &group[half..];

        for (nl, fr) in nl_titles.iter().zip(fr_titles.iter()) {
            let data_nl = extract_proposition_data(nl.clone())?;
            let data_fr = extract_proposition_data(fr.clone())?;
            let dossier_id_opt = data_nl.dossier_id.clone();

            propositions.push(ScrapedProposition {
                proposition_id,
                session_id,
                meeting_id,
                title_nl: data_nl.topic,
                title_fr: data_fr.topic,
                dossier_id: dossier_id_opt.clone().unwrap_or_default(),
                document_id: data_nl.document_id.unwrap_or_default(),
                date: date.to_string(),
            });
            proposition_id += 1;

            if let Some(ref id) = dossier_id_opt {
                record_dossier(encountered_dossier_ids, id, date);
            }
        }
    }

    Ok(propositions)
}

/// Extract the notices from the plenary meeting.
/// - Notices can be found under the <h1> tag with the name "mededeling" or "mededelingen" as <h2> tags.
/// - The notices always have a <h2> in Dutch, and another <h2> in French. The language indicators (lang="NL" for example) are often wrong
///   so we decide NL/FR based on position: NL comes first, then FR.
/// - Some notices are not put under a separate <h1> header but are included wihin the propositions sector. This is handled in the extract_propositions function.
async fn extract_notices(
    document: &Html,
    session_id: u32,
    meeting_id: u32,
    date: &str,
) -> Result<Vec<ScrapedNotice>, Box<dyn Error>> {
    let mut notices = Vec::new();
    let mut notice_id: i32 = 0;
    let mut found = false;
    let mut processing = false;
    let notice_keywords_nl = ["mededeling", "mededelingen"];
    let notice_keywords_fr = ["communication", "communications"];

    let mut all_titles: Vec<(Option<u32>, String, bool)> = Vec::new();

    for element in document.select(selector_h1_or_h2()) {
        let tag = element.value().name();

        if tag == "h1" {
            let text = element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .replace("\n", " ")
                .trim()
                .to_lowercase();
            let is_dutch_notice_header = notice_keywords_nl
                .iter()
                .any(|&keyword| text.contains(keyword));
            if is_dutch_notice_header {
                found = true;
                processing = true;
            } else if found
                && !notice_keywords_fr
                    .iter()
                    .any(|&keyword| text.contains(keyword))
            {
                break;
            }
            continue;
        }

        if !processing || tag != "h2" {
            continue;
        }

        // Extract the agenda number from pure-digit spans.
        let number: Option<u32> = element
            .select(selector_span())
            .filter_map(|span| {
                let raw = clean_text(&span.text().collect::<Vec<_>>().join(" "))
                    .trim()
                    .to_string();
                if !raw.is_empty() && raw.chars().all(|c| c.is_ascii_digit()) {
                    raw.parse().ok()
                } else {
                    None
                }
            })
            .next();

        // Collect the actual title text, skipping pure-digit spans.
        let text: String = element
            .select(selector_span())
            .filter_map(|span| {
                let raw = clean_text(&span.text().collect::<Vec<_>>().join(" "))
                    .replace("\"", "'")
                    .trim()
                    .to_string();
                if raw.is_empty() || raw.chars().all(|c| c.is_ascii_digit()) {
                    None
                } else {
                    Some(raw)
                }
            })
            .collect::<Vec<_>>()
            .last() // NOTE: We pick the last one but why? Otherwise I got duplicates.
            .unwrap()
            .trim()
            .to_string();

        if text.is_empty() {
            continue;
        }

        let is_sub = text.starts_with('-');
        let clean = text.trim_start_matches('-').trim().to_string();
        all_titles.push((number, clean, is_sub));
    }

    // Group by agenda number: each new distinct number starts a new group.
    // Within each group the first half is NL titles, second half is FR titles.
    let mut groups: Vec<Vec<String>> = Vec::new();
    let mut current_number: Option<u32> = None;

    for (number, text, _) in &all_titles {
        match number {
            Some(n) if Some(*n) != current_number => {
                current_number = Some(*n);
                groups.push(vec![text.clone()]);
            }
            _ => {
                if let Some(group) = groups.last_mut() {
                    group.push(text.clone());
                }
            }
        }
    }

    for group in groups {
        let half = group.len() / 2;
        let nl_titles = &group[..half];
        let fr_titles = &group[half..];

        for (nl, fr) in nl_titles.iter().zip(fr_titles.iter()) {
            notices.push(ScrapedNotice {
                notice_id,
                session_id,
                meeting_id,
                title_nl: nl.clone(),
                title_fr: fr.clone(),
                date: date.to_string(),
            });
            notice_id += 1;
        }
    }

    Ok(notices)
}

struct CachedVote {
    yes: u32,
    no: u32,
    abstain: u32,
    yes_names: String,
    no_names: String,
    abstain_names: String,
}

async fn extract_votes(
    document: &Html,
    session_id: u32,
    meeting_id: u32,
    date: &str,
    encountered_dossier_ids: &mut HashMap<String, String>,
) -> Result<Vec<ScrapedVote>, Box<dyn Error>> {
    let mut votes = Vec::new();
    let mut vote_text_nl = String::new();
    let mut vote_text_fr = String::new();
    let mut previous_vote_title_nl = String::new();
    let mut previous_vote_title_fr = String::new();
    let mut found_votes_section = false;
    let mut vote_id: i32 = 0;
    let mut collecting_grouped_vote = false;
    let mut known_vote_results: HashMap<String, CachedVote> = HashMap::new();

    // The keywords that indicate the votes section has started.
    let votes_section_keywords = ["naamstemmingen", "naamstemming"];

    for element in document.select(selector_h1_or_h2_or_table_or_p()) {
        let tag = element.value().name();

        if tag == "h1" {
            let text = element
                .text()
                .collect::<Vec<_>>()
                .join(" ")
                .replace("\n", " ")
                .trim()
                .to_lowercase();

            if votes_section_keywords
                .iter()
                .any(|&keyword| text.contains(keyword))
            {
                found_votes_section = true;
            }
        }

        if !found_votes_section {
            continue;
        }

        // Encountered a <p> element that is not part of a vote section (i.e. not inside a <table>).
        if tag == "p"
            && !element
                .ancestors()
                .any(|a| a.value().as_element().is_some_and(|e| e.name() == "table"))
        {
            let spans: Vec<_> = element.select(selector_span()).collect();

            // Sometimes, a vote result is a <p> element and it reuses the same results from a previous vote.
            if let Some(span) = spans.last() {
                let text =
                    clean_text(&span.text().collect::<Vec<_>>().join(" ")).replace("\"", "'");

                if let Some(captures) = paragraph_vote_section_regex().captures(&text) {
                    let vote_number = captures[1].to_string();

                    // Get vote results from known results.
                    if let Some(known_vote) = known_vote_results.get(&vote_number) {
                        // Look back at last title and extract data.
                        let data_nl = extract_vote_data(previous_vote_title_nl.clone())?;
                        let data_fr = extract_vote_data(previous_vote_title_fr.clone())?;
                        if let Some(ref id) = data_nl.dossier_id.clone() {
                            record_dossier(encountered_dossier_ids, id, date);
                        }

                        // Push vote.
                        votes.push(ScrapedVote {
                            vote_id,
                            session_id,
                            meeting_id,
                            date: date.to_string(),
                            title_nl: if data_nl.topic.is_empty() {
                                vote_text_nl.clone()
                            } else {
                                data_nl.topic
                            },
                            title_fr: if data_fr.topic.is_empty() {
                                vote_text_fr.clone()
                            } else {
                                data_fr.topic
                            },
                            yes: known_vote.yes,
                            no: known_vote.no,
                            abstain: known_vote.abstain,
                            members_yes: convert_voter_names(&known_vote.yes_names),
                            members_no: convert_voter_names(&known_vote.no_names),
                            members_abstain: convert_voter_names(&known_vote.abstain_names),
                            dossier_id: data_nl.dossier_id.unwrap_or_default(),
                            document_id: data_nl.document_id.unwrap_or_default(),
                            motion_id: data_nl.motion_id.unwrap_or_default(),
                        });
                        vote_id += 1;
                    }
                }
            }
        }

        // Sometimes, a vote is a <p> element and not a <h2> element.
        let is_vote_title_as_paragraph = {
            let spans: Vec<_> = element.select(selector_span()).collect();
            spans.last().map_or(false, |span| {
                let text =
                    clean_text(&span.text().collect::<Vec<_>>().join(" ")).replace("\"", "'");
                paragraph_vote_regex().is_match(&text)
            })
        };
        // Encountered a <p> element that looks like a vote title.
        if tag == "p" && is_vote_title_as_paragraph {
            let spans: Vec<_> = element.select(selector_span()).collect();
            let mut nl_text = String::new();
            let mut fr_text = String::new();
            let mut dossier_ref = String::new();
            // Determine the paragraph's primary language from its class attribute.
            let p_class = element.value().attr("class").unwrap_or("");
            let is_nl_paragraph = p_class.contains("NL");
            let is_fr_paragraph = p_class.contains("FR");
            for span in spans {
                let raw = clean_text(&span.text().collect::<Vec<_>>().join(" ")).replace("\"", "'");
                // Skip dossier reference spans like (297/10) — these have swapped lang attrs in source HTML.
                if raw.trim().starts_with('(') {
                    dossier_ref = raw.trim().to_string();
                    continue;
                }
                match span.value().attr("lang") {
                    Some("FR") => fr_text.push_str(&raw),
                    Some("NL") | Some("NL-BE") => {
                        // Source HTML sometimes mis-tags French content as NL-BE.
                        // Use the paragraph class as the primary signal, falling back
                        // to content-based detection when the class is also ambiguous.
                        if is_fr_paragraph || (is_likely_french(&raw) && !is_likely_dutch(&raw)) {
                            fr_text.push_str(&raw);
                        } else {
                            nl_text.push_str(&raw);
                        }
                    }
                    _ => {
                        // No lang attr: use paragraph class, then content detection, then
                        // fall back to "first span is NL, second is FR".
                        if is_nl_paragraph {
                            nl_text.push_str(&raw);
                        } else if is_fr_paragraph {
                            fr_text.push_str(&raw);
                        } else if is_likely_french(&raw) && !is_likely_dutch(&raw) {
                            fr_text.push_str(&raw);
                        } else if nl_text.is_empty() {
                            nl_text.push_str(&raw);
                        } else {
                            fr_text.push_str(&raw);
                        }
                    }
                }
            }

            // Append the dossier ref to both titles so extract_vote_data can parse it.
            // e.g. "Stemming over amendement nr. 13 ... (297/10)"
            if !dossier_ref.is_empty() {
                if !nl_text.is_empty() {
                    nl_text.push(' ');
                    nl_text.push_str(&dossier_ref);
                }
                if !fr_text.is_empty() {
                    fr_text.push(' ');
                    fr_text.push_str(&dossier_ref);
                }
            }

            // Only update the title for the language this paragraph is actually for.
            // This avoids clobbering the other language's title when the source HTML
            // has swapped lang attrs on reference spans like (297/10).
            if is_nl_paragraph && !nl_text.trim().is_empty() {
                previous_vote_title_nl = nl_text.trim().to_string();
            } else if is_fr_paragraph && !fr_text.trim().is_empty() {
                previous_vote_title_fr = fr_text.trim().to_string();
            } else {
                // Fallback: paragraph class doesn't tell us the language,
                // so update whichever fields we actually extracted text for.
                if !nl_text.trim().is_empty() {
                    previous_vote_title_nl = nl_text.trim().to_string();
                }
                if !fr_text.trim().is_empty() {
                    previous_vote_title_fr = fr_text.trim().to_string();
                }
            }
        }

        // Encountered a regular vote title.
        if tag == "h2" {
            // Collect all spans tagged NL/NL-BE. Source HTML sometimes mis-tags French
            // content as NL-BE (e.g. "Chambre des représentants" with lang="NL-BE"),
            // so we verify with is_likely_french / is_likely_dutch and reroute if needed.
            let nl_spans: Vec<_> = element
                .select(selector_span())
                .filter(|s| matches!(s.value().attr("lang"), Some("NL") | Some("NL-BE")))
                .collect();
            if let Some(span) = nl_spans.last() {
                let raw = clean_text(&span.text().collect::<Vec<_>>().join(" ")).replace("\"", "'");

                // Detect mislabelled span: looks French and not Dutch → reroute to FR bucket.
                let (nl_candidate, fr_candidate) =
                    if is_likely_french(&raw) && !is_likely_dutch(&raw) {
                        (String::new(), raw)
                    } else {
                        (raw, String::new())
                    };

                // Apply to NL bucket.
                if !nl_candidate.is_empty() {
                    vote_text_nl = nl_candidate;
                    if !vote_text_nl.starts_with("-") {
                        previous_vote_title_nl = vote_text_nl.clone();
                        collecting_grouped_vote = true;
                    } else if collecting_grouped_vote {
                        previous_vote_title_nl.push('\n');
                        previous_vote_title_nl.push_str(&vote_text_nl);
                    } else {
                        if !previous_vote_title_nl.is_empty() {
                            collecting_grouped_vote = false;
                        }
                        previous_vote_title_nl.clear();
                    }
                }

                // Apply rerouted FR candidate (mislabelled NL-BE span that is actually French).
                if !fr_candidate.is_empty() {
                    vote_text_fr = fr_candidate;
                    if !vote_text_fr.starts_with("-") {
                        previous_vote_title_fr = vote_text_fr.clone();
                        collecting_grouped_vote = true;
                    } else if collecting_grouped_vote {
                        previous_vote_title_fr.push('\n');
                        previous_vote_title_fr.push_str(&vote_text_fr);
                    } else {
                        if !previous_vote_title_fr.is_empty() {
                            collecting_grouped_vote = false;
                        }
                        previous_vote_title_fr.clear();
                    }
                }
            }

            // Find FR title from spans explicitly tagged lang="FR".
            let fr_spans: Vec<_> = element
                .select(selector_span())
                .filter(|s| s.value().attr("lang") == Some("FR"))
                .collect();
            if let Some(span) = fr_spans.last() {
                vote_text_fr =
                    clean_text(&span.text().collect::<Vec<_>>().join(" ")).replace("\"", "'");
                if !vote_text_fr.is_empty() && !vote_text_fr.starts_with("-") {
                    previous_vote_title_fr = vote_text_fr.clone();
                    collecting_grouped_vote = true;
                } else if collecting_grouped_vote && vote_text_fr.starts_with("-") {
                    previous_vote_title_fr.push('\n');
                    previous_vote_title_fr.push_str(&vote_text_fr);
                } else {
                    if !previous_vote_title_fr.is_empty() {
                        collecting_grouped_vote = false;
                    }
                    previous_vote_title_fr.clear();
                }
            }
        }

        // Encountered a vote table.
        if tag == "table" {
            // Extract vote + names from table.
            let vote = extract_vote_from_table(element);
            if vote.yes == 0 && vote.no == 0 && vote.abstain == 0 {
                continue;
            }

            // Get names from vote appendix in document.
            let (yes_names, no_names, abstain_names) =
                extract_voter_names(document, &vote.vote_number.trim());

            // Store vote in known results so other votes can reuse it.
            known_vote_results.insert(
                vote.vote_number.clone().trim().to_string(),
                CachedVote {
                    yes: vote.yes,
                    no: vote.no,
                    abstain: vote.abstain,
                    yes_names: yes_names.clone(),
                    no_names: no_names.clone(),
                    abstain_names: abstain_names.clone(),
                },
            );

            // Look back at last title and extract data.
            let data_nl = extract_vote_data(previous_vote_title_nl.clone())?;
            let data_fr = extract_vote_data(previous_vote_title_fr.clone())?;
            if let Some(ref id) = data_nl.dossier_id.clone() {
                record_dossier(encountered_dossier_ids, id, date);
            }

            // Push vote.
            votes.push(ScrapedVote {
                vote_id,
                session_id,
                meeting_id,
                date: date.to_string(),
                title_nl: if data_nl.topic.is_empty() {
                    vote_text_nl.clone()
                } else {
                    data_nl.topic
                },
                title_fr: if data_fr.topic.is_empty() {
                    vote_text_fr.clone()
                } else {
                    data_fr.topic
                },
                yes: vote.yes,
                no: vote.no,
                abstain: vote.abstain,
                members_yes: convert_voter_names(&yes_names),
                members_no: convert_voter_names(&no_names),
                members_abstain: convert_voter_names(&abstain_names),
                dossier_id: data_nl.dossier_id.unwrap_or_default(),
                document_id: data_nl.document_id.unwrap_or_default(),
                motion_id: data_nl.motion_id.unwrap_or_default(),
            });
            vote_id += 1;
        }
    }
    Ok(votes)
}

/// Extracts vote data (topic/dossier/document/motion) from the vote title.
fn extract_vote_data(vote_text: String) -> Result<VoteData, Box<dyn Error>> {
    // Try regex 1
    if let Some(captures) = vote_regex_1().captures(&vote_text) {
        return Ok(VoteData {
            topic: captures[1].trim().to_string(),
            dossier_id: Some(captures[2].trim().to_string()),
            document_id: Some(captures[3].trim().to_string()),
            motion_id: None,
        });
    }

    // Try regex 2
    if let Some(captures) = vote_regex_2().captures(&vote_text) {
        return Ok(VoteData {
            topic: captures[1].trim().to_string(),
            dossier_id: None,
            document_id: None,
            motion_id: Some(captures[2].trim().to_string()),
        });
    }

    // Try regex 3
    if let Some(c) = vote_regex_3().captures(&vote_text) {
        return Ok(VoteData {
            topic: c[1].trim().to_string(),
            dossier_id: None,
            document_id: None,
            motion_id: None,
        });
    }
    Err("No regex matched for vote text".into())
}

fn convert_voter_names(raw: &str) -> String {
    raw.split(',')
        .map(|name| convert_name(name.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Extract NL/FR spans, swapping where misclassified by the source HTML.
fn extract_bilingual_spans(element: &ElementRef) -> (Option<String>, Option<String>) {
    let french_indicators = ["questions jointes"];
    let dutch_indicators = ["samengevoegde vragen"];

    let mut nl: Option<String> = None;
    let mut fr: Option<String> = None;

    if let Some(span) = element
        .select(selector_span())
        .filter(|s| matches!(s.value().attr("lang"), Some("NL") | Some("NL-BE")))
        .last()
    {
        let text = clean_text(&span.text().collect::<Vec<_>>().join(" ")).replace("\"", "");
        let text = normalize_leading_dash(text);
        if french_indicators
            .iter()
            .any(|w| text.to_lowercase().contains(w))
        {
            fr = Some(text);
        } else {
            nl = Some(text);
        }
    }
    if let Some(span) = element
        .select(selector_span())
        .filter(|s| matches!(s.value().attr("lang"), Some("FR") | Some("FR-BE")))
        .last()
    {
        let text = clean_text(&span.text().collect::<Vec<_>>().join(" ")).replace("\"", "");
        let text = normalize_leading_dash(text);

        // NOTE: We override FR -> NL if clearly dutch based on some indicator words.
        if text.to_lowercase().contains(" aan ") {
            nl = Some(text);
        } else if dutch_indicators
            .iter()
            .any(|w| text.to_lowercase().contains(w))
        {
            nl = Some(text);
        } else {
            fr = Some(text);
        }
    }
    (nl, fr)
}

/// Check if the text is likely French based on common French patterns and no Dutch core words.
fn is_likely_french(text: &str) -> bool {
    let lower = text.to_lowercase();

    // French contractions and unambiguous function words that don't appear in Dutch
    let french_patterns = [
        "d'",
        "d’",
        "l'",
        "qu'", // contractions
        "à la",
        "à l'", // safer than bare "à"
        " au ",
        " aux ",
        " les ",
        " du ",
        " des ",
        " une ",
        " pour ",
        " dans ",
        " sont ",
        " qui ",
        " sur ",
        "constitutionnelle", // based on detected language mislabeling issue in plenary meeting 19
        "comptes",           // based on detected language mislabeling issue in plenary meeting 19
        "commission",        // based on detected language mislabeling issue in plenary meeting 19
    ];

    if french_patterns.iter().any(|&p| lower.contains(p)) {
        return true;
    }

    // Accented chars + no Dutch core words = probably a French loanword context
    let has_accented = lower.chars().any(|c| "éèêëàâîïôùûü".contains(c));
    has_accented && !is_likely_dutch(text)
}

/// Check if the text is likely Dutch based on common Dutch core words and no French patterns.
fn is_likely_dutch(text: &str) -> bool {
    let lower = text.to_lowercase();
    let dutch_core_words = [
        "van", "het", "een", "met", "tot", "aan", "bij", "naar", "over", "uit", "zijn", "voor",
        "ons",
    ];
    lower
        .split_whitespace()
        .map(|w| w.trim_matches(|c: char| !c.is_alphabetic()))
        .any(|w| dutch_core_words.contains(&w))
}

fn extract_proposition_data(proposition_text: String) -> Result<PropositionData, Box<dyn Error>> {
    if let Some(captures) = proposition_regex().captures(&proposition_text) {
        let document_id = captures[3]
            .trim()
            .strip_prefix("1-")
            .unwrap_or(captures[3].trim())
            .to_string();
        return Ok(PropositionData {
            topic: captures[1].trim().to_string(),
            dossier_id: Some(captures[2].trim().to_string()),
            document_id: Some(document_id),
        });
    }
    if let Some(captures) = proposition_topic_regex().captures(&proposition_text) {
        return Ok(PropositionData {
            topic: captures[1].trim().to_string(),
            dossier_id: None,
            document_id: None,
        });
    }
    Err("No regex matched for proposition text".into())
}

fn extract_question_data(
    typo_map: &HashMap<String, String>,
    question_text: &str,
    discussion_text: &str,
) -> Result<QuestionData, Box<dyn Error>> {
    let mut questioners = Vec::new();
    let mut topics = Vec::new();
    let mut questionees = Vec::new();
    let mut internal_ids = Vec::new();

    for capture in question_regex().captures_iter(question_text) {
        let questioner_raw = capture[1].trim().replace("- ", "").trim().to_string();
        let questioner = typo_map
            .get(&questioner_raw)
            .cloned()
            .unwrap_or(questioner_raw);
        let questionee = capture[2].trim().to_string();
        let topic = capture
            .get(3)
            .or_else(|| capture.get(4))
            .or_else(|| capture.get(5))
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_default();

        // Create the internal question ID format (Q56001734P).
        let internal_id = capture
            .get(4)
            .map(|m| format!("Q{}", m.as_str().trim()))
            .unwrap_or_default();

        questioners.push(questioner.clone());
        if !questionees.contains(&questionee) {
            questionees.push(questionee);
        }
        internal_ids.push(internal_id);
        topics.push(topic);
    }

    // The actual respondents may differ from the questionees so we need to extract the actual respondents from the discussion text.
    let speakers = extract_speakers_from_discussion(discussion_text, typo_map);
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
fn extract_speakers_from_discussion(
    discussion_text: &str,
    typo_map: &HashMap<String, String>,
) -> Vec<String> {
    let mut seen = Vec::new();
    for paragraph in discussion_text.split("NEWPARAGRAPH") {
        let Some(raw) = discussion_speaker_regex()
            .captures(paragraph.trim_start())
            .map(|cap| cap[1].to_string())
        else {
            continue;
        };
        let cleaned = normalize_speaker_name(&raw);
        let resolved = typo_map.get(&cleaned).cloned().unwrap_or(cleaned);
        if !seen.contains(&resolved) && resolved.to_lowercase() != "de voorzitter" {
            seen.push(resolved);
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
    // NOTE: See IC311 question 1: some speaker paragraphs have multiple leading sequence numbers such as '01.02 01.03' instead of just '01.01'.
    // We need to make sure to ignore all of them.
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^(?:\d{2}\.\d{2}\s+)*\d{2}\.\d{2}\s+([^,:\n]+?)\s*[,:]").unwrap()
    })
}

/// Some reports (for example meeting IC156) mistakenly omit the space after a leading "-" for sub-questions,
/// e.g. "-Kjell Vander Elst aan ..." instead of "- Kjell Vander Elst aan ...".
fn normalize_leading_dash(text: String) -> String {
    if let Some(rest) = text.strip_prefix('-') {
        if !rest.starts_with(' ') {
            return format!("- {}", rest);
        }
    }
    text
}

fn get_discussion_json(input: &str) -> String {
    let cleaned_input = clean_text(input);

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

/// Extracts a vote record from a table element.
fn extract_vote_from_table(table: ElementRef) -> VoteRecord {
    let mut vote_number = String::new();
    let mut processing = false;
    let mut yes = 0u32;
    let mut no = 0u32;
    let mut abstain = 0u32;

    // Index of the column holding the actual total to use.
    // Defaults to 1 (old format: label, value, french-label).
    // Gets overridden to the "Tot." column index if a header row is found
    // (new format: label, N, Tot., F, french-label).
    let mut value_col: usize = 1;

    for (i, row) in table.select(selector_tr()).enumerate() {
        let cells: Vec<_> = row.select(selector_td()).collect();

        if i == 0 {
            let text = row.text().collect::<Vec<_>>().join(" ").trim().to_string();
            if text.contains("Stemming/vote") {
                vote_number = text
                    .replace("(", "")
                    .replace(")", "")
                    .replace("Stemming/vote", "");
                processing = true;
            }
            continue;
        }

        if !processing || cells.len() < 2 {
            continue;
        }

        let label = cells[0]
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string();

        // Detect the new-format header row (empty label, cells containing
        // "N" / "Tot." / "F") and record which column is "Tot.".
        if label.is_empty() {
            if let Some(idx) = cells
                .iter()
                .position(|c| c.text().collect::<Vec<_>>().join(" ").trim() == "Tot.")
            {
                value_col = idx;
            }
            continue;
        }

        if cells.len() <= value_col {
            continue;
        }

        let value_str = cells[value_col].text().collect::<Vec<_>>().join(" ");
        if let Ok(v) = value_str.trim().parse::<u32>() {
            match label.as_str() {
                "Ja" => yes = v,
                "Nee" => no = v,
                "Onthoudingen" => abstain = v,
                _ => {}
            }
        }
    }

    VoteRecord {
        vote_number,
        yes,
        no,
        abstain,
    }
}

fn extract_voter_names(document: &Html, vote_index: &str) -> (String, String, String) {
    let mut tables = Vec::new();
    for span in document.select(selector_span()) {
        let text = span
            .text()
            .flat_map(|t| t.split_whitespace())
            .collect::<Vec<_>>()
            .join(" ");
        if text.contains(&format!("Vote nominatif - Naamstemming: {}", vote_index))
            || text.contains(&format!("Naamstemming - Vote nominatif: {}", vote_index))
        {
            let mut node = span.parent();
            while let Some(n) = node {
                if let Some(el) = ElementRef::wrap(n) {
                    if el.value().name() == "table" {
                        tables.push(el);
                        if tables.len() == 3 {
                            break;
                        }
                    }
                }
                node = n.next_sibling();
            }
            break;
        }
    }

    let mut yes_voters = String::new();
    let mut no_voters = String::new();
    let mut abstain_voters = String::new();
    let vote_types = [&mut yes_voters, &mut no_voters, &mut abstain_voters];

    for (i, table) in tables.iter().enumerate() {
        let mut tds = table.select(selector_td());
        tds.next();
        let count: usize = tds
            .next()
            .map(|td| {
                td.text()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .trim()
                    .parse()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        if count == 0 {
            *vote_types[i] = String::new();
            continue;
        }

        // In extract_voter_names, replace the inner while loop body for each vote_type bucket:
        let mut node = table.next_sibling();
        let mut collected_names = Vec::new();

        while let Some(n) = node {
            if let Some(el) = ElementRef::wrap(n) {
                if el.value().name() == "table" {
                    break;
                }
                if el.value().name() == "p" {
                    if let Some(span_node) = el.first_child() {
                        if let Some(span_el) = ElementRef::wrap(span_node) {
                            if span_el.value().name() == "span" {
                                let raw = span_el
                                    .text()
                                    .collect::<Vec<_>>()
                                    .join(" ")
                                    .trim()
                                    .to_string();
                                let looks_like_names = raw
                                    .chars()
                                    .next()
                                    .map(|c| c.is_alphabetic())
                                    .unwrap_or(false)
                                    && !raw.contains("Vote nominatif")
                                    && !raw.contains("Naamstemming")
                                    && raw.chars().any(|c| c.is_alphabetic());
                                if looks_like_names {
                                    // Strip trailing comma before joining paragraphs
                                    let trimmed = raw.trim_end_matches(',').trim().to_string();
                                    collected_names.push(trimmed);
                                }
                            }
                        }
                    }
                }
            }
            node = n.next_sibling();
        }

        if !collected_names.is_empty() {
            *vote_types[i] = collected_names
                .join(", ")
                .replace(", ", ",")
                .replace(",\n", ",")
                .replace('\n', " ");
        }
    }
    (yes_voters, no_voters, abstain_voters)
}

fn convert_name(name: &str) -> String {
    let parts: Vec<&str> = name.split_whitespace().collect();
    match parts.as_slice() {
        [] => String::new(),
        [only] => only.to_string(),
        [rest @ .., first] => format!("{} {}", first, rest.join(" ")),
    }
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
    ["wordt geopend", "wordt hervat"]
        .iter()
        .find_map(|phrase| extract_time_from_document(document, phrase).ok())
        .ok_or_else(|| "Could not extract start time from the document".into())
}

fn extract_end_time_from_document(document: &Html) -> Result<String, Box<dyn Error>> {
    [
        "De vergadering wordt gesloten",
        "De vergadering wordt geschorst",
    ]
    .iter()
    .find_map(|phrase| extract_time_from_document(document, phrase).ok())
    .ok_or_else(|| "Could not extract end time from the document".into())
}

fn extract_time_from_document(document: &Html, keyword: &str) -> Result<String, Box<dyn Error>> {
    let keyword_lower = keyword.to_lowercase();

    document
        .select(selector_span())
        .filter_map(|span| {
            let text = span.text().collect::<Vec<_>>().join(" ");
            let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
            if !text.to_lowercase().contains(&keyword_lower) {
                return None;
            }
            time_regex()
                .captures(&text)
                .map(|caps| format!("{}h{}", &caps[1], &caps[2]))
        })
        .last()
        .ok_or_else(|| "Could not extract time from the document".into())
}
