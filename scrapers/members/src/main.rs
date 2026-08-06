use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use crawl::client::ScrapingClient;
use crawl::paths::{cache_dir, data_dir};
use crawl::utils::{dutch_language_to_language_code, dutch_month_to_number};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use regex::{Captures, Regex};
use scraper::{ElementRef, Html, Selector};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{File, read_to_string};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::Path;
use std::sync::{Arc, LazyLock, OnceLock};
use tokio::fs;

/// SESSIONS
const CURRENT_SESSION: i32 = 56;
const SESSIONS: &[i32] = &[56, 55, 54, 53, 52, 51, 50, 49, 48];

/// FRACTION REGEXES
static REGEX_FRACTION_DEFAULT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"Fractie:\s*([^|(]+)").unwrap());
static REGEX_FRACTION_LID_DUTCH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?:Lid[-\s]+van|Voorzitter\s+van|Voorzitster\s+van)\s+(?:de\s+)?((?:cdH|sp\.a|[A-ZÀ-Ý][A-Za-zÀ-ÿ&]*!?)(?:[-\s]+(?:[A-Za-zÀ-ÿ&]+!?))*)[-\s]+fractie\b",
    )
    .unwrap()
});
static REGEX_FRACTION_LID_FRENCH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?:Membre\s+du|Président\s+du|Présidente\s+du)\s+groupe\s+((?:cdH|sp\.a|[A-ZÀ-Ý][A-Za-zÀ-ÿ&]*!?)(?:[-\s]+(?:[A-Za-zÀ-ÿ&]+!?))*)",
    )
    .unwrap()
});
static REGEX_FRACTION_PARTY_BRACKET: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(gewezen\s+)?(?:volksvertegenwoordiger|volkvertegenwoordiger)\b(?:\s+[^(.,0-9]+?)?\s*\(([^)]+)\)")
        .unwrap()
});
static REGEX_FRACTION_PARTY_NAME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(Gewezen\s+)?\bvolksvertegenwoordiger\b\s+van\s+(?:het|de)\s+([^.,]+?)(?:\s+(?:voor|sedert|van|tot)\b|[.,]|$)"
    )
    .unwrap()
});
static REGEX_FRACTION_PARTY_PLAIN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bvolksvertegenwoordiger\b\s+([^\s().,]+)").unwrap());

static REGEX_BIRTH_DATE_SLASH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"Geboren(?:\s+te\s+(?P<place>[^|]+?))?\s+op\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})")
        .unwrap()
});

static REGEX_BIRTH_DATE_WORDS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"Geboren(?:\s+te\s+(?P<place>[^|.]+?))?\s+op\s+(?P<date>\d{1,2}\s+[A-Za-z]+\s+\d{4})",
    )
    .unwrap()
});

/// DATE REGEXES
static REGEX_DATE_SLASH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(\d{1,2})/(\d{1,2})/(\d{4})$").unwrap());

/// Matches "van <date> tot <date>", where each <date> is either
/// "9/3/2017" or "9 maart 2017" style.
static REGEX_VAN_TOT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
            r"\bvan\s+(\d{1,2}(?:/\d{1,2}/\d{4}|\s+[A-Za-z]+\s+\d{4}))\s+tot\s+(\d{1,2}(?:/\d{1,2}/\d{4}|\s+[A-Za-z]+\s+\d{4}))",
        )
        .unwrap()
});

static REGEX_START_DATE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\d{1,2})/(\d{1,2})/(\d{4})").unwrap());

static REGEX_START_DATE_WORDS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\d{1,2})\s+([A-Za-zÀ-ÿ]+)\s+(\d{4})").unwrap());

static REGEX_END_DATE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"tot\s+(\d{1,2}/\d{1,2}/\d{4})").unwrap());

static REGEX_CONSTITUENCY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
            r"(?:voor de kieskring|voor het arrondissement)\s+([A-Za-z0-9][A-Za-z0-9\s-]*?)(?:\s+(sedert|sinds|van|tot)\b|[.,|]|$)"
        ).unwrap()
});

static REGEX_MEMBER_KEY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"cvview54\.cfm\?key=([O0]\d+)").unwrap());

/// "CV: Zittingsperiode 55 (20.06.2019 - 27.05.2024)" — used as a fallback
/// source for start/end dates when the representative paragraph doesn't
/// contain "sedert"/"sinds"/"tot" (common on older/compact CV pages).
/// The end date is optional since ongoing periods are shown as
/// "(09.06.2024 - ....)".
static REGEX_ZITTINGSPERIODE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
            r"Zittingsperiode\s+(\d+)\s*\((\d{2})\.(\d{2})\.(\d{4})\s*-\s*(?:(\d{2})\.(\d{2})\.(\d{4}))?[^)]*\)",
        )
        .unwrap()
});

static REGEX_REPLACEE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:ter vervanging van|in opvolging van|opvolging van)\s+([^)|.]+)").unwrap()
});

/// SELECTORS
static SELECTOR_A: OnceLock<Selector> = OnceLock::new();
static SELECTOR_P: OnceLock<Selector> = OnceLock::new();
static SELECTOR_H4: OnceLock<Selector> = OnceLock::new();
static SELECTOR_P_I: OnceLock<Selector> = OnceLock::new();
static SELECTOR_TR: OnceLock<Selector> = OnceLock::new();
static SELECTOR_NAME: OnceLock<Selector> = OnceLock::new();
static SELECTOR_FRACTION: OnceLock<Selector> = OnceLock::new();
static SELECTOR_DETAIL_PAGE_LINK: OnceLock<Selector> = OnceLock::new();
static SELECTOR_EMAIL: OnceLock<Selector> = OnceLock::new();

fn selector_a() -> &'static Selector {
    SELECTOR_A.get_or_init(|| Selector::parse("a").unwrap())
}

fn selector_p() -> &'static Selector {
    SELECTOR_P.get_or_init(|| Selector::parse("p").unwrap())
}

fn selector_h4() -> &'static Selector {
    SELECTOR_H4.get_or_init(|| Selector::parse("h4").unwrap())
}

fn selector_p_i() -> &'static Selector {
    SELECTOR_P_I.get_or_init(|| Selector::parse("p i").unwrap())
}

fn selector_tr() -> &'static Selector {
    SELECTOR_TR.get_or_init(|| Selector::parse("tr").unwrap())
}

fn selector_name() -> &'static Selector {
    SELECTOR_NAME.get_or_init(|| Selector::parse("tr a[href*='cvview54.cfm'] > b").unwrap())
}

fn selector_fraction() -> &'static Selector {
    SELECTOR_FRACTION.get_or_init(|| Selector::parse("tr a[href*='cvlist54.cfm']").unwrap())
}

fn selector_detail_page_link() -> &'static Selector {
    SELECTOR_DETAIL_PAGE_LINK.get_or_init(|| Selector::parse("tr a[href*='cvview54.cfm']").unwrap())
}

fn selector_email() -> &'static Selector {
    SELECTOR_EMAIL.get_or_init(|| Selector::parse("tr a[href*='mailto:']").unwrap())
}

static FRACTION_DISPLAY_NAMES: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        HashMap::from([
            ("cd&v", "CD&V"),
            ("anders.", "Anders."),
            ("vb", "VB"),
            ("pvda-ptb", "PVDA-PTB"),
            ("ps", "PS"),
            ("ecolo-groen!", "Ecolo-Groen!"),
            ("mr", "MR"),
            ("n-va", "N-VA"),
            ("vooruit", "Vooruit"),
            ("onafh", "ONAFH"),
            ("les engagés", "Les Engagés"),
            ("défi", "DéFI"),
        ])
    });

/// A scraped member.
#[derive(Debug)]
struct ScrapedMember {
    member_id: String,
    session_id: i32,
    first_name: String,
    last_name: String,
    date_of_birth: String,
    place_of_birth: String,
    language: String,
    constituency: String,
    fraction: String,
    function: String,
    email: String,
    active: bool,
    start: Option<String>,
    end: Option<String>,
    replacee: Option<String>,
}

#[derive(Hash)]
struct MemberKey {
    session_id: i32,
    first_name: String,
    last_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let client = ScrapingClient::new();

    // Build the (session_id, is_active) -> index-page-url list for every
    // session in SESSIONS. Only CURRENT_SESSION gets the extra "active/today"
    // page (page 56 there is not the same as 'today', so we add both) - this
    // is needed to determine the active/inactive state of the members.
    let sessions: Vec<((i32, bool), String)> = {
        let mut v = Vec::new();
        for &session_id in SESSIONS {
            if session_id == CURRENT_SESSION {
                v.push((
                            (session_id, true),
                            "https://www.dekamer.be/kvvcr/showpage.cfm?section=/depute&language=nl&cfm=/site/wwwcfm/depute/cvlist54.cfm"
                                .to_string(),
                        ));
            }
            v.push((
                        (session_id, false),
                        format!(
                            "https://www.dekamer.be/kvvcr/showpage.cfm?section=/depute&language=nl&cfm=cvlist54.cfm?legis={:02}&today=n",
                            session_id
                        ),
                    ));
        }
        v
    };

    let members_path = data_dir().join("members.parquet");
    fs::create_dir_all(members_path.parent().unwrap()).await?;

    let mut seen: HashSet<(i32, u64)> = HashSet::new();
    let mut all_members: Vec<ScrapedMember> = Vec::new();
    let mut web_request_count = 0u32;

    for ((session_id, active), url) in &sessions {
        let index_path = cache_dir().join(format!(
            "sessions/{}/members/{}.html",
            session_id,
            if *active { "active" } else { "all" }
        ));

        if !index_path.exists() {
            let content = client.get(url).await?.text().await?;
            web_request_count += 1;
            fs::create_dir_all(index_path.parent().unwrap()).await?;
            fs::write(&index_path, &content).await?;
        }

        let content = read_to_string(&index_path)?;
        let document = Html::parse_document(&content);

        let mut members = extract_members(
            &client,
            document,
            (*session_id, *active),
            &mut seen,
            &mut web_request_count,
        )
        .await?;

        all_members.append(&mut members);
    }

    write_parquet(&members_path, &all_members)?;

    println!(
        "[members] scraped {} members using {} web requests",
        all_members.len(),
        web_request_count
    );
    Ok(())
}

fn write_parquet(path: &Path, members: &[ScrapedMember]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("member_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("date_of_birth", DataType::Utf8, false),
        Field::new("place_of_birth", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, false),
        Field::new("constituency", DataType::Utf8, false),
        Field::new("fraction", DataType::Utf8, false),
        Field::new("function", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("active", DataType::Utf8, false),
        Field::new("start", DataType::Utf8, true),
        Field::new("end", DataType::Utf8, true),
        Field::new("replacee", DataType::Utf8, true),
    ]));

    macro_rules! col {
        ($f:expr) => {
            Arc::new(StringArray::from(
                members.iter().map($f).collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }

    macro_rules! col_opt {
        ($f:expr) => {
            Arc::new(StringArray::from(
                members.iter().map($f).collect::<Vec<Option<String>>>(),
            )) as ArrayRef
        };
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            col!(|m| m.member_id.clone()),
            col!(|m| m.session_id.to_string()),
            col!(|m| m.first_name.clone()),
            col!(|m| m.last_name.clone()),
            col!(|m| m.date_of_birth.clone()),
            col!(|m| m.place_of_birth.clone()),
            col!(|m| m.language.clone()),
            col!(|m| m.constituency.clone()),
            col!(|m| m.fraction.clone()),
            col!(|m| m.function.clone()),
            col!(|m| m.email.clone()),
            col!(|m| m.active.to_string()),
            col_opt!(|m| m.start.clone()),
            col_opt!(|m| m.end.clone()),
            col_opt!(|m| m.replacee.clone()),
        ],
    )?;

    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

async fn extract_members(
    client: &ScrapingClient,
    index_document: Html,
    session_info: (i32, bool),
    seen: &mut HashSet<(i32, u64)>,
    web_request_count: &mut u32,
) -> Result<Vec<ScrapedMember>, Box<dyn Error>> {
    let (session_id, active) = session_info;

    let total_rows = index_document.select(&selector_tr()).count();
    let mut members = Vec::new();

    let pb = ProgressBar::new(total_rows as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[members] [{elapsed_precise}] {spinner:.blue} {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {msg}",
        )?
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );

    pb.set_message(format!(
        "reqs={} session {} ({})",
        web_request_count,
        session_id,
        if active { "active" } else { "all" }
    ));

    // Extract data from rows of members
    for row in index_document.select(&selector_tr()) {
        // Skip rows that don't have a member link.
        let raw_name = match extract_from_row(&row, &selector_name(), None) {
            Some(n) => n,
            None => continue,
        };
        let raw_name = fix_raw_name(raw_name);
        let name = reorder_name(raw_name);

        pb.set_message(format!(
            "reqs={} session={} member={}",
            web_request_count, session_id, name
        ));
        let (first_name, last_name) = split_name(&name);
        let (first_name, last_name) = fix_name(first_name, last_name);

        // Dedup check before any I/O.
        let member_id = calculate_hash(&MemberKey {
            session_id,
            first_name: first_name.clone(),
            last_name: last_name.clone(),
        });
        if !seen.insert((session_id, member_id)) {
            continue;
        }

        let member_detail_page_link =
            extract_from_row(&row, &selector_detail_page_link(), Some("href"))
                .unwrap_or_else(|| "unknown".to_string());
        let fraction = extract_from_row(&row, &selector_fraction(), None)
            .unwrap_or_default()
            .to_lowercase();
        let email = extract_from_row(&row, &selector_email(), None)
            .map(|e| e.chars().rev().collect::<String>())
            .unwrap_or_default();

        // Load / cache the member detail page.
        let detail_path = cache_dir().join(format!(
            "sessions/{}/members/{}/details.html",
            session_id, name
        ));
        if !detail_path.exists() {
            let url = format!("https://www.dekamer.be/kvvcr/{}", member_detail_page_link);
            let content = client.get(&url).await?.text().await?;
            *web_request_count += 1;
            fs::create_dir_all(detail_path.parent().unwrap()).await?;
            fs::write(&detail_path, &content).await?;
        }

        // Read detail page
        let content = read_to_string(&detail_path)?;
        let detail = Html::parse_document(&content);
        let member_id = extract_member_key(&detail).unwrap_or_default();

        // Extract the representative paragraph
        let paragraph: Option<String> = detail
            .select(&selector_p())
            .find(|el| {
                el.text().any(|t| {
                    (t.contains("olksvertegenwoordiger")
                        || t.contains("olkvertegenwoordiger")
                        || t.contains("éputée"))
                        && (t.contains("arrondissement")
                            || t.contains("kieskring")
                            || t.contains("circonscription"))
                })
            })
            .map(|el| el.text().collect());

        let language = extract_sibling_text(&detail, "Taal")
            .and_then(|l| dutch_language_to_language_code(l.as_str()).map(str::to_string))
            .map(|c| c.to_ascii_lowercase())
            .unwrap_or_default();

        let fraction = if fraction.is_empty() {
            extract_fraction(paragraph.as_deref())
        } else {
            fraction
        };

        let fraction = capitalize_fraction(&fraction);

        let (van_tot_start, van_tot_end) = extract_van_tot_period(paragraph.as_deref());
        let (period_start, period_end) = extract_period_dates(&detail, session_id);

        // Discard a "van ... tot ..." match that doesn't overlap this session's
        // own period — it likely describes an earlier, unrelated mandate stint.
        let (van_tot_start, van_tot_end) = if is_period_relevant(
            van_tot_start.as_deref(),
            van_tot_end.as_deref(),
            period_start.as_deref(),
            period_end.as_deref(),
        ) {
            (van_tot_start, van_tot_end)
        } else {
            (None, None)
        };

        let start = extract_start_date(&detail)
            .or_else(|| van_tot_start.clone())
            .or_else(|| period_start.clone())
            .or_else(|| {
                if session_id == CURRENT_SESSION && active {
                    Some("2024-06-09".to_string())
                } else {
                    None
                }
            });
        let start = clamp_start_to_period(start, period_start.as_deref());

        let end = extract_end_date(paragraph.as_deref())
            .or_else(|| van_tot_end.clone())
            .or_else(|| period_end.clone());

        let place_of_birth = extract_birth_place(&detail, &first_name, &last_name);
        let replacee = extract_replacee(paragraph.as_deref());

        members.push(ScrapedMember {
            member_id,
            session_id,
            first_name,
            last_name,
            date_of_birth: extract_birth_date(&detail),
            place_of_birth,
            language,
            constituency: extract_constituency(paragraph.as_deref()),
            fraction,
            function: extract_function(paragraph.as_deref()),
            email,
            active,
            start,
            end,
            replacee,
        });

        pb.inc(1);
    }

    pb.finish_with_message(format!(
        "session {} ({}) done",
        session_id,
        if active { "active" } else { "all" }
    ));

    Ok(members)
}

/// Corrects known raw-name issues on the index page, e.g. missing hyphens
/// between compound first names. Applied before `reorder_name`.
fn fix_raw_name(raw: String) -> String {
    match raw.as_str() {
        // ONE-OFF FIX: "Dedecker Jean Marie" -> "Dedecker Jean-Marie"
        "Dedecker Jean Marie" => "Dedecker Jean-Marie".to_string(),
        _ => raw,
    }
}

fn capitalize_fraction(fraction: &str) -> String {
    FRACTION_DISPLAY_NAMES
        .get(fraction)
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            fraction
                .split_whitespace()
                .map(|word| {
                    let mut chars = word.chars();
                    match chars.next() {
                        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                        None => String::new(),
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
}

/// Extract the member's fraction from the given paragraph.
fn extract_fraction(paragraph: Option<&str>) -> String {
    let full_text = paragraph.unwrap_or_default();

    // Check if the member is not a member of an official fraction.
    if full_text.contains("Geen lid van een erkende fractie")
        || full_text.contains("Behoort niet tot een erkende politieke fractie")
        || full_text.contains("Volksvertegenwoordiger (- Onafhankelijke -)")
    {
        return "onafh".to_string();
    }

    // ACCEPTED PATTERNS
    // "Fractie: Ecolo-Groen"
    if let Some(cap) = REGEX_FRACTION_DEFAULT.captures(full_text) {
        if let Some(m) = cap.get(1) {
            return normalize_fraction(m.as_str().trim().to_lowercase());
        }
    }

    // ACCEPTED PATTERNS
    // "Lid van de cdH-fractie." -> cdH
    // "Voorzitter van de MR-fractie van de Kamer." -> MR
    // "Voorzitster van de Open Vld-fractie." -> Open Vld
    // "Lid van de Ecolo-Groen fractie." -> Ecolo-Groen
    // "Lid van de Ecolo-Groen!-fractie." -> Ecolo-Groen!
    // "Lid van sp.a-fractie." -> sp.a
    // "Lid van de Vlaams Belang-fractie." -> Vlaams Belang
    // "Lid van de PVDA-PTB-fractie." -> PVDA-PTB
    // "Lid-van de PVDA-PTB-fractie." -> PVDA-PTB
    // "Lid van de CD&V-fractie." -> CD&V
    // REJECTED PATTERNS
    // "Voorzitter van de liberale fractie van de Vergadering van de Westeuropese Unie."
    // "Voorzitter van de Volksunie. Gewezen voorzitter van de Volksunie-fractie van de Kamer."
    if let Some(cap) = REGEX_FRACTION_LID_DUTCH
        .captures_iter(full_text)
        .find(|cap| cap.get(1).is_some())
    {
        if let Some(m) = cap.get(1) {
            return normalize_fraction(m.as_str().trim().to_lowercase());
        }
    }

    // ONE-OFF FIX: Juliette Boulet sessions 52 has paragraph in French
    // ACCEPTED PATTERNS
    // "Membre du groupe Ecolo-Groen!." -> Ecolo-Groen!
    if let Some(cap) = REGEX_FRACTION_LID_FRENCH
        .captures_iter(full_text)
        .find(|cap| cap.get(1).is_some())
    {
        if let Some(m) = cap.get(1) {
            return normalize_fraction(m.as_str().trim().to_lowercase());
        }
    }

    // ACCEPTED PATTERNS
    // "Volksvertegenwoordiger (Vooruit) voor"
    // "Volksvertegenwoordiger van het Front National (FN) voor"
    // "Volksvertegenwoordiger Spirit (sp.a-spirit) voor"
    // "volkvertegenwoordiger (sp.a-spirit) voor"
    // REJECTED PATTERNS
    // "Volksvertegenwoordiger FDF voor de kieskring Brussel-Halle-Vilvoorde van 10 juni 2007 tot 16 juli 2009 en sedert 13 juni 2010 (vervangen voor de duur van zijn ambt van staatssecretaris van 20 maart 2008 tot 16 juli 2009)."
    if let Some(cap) = REGEX_FRACTION_PARTY_BRACKET.captures(full_text) {
        if cap.get(1).is_none() {
            if let Some(m) = cap.get(2) {
                return normalize_fraction(m.as_str().trim().to_lowercase());
            }
        }
    }

    // ACCEPTED PATTERNS
    // "Volksvertegenwoordiger van het Vlaams Belang voor"
    if let Some(cap) = REGEX_FRACTION_PARTY_NAME.captures(full_text) {
        if cap.get(1).is_none() {
            if let Some(m) = cap.get(2) {
                return normalize_fraction(m.as_str().trim().to_lowercase());
            }
        }
    }

    // ACCEPTED PATTERNS
    // "Volksvertegenwoordiger FDF voor"
    if let Some(cap) = REGEX_FRACTION_PARTY_PLAIN.captures(full_text) {
        if let Some(m) = cap.get(1) {
            return normalize_fraction(m.as_str().trim().to_lowercase());
        }
    }

    "".to_string()
}

/// Cleans up known data-quality quirks in specific source texts
/// (e.g. stray whitespace before punctuation).
fn normalize_fraction(fraction: String) -> String {
    match fraction.as_str() {
        "ecolo-groen !" => "ecolo-groen!".to_string(),
        "vlaams-belang" => "vlaams belang".to_string(),
        "prlfdf" => "prl-fdf".to_string(),
        "\"prlfdf\"" => "prl-fdf".to_string(),
        "prl fdf mcc" => "prl-fdf-mcc".to_string(),
        "vl.blok" => "vl. blok".to_string(),
        "vl?blok" => "vl. blok".to_string(),
        "volksunie" => "vu".to_string(),
        _ => fraction,
    }
}

/// Extracts the name of the member being replaced from
/// "(opvolging van Theo Francken)" in the representative paragraph, if present.
fn extract_replacee(paragraph: Option<&str>) -> Option<String> {
    let text = paragraph?;
    let caps = REGEX_REPLACEE.captures(text)?;
    let raw = caps.get(1)?.as_str().trim();
    let name = strip_honorific(raw);
    if name.is_empty() { None } else { Some(name) }
}

/// Strips common Dutch honorific prefixes ("de heer", "mevrouw", ...) that
/// often precede the replaced member's name, e.g. "de heer Jan Jambon" -> "Jan Jambon".
fn strip_honorific(raw: &str) -> String {
    const PREFIXES: &[&str] = &["de heer ", "mevrouw ", "mevr. ", "mevr ", "de dame "];
    let lower = raw.to_lowercase();
    for prefix in PREFIXES {
        if lower.starts_with(prefix) {
            return raw[prefix.len()..].trim().to_string();
        }
    }
    raw.trim().to_string()
}

fn extract_birth_place(document: &Html, first_name: &str, last_name: &str) -> String {
    let result = document
        .select(&selector_p())
        .find(|el| el.text().any(|t| t.contains("Geboren te")))
        .and_then(|el| {
            let text = el.text().collect::<String>();
            let after = text.split("Geboren te").nth(1)?.to_string();
            let place = if after.contains("op") {
                after.split("op").next()?.trim().to_string()
            } else {
                after
                    .split(|c| c == '.' || c == '|' || c == '\n')
                    .next()?
                    .trim()
                    .to_string()
            };

            // Normalize: drop any parenthetical qualifier, e.g. "Borgerhout (Antwerpen)" -> "Borgerhout"
            let normalized = place.split('(').next().unwrap_or(&place).trim().to_string();
            Some(normalized)
        })
        .unwrap_or_default();

    // ONE-OFF FIX: Nabil Boukili session 55
    if result.eq_ignore_ascii_case("dujda") {
        return "Oujda".to_string();
    }

    // ONE-OFF FIX: Florence Reuter session 55
    if result.eq_ignore_ascii_case("malmédy") {
        return "Malmedy".to_string();
    }

    // ONE-OFF FIX: Robert Denis session 51
    if result.eq_ignore_ascii_case("butgenbach") {
        return "Bütgenbach".to_string();
    }

    // ONE-OFF FIX: Zakia Khattabi session 55
    if result.eq("Sint-Joost-Ten-Noode") {
        return "Sint-Joost-ten-Node".to_string();
    }

    // ONE-OFF FIX: Alfons Borginon session 50 incorrectly lists "Mortsel", actually born in Lier
    if first_name == "Alfons" && last_name == "Borginon" && result.eq_ignore_ascii_case("mortsel") {
        return "Lier".to_string();
    }

    // ONE-OFF FIX: Elio Di Rupo session 55 has a too-specific birth place
    if first_name == "Elio"
        && last_name == "Di Rupo"
        && result.eq_ignore_ascii_case("morlanwelz-mariemont")
    {
        return "Morlanwelz".to_string();
    }

    // ONE-OFF FIX: Ahmed Laaouej session 55 has a too-general birth place
    if first_name == "Ahmed" && last_name == "Laaouej" && result.eq_ignore_ascii_case("luik") {
        return "Beyne-Heusay".to_string();
    }

    // ONE-OFF FIX: Jean-Marc Delizée session 48-55 has a too-general birth place
    if first_name == "Jean-Marc" && last_name == "Delizée" && result.eq_ignore_ascii_case("oignies")
    {
        return "Oignies-en-Thiérache".to_string();
    }

    // ONE-OFF FIX: Yoleen Van Camp session 54 has a too-general birth place
    if first_name == "Yoleen" && last_name == "Van Camp" && result.eq_ignore_ascii_case("antwerpen")
    {
        return "Wilrijk".to_string();
    }

    result
}

fn normalize_hyphens(s: &str) -> String {
    s.split('-')
        .map(|part| part.trim())
        .collect::<Vec<_>>()
        .join("-")
}

fn extract_birth_date(document: &Html) -> String {
    document
        .select(&selector_p())
        .find(|el| el.text().any(|t| t.contains("Geboren")))
        .and_then(|el| {
            let text = el.text().collect::<String>();
            let segment = text
                .split('|')
                .map(str::trim)
                .find(|s| s.to_lowercase().contains("geboren"))
                .unwrap_or("")
                .to_string();
            // Modern format: "12/05/1992"
            if let Some(caps) = REGEX_BIRTH_DATE_SLASH.captures(&segment) {
                let date_str = caps.name("date")?.as_str();
                let parts: Vec<&str> = date_str.split('/').collect();
                let day: u32 = parts[0].parse().ok()?;
                let month: u32 = parts[1].parse().ok()?;
                let year: i32 = parts[2].parse().ok()?;
                return NaiveDate::from_ymd_opt(year, month, day)
                    .map(|d| d.format("%Y-%m-%d").to_string());
            }
            // Old dutch-month format: "12 april 1975"
            if let Some(caps) = REGEX_BIRTH_DATE_WORDS.captures(&segment) {
                let raw = caps
                    .name("date")?
                    .as_str()
                    .trim()
                    .trim_end_matches('.')
                    .to_string();
                let parsed = parse_dutch_date(&raw);
                if !parsed.is_empty() {
                    return Some(parsed);
                }
            }
            None
        })
        .unwrap_or_default()
}

/// Returns false if the candidate (start, end) period clearly does not
/// overlap this session's own (period_start, period_end) window — e.g. a
/// "van ... tot ..." match in the paragraph that actually describes an
/// earlier, unrelated stint (different constituency/arrondissement) rather
/// than the member's tenure during this specific session.
/// Relies on ISO "YYYY-MM-DD" strings, which sort lexicographically in
/// chronological order.
fn is_period_relevant(
    start: Option<&str>,
    end: Option<&str>,
    period_start: Option<&str>,
    period_end: Option<&str>,
) -> bool {
    // Candidate ended before this session's period even started.
    if let (Some(end), Some(p_start)) = (end, period_start) {
        if end < p_start {
            return false;
        }
    }
    // Candidate starts after this session's period already ended.
    if let (Some(start), Some(p_end)) = (start, period_end) {
        if start > p_end {
            return false;
        }
    }
    true
}

/// If `start` predates the current session's own Zittingsperiode start
/// (`period_start`), clamp it to `period_start` instead. This handles
/// members whose mandate paragraph reports the date of their *original*
/// election (e.g. "sedert 31 juli 1984") even though the row being built
/// is for a later session they continued into (e.g. session 48, which
/// itself started 16.12.1991).
/// Relies on both dates being ISO "YYYY-MM-DD" strings, which sort
/// lexicographically in chronological order.
fn clamp_start_to_period(start: Option<String>, period_start: Option<&str>) -> Option<String> {
    match (start, period_start) {
        (Some(s), Some(p)) if s.as_str() < p => Some(p.to_string()),
        (start, _) => start,
    }
}

/// Extract the date on which the member started.
fn extract_start_date(document: &Html) -> Option<String> {
    // Try to find a paragraph containing "sedert" or "sinds"
    let text = document
        .select(&selector_p())
        .find(|el| {
            el.text()
                .any(|t| t.contains("sedert") || t.contains("sinds"))
        })
        .map(|el| el.text().collect::<String>());

    let text = match text {
        Some(t) => t,
        None => {
            // Fallback: search ALL text in the document for sedert/sinds
            let full = document.root_element().text().collect::<String>();
            if full.contains("sedert") || full.contains("sinds") {
                full
            } else {
                return None;
            }
        }
    };

    let keyword_pos = text
        .find("sedert")
        .map(|pos| (pos, 6))
        .or_else(|| text.find("sinds").map(|pos| (pos, 5)))?;
    let (pos, keyword_len) = keyword_pos;
    let after = text[pos + keyword_len..].trim_start();

    // Try numeric format: dd/mm/yyyy
    if let Some(caps) = REGEX_START_DATE.captures(after) {
        let day: u32 = caps.get(1)?.as_str().parse().ok()?;
        let month: u32 = caps.get(2)?.as_str().parse().ok()?;
        let year: i32 = caps.get(3)?.as_str().parse().ok()?;
        if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
            return Some(date.format("%Y-%m-%d").to_string());
        }
    }

    // Try Dutch month format: "3 oktober 2019" (search anywhere in `after`,
    // not just an exact 3-token segment — there may be trailing text like
    // "ter vervanging van de heer Jan Jambon." before the next `|`)
    if let Some(caps) = REGEX_START_DATE_WORDS.captures(after) {
        let day: u32 = caps.get(1)?.as_str().parse().ok()?;
        let month = dutch_month_to_number(caps.get(2)?.as_str())?;
        let year: i32 = caps.get(3)?.as_str().parse().ok()?;
        if let Some(date) = NaiveDate::from_ymd_opt(year, month, day) {
            return Some(date.format("%Y-%m-%d").to_string());
        }
    }

    None
}

/// Parses a single date string in either "9/3/2017" or "9 maart 2017" format.
fn parse_flexible_date(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if let Some(caps) = REGEX_DATE_SLASH.captures(raw) {
        let day: u32 = caps.get(1)?.as_str().parse().ok()?;
        let month: u32 = caps.get(2)?.as_str().parse().ok()?;
        let year: i32 = caps.get(3)?.as_str().parse().ok()?;
        return NaiveDate::from_ymd_opt(year, month, day).map(|d| d.format("%Y-%m-%d").to_string());
    }
    let parsed = parse_dutch_date(raw);
    if !parsed.is_empty() {
        return Some(parsed);
    }
    None
}

/// Extracts an explicit "van <date> tot <date>" period from the
/// representative paragraph, e.g. "van 9 maart 2017 tot 9 december 2018".
/// Used when the mandate has already ended and isn't phrased with
/// "sedert"/"sinds".
fn extract_van_tot_period(paragraph: Option<&str>) -> (Option<String>, Option<String>) {
    let text = match paragraph {
        Some(t) => t,
        None => return (None, None),
    };
    match REGEX_VAN_TOT.captures(text) {
        Some(caps) => {
            let start = caps.get(1).and_then(|m| parse_flexible_date(m.as_str()));
            let end = caps.get(2).and_then(|m| parse_flexible_date(m.as_str()));
            (start, end)
        }
        None => (None, None),
    }
}

/// Extracts an end-of-mandate date such as "tot 30/09/2024" from the
/// representative paragraph, if present.
fn extract_end_date(paragraph: Option<&str>) -> Option<String> {
    let text = paragraph?;
    let caps = REGEX_END_DATE.captures(text)?;
    let date_str = caps.get(1)?.as_str();
    let parts: Vec<&str> = date_str.split('/').collect();
    let day: u32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let year: i32 = parts[2].parse().ok()?;
    NaiveDate::from_ymd_opt(year, month, day).map(|d| d.format("%Y-%m-%d").to_string())
}

/// Parses the "CV: Zittingsperiode NN (dd.mm.yyyy - dd.mm.yyyy)" heading
/// that appears on some (mostly older) CV pages, for the entry matching
/// `session_id`. Returns (start, end) as ISO dates.
fn extract_period_dates(document: &Html, session_id: i32) -> (Option<String>, Option<String>) {
    let text = document
        .select(&selector_h4())
        .map(|el| el.text().collect::<String>())
        .collect::<Vec<_>>()
        .join(" ");
    for caps in REGEX_ZITTINGSPERIODE.captures_iter(&text) {
        let period: i32 = match caps.get(1).and_then(|m| m.as_str().parse().ok()) {
            Some(p) => p,
            None => continue,
        };
        if period != session_id {
            continue;
        }
        let start = make_date(&caps, 2, 3, 4);
        let end = make_date(&caps, 5, 6, 7);
        return (start, end);
    }
    (None, None)
}

fn make_date(caps: &Captures, day_idx: usize, month_idx: usize, year_idx: usize) -> Option<String> {
    let day: u32 = caps.get(day_idx)?.as_str().parse().ok()?;
    let month: u32 = caps.get(month_idx)?.as_str().parse().ok()?;
    let year: i32 = caps.get(year_idx)?.as_str().parse().ok()?;
    NaiveDate::from_ymd_opt(year, month, day).map(|d| d.format("%Y-%m-%d").to_string())
}

/// Extracts the "function" segment(s) from the representative paragraph,
/// e.g. "Gemeenteraadslid van Harelbeke". This is whatever is left over in
/// the `|`-separated paragraph after removing the representative/kieskring
/// segment, the "Fractie:" segment, and the "Geboren..." segment.
fn extract_function(paragraph: Option<&str>) -> String {
    let text = match paragraph {
        Some(t) => t,
        None => return String::new(),
    };
    text.split('|')
        .map(str::trim)
        .filter(|seg| {
            !seg.is_empty()
                && !seg.starts_with("Volksvertegenwoordiger")
                && !seg.starts_with("Fractie")
                && !seg.contains("Geboren")
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn extract_constituency(rep_para: Option<&str>) -> String {
    let text = rep_para.unwrap_or_default();
    let mut current: Option<String> = None;

    for caps in REGEX_CONSTITUENCY.captures_iter(text) {
        let name = caps.get(1).unwrap().as_str().trim().to_string();
        let keyword = caps.get(2).map(|m| m.as_str());
        // "sedert X" / "sinds X" (or no trailing date at all) => ongoing/current term.
        // "van X tot Y" => this term has ended, skip it.
        let is_ongoing = matches!(keyword, None | Some("sedert") | Some("sinds") | Some("tot"));
        if is_ongoing {
            current = Some(name);
        }
    }

    let result = normalize_hyphens(&current.unwrap_or_default());

    // ONE-OFF FIX: Vincent Scourneau session 55
    if result.eq_ignore_ascii_case("waals brabant") {
        return "Waals-Brabant".to_string();
    }

    // ONE-OFF FIX: Jacques Simonet session 49
    if result.eq_ignore_ascii_case("Brussel-halle-Vilvoorde") {
        return "Brussel-Halle-Vilvoorde".to_string();
    }

    // ONE-OFF FIX: Massin Eric session 54
    if result.eq_ignore_ascii_case("Henegouwen van18 mei 2003") {
        return "Henegouwen".to_string();
    }

    result
}

/// The list page stores names as "Last First"; reorder to "First Last".
fn reorder_name(raw: String) -> String {
    let mut parts: Vec<&str> = raw.split_whitespace().collect();
    if parts.len() > 1 {
        let first = parts.pop().unwrap();
        format!("{} {}", first, parts.join(" "))
    } else {
        raw
    }
}

fn split_name(name: &str) -> (String, String) {
    match name.splitn(2, ' ').collect::<Vec<_>>().as_slice() {
        [first, rest] => (first.to_string(), rest.to_string()),
        [first] => (first.to_string(), String::new()),
        _ => (String::new(), String::new()),
    }
}

/// Applies known corrections to scraped first/last names.
fn fix_name(first_name: String, last_name: String) -> (String, String) {
    let first_name = match first_name.as_str() {
        "Karine" if last_name == "Jiroflée" => "Karin".to_string(),
        "Éric" if last_name == "Thiébaut" => "Eric".to_string(),
        _ => first_name,
    };

    let last_name = match last_name.as_str() {
        "Hugon" if first_name == "Claire" => "Hugon Lecharlier".to_string(),
        _ => last_name,
    };

    (first_name, last_name)
}

fn parse_dutch_date(raw: &str) -> String {
    let parts: Vec<&str> = raw.split_whitespace().collect();
    if parts.len() != 3 {
        return String::new();
    }
    match (
        parts[0].parse::<u32>(),
        dutch_month_to_number(parts[1]),
        parts[2].parse::<i32>(),
    ) {
        (Ok(day), Some(month), Ok(year)) => NaiveDate::from_ymd_opt(year, month, day)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn extract_from_row(row: &ElementRef, selector: &Selector, attr: Option<&str>) -> Option<String> {
    row.select(selector).next().map(|el| match attr {
        Some(a) => el.value().attr(a).unwrap_or_default().to_string(),
        None => el
            .text()
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
    })
}

fn extract_sibling_text(document: &Html, label: &str) -> Option<String> {
    document
        .select(&selector_p_i())
        .find(|el| el.text().any(|t| t.contains(label)))
        .and_then(|el| {
            el.next_sibling()
                .and_then(|sib| sib.value().as_text().map(|t| t.trim().to_string()))
        })
}

fn extract_member_key(document: &Html) -> Option<String> {
    document.select(selector_a()).find_map(|el| {
        let href = el.value().attr("href")?;
        REGEX_MEMBER_KEY
            .captures(href)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string())
    })
}
