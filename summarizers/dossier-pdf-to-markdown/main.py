import base64
import os
import re
import shutil
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Callable, Optional

import fitz
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# GLOBAL FLAGS
DEBUG = True
SESSION_ID = 56

# SINGLE DOSSIER FILTER (SET TO NONE TO PROCESS ALL)
ONLY_DOSSIER_ID = None  # "859"

# LAYOUT FRACTIONS
FRENCH_RIGHT_HALF_FRACTION = 0.50
TITLE_PAGE_TOP_FRACTION = 0.30
TITLE_PAGE_BOTTOM_FRACTION = 0.55
PAGE_TOP_FRACTION = 0.08
PAGE_BOTTOM_FRACTION = 0.09

# STRIP CERTAIN PATTERNS
_PATTERNS_TO_REMOVE = [
    # Date line
    r"\*?(?:Brussel|Bruxelles)[^*\n]*\d{4}[^*\n]*\*?\n?",
    # Dutch
    r"\*?De voorzitt?e?r(?:ster)? van de Kamer[^*\n]*\*?\n?",
    r"\*?van volksvertegenwoordigers,[^*\n]*\*?\n?",
    r"\*?De griffier(?:ster)? van de Kamer[^*\n]*\*?\n?",
    # French
    r"\*?L[ae] pr[eé]sident(?:e)? de la Chambre[^*\n]*\*?\n?",
    r"\*?des représentants,[^*\n]*\*?\n?",
    r"\*?L[ae] greffier(?:ère)? de la Chambre[^*\n]*\*?\n?",
    # Stray star separators: **, * *, ** *, * ** etc. (any combo of 1-3 stars with spaces)
    r"^\s*\*{1,2}\s*\*?\s*\*{0,2}\s*$",
    # Signature block: *De rapporteur(s)/rapportrice, De voorzitt(er/ster),*
    # followed by a line of names run together
    r"\*De rapport(?:eur(?:s)?|rice),\s*De voorzitt?e?r(?:ster)?,\*\n[^\n]+\n?",
    # Salutation
    r"\*?Dames en [Hh]eren,\*?\n?",
]


def strip_patterns(text: str) -> str:
    for pattern in _PATTERNS_TO_REMOVE:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.MULTILINE)
    text = text.rstrip() + "\n"
    return text


# HELPER TO DETECT AND IGNORE CERTAIN PAGES
COMMITTEE_PAGE_MARKERS = [
    "samenstelling van de commissie",
    "composition de la commission",
    "vaste leden",
    "titulaires",
    "plaatsvervangers",
    "suppléants",
    "afkorting bij de nummering van de publicaties",
    "abréviations",
    "mouvement réformateur",
]

TABLE_OF_CONTENTS_PAGE_MARKERS = [
    "inhoud",
    "sommaire",
]

# Regex to detect "same as committee text" redirect pages
# _REDIRECT_PATTERN = re.compile(
#     r"(?:"
#     # Dutch: "...is dezelfde als de tekst/artikelen aangenomen [in eerste/tweede lezing] door de commissie"
#     r"tekst aangenomen door de plenaire vergadering\s+is dezelfde als de (?:tekst|artikelen) aangenomen"
#     r"(?:\s+in (?:eerste|tweede) lezing)?\s+door de commissie"
#     r"|"
#     # French: mirror both variants (texte/articles, première/deuxième lecture)
#     r"texte adopt[eé] (?:en s[eé]ance pl[eé]ni[eè]re|par la s[eé]ance pl[eé]ni[eè]re)\s+est identique (?:au texte adopt[eé]|aux articles adopt[eé]s)"
#     r"(?:\s+en (?:premi[eè]re|deuxi[eè]me) lecture)?\s+par la commission"
#     r")"
#     r"\s*\(?DOC\s+(\d+)\s+(\d+)/(\d{3,4})\)?",
#     re.IGNORECASE | re.DOTALL,
# )

_REDIRECT_PATTERN = re.compile(
    r"(?:tekst aangenomen door de plenaire vergadering\s+is dezelfde als de (?:tekst|artikelen)[^()]{0,150}?|texte adopt[eé] (?:en s[eé]ance pl[eé]ni[eè]re|par la s[eé]ance pl[eé]ni[eè]re)\s+est identique (?:au texte|aux articles)[^()]{0,150}?)\s*\(?DOC\s+(\d+)\s+(\d+)\/(\d{3,4})\)?",
    re.IGNORECASE | re.DOTALL,
)


# Trigger phrases indicating a "same as" redirect (Dutch + French, any variant:
# plenary=committee, second reading=first reading, etc.)
_REDIRECT_TRIGGER = re.compile(
    r"is dezelfde als|est identique (?:à|au|aux)",
    re.IGNORECASE,
)

# DOC citation, tolerant of "zie DOC", "voir DOC", "(DOC", "(zie DOC", etc.
_DOC_REF_PATTERN = re.compile(
    r"DOC\s+(\d+)\s+(\d+)\/(\d{3,4})",
    re.IGNORECASE,
)

# How far past the trigger phrase we're willing to look for the DOC reference
_REDIRECT_WINDOW = 250


def detect_adopted_text_redirect(pdf_path: Path) -> Optional[str]:
    """
    If the first page(s) of *pdf_path* say the text is the same as some
    other DOC (plenary==committee, second reading==first reading, etc.)
    and cite a DOC reference nearby, return the URL of that document.
    Otherwise return None.
    """
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc[: min(2, len(doc))]:
            text += page.get_text()
        doc.close()
    except Exception:
        return None

    trig = _REDIRECT_TRIGGER.search(text)
    if not trig:
        return None

    window = text[trig.end(): trig.end() + _REDIRECT_WINDOW]
    m = _DOC_REF_PATTERN.search(window)
    if not m:
        return None

    session_str, dossier_str, seq_str = m.group(1), m.group(2), m.group(3)
    filename = f"{session_str}K{dossier_str}{seq_str}.pdf"
    url = f"https://www.dekamer.be/FLWB/PDF/56/{dossier_str}/{filename}"
    tqdm.write(f"  [redirect] DOC {session_str} {dossier_str}/{seq_str} → {url}")
    return url


def is_committee_composition_page(page) -> bool:
    text = page.get_text().lower()
    hits = sum(1 for marker in COMMITTEE_PAGE_MARKERS if marker in text)
    return hits >= 2


def is_table_of_contents_page(page) -> bool:
    text = page.get_text().lower()
    hits = sum(1 for marker in TABLE_OF_CONTENTS_PAGE_MARKERS if marker in text)
    return hits >= 2


def _report_skip_page(page_idx: int, page) -> bool:
    if page_idx == 1 and is_committee_composition_page(page):
        return True
    if page_idx == 2 and is_table_of_contents_page(page):
        return True
    text = page.get_text()
    if text and is_garbled(text, threshold=0.02):
        return True
    return False


def _adopted_text_skip_page(page_idx: int, page) -> bool:
    if page_idx == 1 and is_committee_composition_page(page):
        return True
    if page_idx == 2 and is_table_of_contents_page(page):
        return True
    text = page.get_text()
    if text and is_garbled(text, threshold=0.02):
        return True
    return False


# ── Per-document extraction config ────────────────────────────────────────────
@dataclass
class ExtractionConfig:
    """Controls how a single PDF is extracted to markdown."""

    # Output markdown filename (relative to the dossier dir)
    md_filename: str
    # Header/footer/column fractions — defaults mirror the original constants
    french_right_half_fraction: float = FRENCH_RIGHT_HALF_FRACTION
    title_page_top_fraction: float = TITLE_PAGE_TOP_FRACTION
    title_page_bottom_fraction: float = TITLE_PAGE_BOTTOM_FRACTION
    page_top_fraction: float = PAGE_TOP_FRACTION
    page_bottom_fraction: float = PAGE_BOTTOM_FRACTION
    skip_page: Optional[Callable[[int, "fitz.Page"], bool]] = None


ADOPTED_TEXT_CONFIG = ExtractionConfig(
    md_filename="adopted_text.md", skip_page=_adopted_text_skip_page
)
REPORT_CONFIG = ExtractionConfig(
    md_filename="report.md",
    page_top_fraction=0.07,
    page_bottom_fraction=0.08,
    skip_page=_report_skip_page,
)
ORIGINAL_TEXT_CONFIG = ExtractionConfig(
    md_filename="original_text.md", skip_page=_adopted_text_skip_page
)


# ── Layout helpers ─────────────────────────────────────────────────────────────
def is_right_column(bbox, page_width, cfg: ExtractionConfig):
    center_x = (bbox[0] + bbox[2]) / 2
    return center_x > page_width * (1.0 - cfg.french_right_half_fraction)


def is_header(bbox, page_height, page_idx, cfg: ExtractionConfig):
    top_frac = cfg.title_page_top_fraction if page_idx == 0 else cfg.page_top_fraction
    return bbox[3] < page_height * top_frac


def is_footer(bbox, page_height, page_idx, cfg: ExtractionConfig):
    bot_frac = (
        cfg.title_page_bottom_fraction if page_idx == 0 else cfg.page_bottom_fraction
    )
    return bbox[1] > page_height * (1.0 - bot_frac)


def span_to_markdown(span) -> str:
    text = span["text"]
    flags = span["flags"]
    if flags & (1 << 4):
        text = f"**{text}**"
    if flags & (1 << 1):
        text = f"*{text}*"
    return text


# CONVERT A SINGLE PDF PAGE TO MARKDOWN
def convert_page_to_markdown(page, page_idx: int, cfg: ExtractionConfig):
    """
    Returns (markdown_str, debug_blocks).
    debug_blocks: list of {"bbox", "reason": kept|header|footer|right_column, "text"}
    """
    if cfg.skip_page and cfg.skip_page(page_idx, page):
        # Return a single debug block covering the whole page
        pw = page.rect.width
        ph = page.rect.height
        debug_blocks = [
            {
                "bbox": (0, 0, pw, ph),
                "reason": "skipped_page",
                "text": "whole page skipped (committee composition / classifier match OR garbled)",
            }
        ]
        return "", debug_blocks

    pw = page.rect.width
    ph = page.rect.height
    blocks = page.get_text("dict")["blocks"]

    valid = []
    debug_blocks = []

    for b in blocks:
        if b["type"] != 0:
            continue
        bbox = b["bbox"]
        preview = ""
        for line in b["lines"]:
            for span in line["spans"]:
                preview += span["text"]
            if len(preview) > 60:
                break
        preview = preview[:80].replace('"', "'")

        if is_right_column(bbox, pw, cfg):
            debug_blocks.append(
                {"bbox": bbox, "reason": "right_column", "text": preview}
            )
            continue
        if is_header(bbox, ph, page_idx, cfg):
            debug_blocks.append({"bbox": bbox, "reason": "header", "text": preview})
            continue
        if is_footer(bbox, ph, page_idx, cfg):
            debug_blocks.append({"bbox": bbox, "reason": "footer", "text": preview})
            continue

        valid.append(b)

        debug_blocks.append({"bbox": bbox, "reason": "kept", "text": preview})

    valid.sort(key=lambda b: (b["bbox"][1], b["bbox"][0]))

    md = ""
    for b in valid:
        first_size = 0
        if b["lines"] and b["lines"][0]["spans"]:
            first_size = b["lines"][0]["spans"][0]["size"]

        if first_size >= 12:
            prefix = "## "
        elif first_size >= 11:
            prefix = "### "
        else:
            prefix = ""

        is_heading = bool(prefix)

        if is_heading:
            # For headings: plain text, all lines joined
            block_text = re.sub(
                r"\s+",
                " ",
                " ".join(span["text"] for line in b["lines"] for span in line["spans"]),
            ).strip()
        else:
            # For body text: collect (text, flags) pairs across all lines,
            # then merge consecutive runs with the same flags before wrapping.
            runs = []
            for line in b["lines"]:
                for span in line["spans"]:
                    text = span["text"]
                    flags = span["flags"]
                    if runs and runs[-1][1] == flags:
                        runs[-1] = (runs[-1][0] + text, flags)
                    else:
                        runs.append([text, flags])

            block_text = ""
            for text, flags in runs:
                text = re.sub(r"\s+", " ", text)
                if flags & (1 << 4):
                    text = f"**{text}**"
                if flags & (1 << 1):
                    text = f"*{text}*"
                block_text += text
            block_text = block_text.strip()

        md += f"{prefix}{block_text}\n\n"

    return md, debug_blocks


# ── Debug HTML ─────────────────────────────────────────────────────────────────
def render_page_as_base64(page, scale: float = 1.5):
    mat = fitz.Matrix(scale, scale)
    pix = page.get_pixmap(matrix=mat)
    buf = BytesIO()
    buf.write(pix.tobytes("png"))
    return base64.b64encode(buf.getvalue()).decode(), pix.width, pix.height


def is_garbled(text: str, threshold: float = 0.4) -> bool:
    """True if too high a fraction of characters are Cyrillic or private-use glyphs."""
    if not text:
        return False
    suspicious = sum(
        1
        for ch in text
        if "\u0400" <= ch <= "\u04ff"  # Cyrillic block
        or "\ue000" <= ch <= "\uf8ff"  # private use area
        or "\u0400" <= ch <= "\u052f"  # extended Cyrillic
    )
    return suspicious / len(text) > threshold


# GENERATE A DEBUG HTML PAGE
def write_debug_html(
    html_path: Path,
    page_data: list,
    dossier_id: str,
    doc_type: str,
    cfg: ExtractionConfig,
):
    colors = {
        "kept": ("rgba(0,180,60,0.15)", "rgba(0,160,50,0.8)"),
        "header": ("rgba(200,0,0,0.15)", "rgba(180,0,0,0.8)"),
        "footer": ("rgba(200,0,0,0.15)", "rgba(180,0,0,0.8)"),
        "right_column": ("rgba(220,100,0,0.15)", "rgba(200,80,0,0.8)"),
        "skipped_page": ("rgba(100,0,200,0.15)", "rgba(80,0,180,0.8)"),
    }
    page_title = f"Dossier {dossier_id} — {doc_type} extraction debug view"
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{page_title}</title>
<style>
  body  {{ background:#fff; color:#000; font-family:monospace; padding:20px; }}
  h1   {{ font-size:1.1em; }}
  h2   {{ font-size:1em; margin-top:2em; }}
  .wrap {{ position:relative; display:inline-block; border:1px solid #999; margin:8px 0; }}
  .wrap img {{ display:block; }}
  .block {{
    position: absolute;
    box-sizing: border-box;
    cursor: default;
  }}
  .block .tip {{
    display: none;
    position: absolute;
    left: 0; top: 100%;
    background: #fff;
    color: #000;
    font-size: 11px;
    padding: 3px 6px;
    border: 1px solid #999;
    white-space: normal;
    z-index: 10;
    max-width: 320px;
    word-break: break-word;
    pointer-events: none;
  }}
  .block:hover .tip {{ display: block; }}
  /* fraction guide lines */
  .guide {{
    position: absolute;
    box-sizing: border-box;
    pointer-events: none;
    z-index: 5;
  }}
  .guide-h {{
    left: 0; right: 0;
    height: 0;
    border-top: 2px dashed;
  }}
  .guide-v {{
    top: 0; bottom: 0;
    width: 0;
    border-left: 2px dashed;
  }}
  .guide .glabel {{
    position: absolute;
    font-size: 10px;
    padding: 1px 4px;
    white-space: nowrap;
    color: #fff;
    border-radius: 2px;
    line-height: 1.3;
  }}
  .guide-h .glabel {{ left: 4px; top: 2px; }}
  .guide-v .glabel {{ top: 4px; left: 4px; writing-mode: vertical-rl; }}
  .legend {{ margin: 12px 0 4px; font-size: 0.85em; display:flex; gap:16px; flex-wrap:wrap; }}
  .dot {{ display:inline-block; width:12px; height:12px; margin-right:4px;
         vertical-align:middle; border-radius:2px; }}
</style>
</head>
<body>
<h1>{page_title}</h1>
<div class="legend">
  <span><span class="dot" style="background:rgba(0,180,60,0.4);border:2px solid rgba(0,160,50,0.8)"></span>included</span>
  <span><span class="dot" style="background:rgba(200,0,0,0.4);border:2px solid rgba(180,0,0,0.8)"></span>excluded header/footer</span>
  <span><span class="dot" style="background:rgba(220,100,0,0.4);border:2px solid rgba(200,80,0,0.8)"></span>excluded right column</span>
  <span><span class="dot" style="background:transparent;border:2px dashed #e63;"></span>fraction guide (title page)</span>
  <span><span class="dot" style="background:transparent;border:2px dashed #36e;"></span>fraction guide (body pages)</span>
  <span><span class="dot" style="background:transparent;border:2px dashed #a0a;"></span>fraction guide (right column)</span>
  <span><span class="dot" style="background:rgba(100,0,200,0.4);border:2px solid rgba(80,0,180,0.8)"></span>skipped page</span>
</div>
"""
    html += (
        f"<a href='file:///SUMMARIZERSCACHE/sessions/56/dossiers/"
        f"{dossier_id}/"
        f"{'report_extraction.html' if doc_type == 'Adopted Text' else 'adopted_text_extraction.html'}' "
        f">{'REPORT LINK' if doc_type == 'Adopted Text' else 'ADOPTED TEXT LINK'}</a>"
    )
    html += f"<br>"

    html += (
        f"<a href='file:///SUMMARIZERSCACHE/sessions/56/dossiers/"
        f"{dossier_id}/"
        f"{'adopted_text_original.pdf' if doc_type == 'Adopted Text' else 'report_original.pdf'}' "
        f"target='_blank'>PDF LINK</a>"
    )

    for pd_ in page_data:
        scale_x = pd_["img_w"] / pd_["pdf_w"]
        scale_y = pd_["img_h"] / pd_["pdf_h"]
        img_w = pd_["img_w"]
        img_h = pd_["img_h"]
        page_idx = pd_["page_idx"]
        is_title = page_idx == 0

        # ── compute guide positions in image pixels ────────────────────
        # header fraction (depends on page type)
        title_top_y = cfg.title_page_top_fraction * img_h
        title_bot_y = (1.0 - cfg.title_page_bottom_fraction) * img_h
        body_top_y = cfg.page_top_fraction * img_h
        body_bot_y = (1.0 - cfg.page_bottom_fraction) * img_h
        right_col_x = (1.0 - cfg.french_right_half_fraction) * img_w

        # active guides for this page
        if is_title:
            active_top_y = title_top_y
            active_bot_y = title_bot_y
            guide_color_h = "#e63"
            guide_label_bg = "#e63"
            top_label = f"title_page_top ({cfg.title_page_top_fraction:.0%})"
            bot_label = (
                f"title_page_bottom ({cfg.title_page_bottom_fraction:.0%} from bottom)"
            )
        else:
            active_top_y = body_top_y
            active_bot_y = body_bot_y
            guide_color_h = "#36e"
            guide_label_bg = "#36e"
            top_label = f"page_top ({cfg.page_top_fraction:.0%})"
            bot_label = f"page_bottom ({cfg.page_bottom_fraction:.0%} from bottom)"

        html += f"<h2>Page {page_idx + 1}</h2>\n"
        html += f"<div class='wrap' style='width:{img_w}px;height:{img_h}px;'>\n"
        html += (
            f"<img src='data:image/png;base64,{pd_['img_b64']}' "
            f"style='width:{img_w}px;height:{img_h}px;'>\n"
        )

        # ── shade the excluded zones ───────────────────────────────────
        # header zone
        html += (
            f"<div style='position:absolute;left:0;top:0;width:{img_w}px;"
            f"height:{active_top_y:.1f}px;background:rgba(200,0,0,0.10);"
            f"pointer-events:none;z-index:4;'></div>\n"
        )
        # footer zone
        html += (
            f"<div style='position:absolute;left:0;top:{active_bot_y:.1f}px;"
            f"width:{img_w}px;height:{img_h - active_bot_y:.1f}px;"
            f"background:rgba(200,0,0,0.10);pointer-events:none;z-index:4;'></div>\n"
        )
        # right column zone
        html += (
            f"<div style='position:absolute;left:{right_col_x:.1f}px;top:0;"
            f"width:{img_w - right_col_x:.1f}px;height:{img_h}px;"
            f"background:rgba(220,100,0,0.10);pointer-events:none;z-index:4;'></div>\n"
        )

        # ── dashed guide lines ─────────────────────────────────────────
        # top header line
        html += (
            f"<div class='guide guide-h' style='top:{active_top_y:.1f}px;"
            f"border-color:{guide_color_h};'>"
            f"<span class='glabel' style='background:{guide_label_bg};'>{top_label}</span>"
            f"</div>\n"
        )
        # bottom footer line
        html += (
            f"<div class='guide guide-h' style='top:{active_bot_y:.1f}px;"
            f"border-color:{guide_color_h};'>"
            f"<span class='glabel' style='background:{guide_label_bg};'>{bot_label}</span>"
            f"</div>\n"
        )
        # right column line
        html += (
            f"<div class='guide guide-v' style='left:{right_col_x:.1f}px;border-color:#a0a;'>"
            f"<span class='glabel' style='background:#a0a;'>"
            f"right_col ({cfg.french_right_half_fraction:.0%})</span>"
            f"</div>\n"
        )

        # ── block overlays ─────────────────────────────────────────────
        for blk in pd_["blocks"]:
            x0, y0, x1, y1 = blk["bbox"]
            cx = x0 * scale_x
            cy = y0 * scale_y
            cw = (x1 - x0) * scale_x
            ch = (y1 - y0) * scale_y
            bg, border = colors.get(blk["reason"], colors["kept"])
            reason = blk["reason"]
            tip = blk["text"].replace("<", "&lt;").replace(">", "&gt;")
            html += (
                f"<div class='block' style='"
                f"left:{cx:.1f}px;top:{cy:.1f}px;width:{cw:.1f}px;height:{ch:.1f}px;"
                f"background:{bg};border:1px solid {border};'>"
                f"<span class='tip'>[{reason}] {tip}</span>"
                f"</div>\n"
            )
        html += "</div>\n"
    html += "</body></html>\n"
    html_path.write_text(html, encoding="utf-8")


# CONVERT A PDF TO MARKDOWN
def pdf_to_markdown(
    pdf_path: Path,
    cfg: ExtractionConfig,
    debug_html_path: Optional[Path],
    dossier_id: str,
    doc_type: str,
) -> str:
    doc = fitz.open(pdf_path)
    md = ""
    page_data = []

    for page_idx, page in enumerate(doc):
        page_md, debug_blocks = convert_page_to_markdown(page, page_idx, cfg)
        md += page_md
        if debug_html_path is not None:
            img_b64, img_w, img_h = render_page_as_base64(page, scale=1.5)
            page_data.append(
                {
                    "page_idx": page_idx,
                    "img_b64": img_b64,
                    "img_w": img_w,
                    "img_h": img_h,
                    "pdf_w": page.rect.width,
                    "pdf_h": page.rect.height,
                    "blocks": debug_blocks,
                }
            )

    if debug_html_path is not None:
        write_debug_html(debug_html_path, page_data, dossier_id, doc_type, cfg)

    md = strip_patterns(md)

    return md


# ── Download helper ────────────────────────────────────────────────────────────
def download_pdf(url: str, dest: Path) -> bool:
    """Download *url* to *dest*. Returns True on success, False otherwise."""
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        return True
    except Exception as exc:
        tqdm.write(f"[download error] {url}: {exc}")
        return False


def download_pdf_following_redirect(
    url: str, dest: Path, max_hops: int = 3
) -> tuple[bool, str]:
    """
    Download *url* to *dest*, following any adopted-text redirects.
    Returns (success, final_url_used).
    """
    current_url = url
    for hop in range(max_hops):
        if not download_pdf(current_url, dest):
            return False, current_url
        redirect_url = detect_adopted_text_redirect(dest)
        if redirect_url is None:
            return True, current_url  # no redirect — we're done
        # Follow the redirect: overwrite dest with the real document
        tqdm.write(f"  [redirect hop {hop + 1}] {current_url} → {redirect_url}")
        current_url = redirect_url
    # Exhausted hops — accept whatever we have
    tqdm.write(f"  [redirect] max hops reached, keeping last download")
    return True, current_url


# ── Cleanup helper ─────────────────────────────────────────────────────────────
# Files that should never be deleted (the raw PDF downloads)
_KEEP_FILENAMES = {
    "adopted_text_original.pdf",
    "report_original.pdf",
    "original_text_original.pdf",
}


def clean_dossier_dir(dossier_dir: Path):
    """Remove every file in *dossier_dir* except the original PDFs."""
    for item in dossier_dir.iterdir():
        if item.is_file() and item.name not in _KEEP_FILENAMES:
            item.unlink()


# ── Parquet helpers ────────────────────────────────────────────────────────────
def read_dossier_rows(parquet_path: Path) -> list[dict]:
    """
    Returns a list of dicts with keys: dossier_id, adopted_text_url, report_url.
    Rows where both URLs are absent are skipped.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("pyarrow is required: pip install pyarrow")

    table = pq.read_table(parquet_path)
    schema_names = table.schema.names

    rows = []
    ids = table.column("id").to_pylist() if "id" in schema_names else []
    adopted = (
        table.column("latest_adopted_text_url").to_pylist()
        if "latest_adopted_text_url" in schema_names
        else [None] * len(ids)
    )
    report = (
        table.column("latest_report_url").to_pylist()
        if "latest_report_url" in schema_names
        else [None] * len(ids)
    )
    original = (
        table.column("original_text_url").to_pylist()
        if "original_text_url" in schema_names
        else [None] * len(ids)
    )

    for dossier_id, at_url, r_url, orig_url in zip(ids, adopted, report, original):
        at_url = (at_url or "").strip() or None
        r_url = (r_url or "").strip() or None
        orig_url = (orig_url or "").strip() or None
        if at_url is None and r_url is None and orig_url is None:
            continue
        rows.append(
            {
                "dossier_id": str(dossier_id),
                "adopted_text_url": at_url,
                "report_url": r_url,
                "original_text_url": orig_url,
            }
        )

    return rows


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    load_dotenv()

    SCRAPER_DATA_DIR = Path(os.environ["SCRAPER_DATA_DIR"])
    SCRAPER_CACHE_DIR = Path(os.environ["SCRAPER_CACHE_DIR"])

    root = Path(__file__).resolve().parents[0]

    dossiers_parquet = SCRAPER_DATA_DIR / f"sessions/{SESSION_ID}/dossiers.parquet"

    pdf_cache_root = SCRAPER_CACHE_DIR / f"sessions/{SESSION_ID}/dossiers/pdfs"
    pdf_cache_root.mkdir(parents=True, exist_ok=True)

    output_root = SCRAPER_CACHE_DIR / f"sessions/{SESSION_ID}/dossiers"
    output_root.mkdir(parents=True, exist_ok=True)

    # Read dossier list
    if not dossiers_parquet.exists():
        print(f"[error] Parquet not found: {dossiers_parquet}")
        return

    rows = read_dossier_rows(dossiers_parquet)
    if not rows:
        print("[info] No rows with URLs found in parquet.")
        return

    # ── Optional single-dossier filter ────────────────────────────────────────
    if ONLY_DOSSIER_ID is not None:
        rows = [r for r in rows if r["dossier_id"] == ONLY_DOSSIER_ID]
        if not rows:
            print(f"[info] Dossier {ONLY_DOSSIER_ID!r} not found in parquet.")
            return

    web_requests = 0
    with tqdm(
        rows,
        desc="Processing dossiers",
        unit="dossier",
        dynamic_ncols=True,
    ) as pbar:
        pbar.set_postfix(web_req=0)
        for row in pbar:
            dossier_id = row["dossier_id"]
            pbar.set_description(f"Processing {dossier_id}")

            dossier_dir = pdf_cache_root / dossier_id
            dossier_dir.mkdir(parents=True, exist_ok=True)

            # ── Clean up derived files from previous runs ──────────────────
            # clean_dossier_dir(dossier_dir)

            # ── Define the two documents to process ───────────────────────
            documents = []
            if row["adopted_text_url"]:
                documents.append(
                    {
                        "url": row["adopted_text_url"],
                        "pdf_path": dossier_dir / "adopted_text_original.pdf",
                        "cfg": ADOPTED_TEXT_CONFIG,
                        "doc_type": "Adopted Text",
                    }
                )
            if row["report_url"]:
                documents.append(
                    {
                        "url": row["report_url"],
                        "pdf_path": dossier_dir / "report_original.pdf",
                        "cfg": REPORT_CONFIG,
                        "doc_type": "Report",
                    }
                )
            if row["original_text_url"]:
                documents.append(
                    {
                        "url": row["original_text_url"],
                        "pdf_path": dossier_dir / "original_text_original.pdf",
                        "cfg": ORIGINAL_TEXT_CONFIG,
                        "doc_type": "Original Text",
                    }
                )

            # ── Download + extract each document ──────────────────────────
            for doc in documents:
                url: str = doc["url"]
                pdf_path: Path = doc["pdf_path"]
                cfg: ExtractionConfig = doc["cfg"]
                doc_type: str = doc["doc_type"]

                md_path = dossier_dir / cfg.md_filename
                debug_name = Path(cfg.md_filename).stem + "_extraction.html"
                debug_path = dossier_dir / debug_name if DEBUG else None

                # Download if not cached
                if not pdf_path.exists():
                    pbar.set_description(f"Downloading {dossier_id}/{pdf_path.name}")
                    ok, final_url = download_pdf_following_redirect(url, pdf_path)
                    web_requests += 1  # at minimum 1 request; hops add more
                    pbar.set_postfix({"web_req": web_requests})
                    if not ok:
                        continue

                # Extract if not cached
                if md_path.exists():
                    continue

                # Extract
                try:
                    md = pdf_to_markdown(
                        pdf_path, cfg, debug_path, dossier_id, doc_type
                    )
                    md_path.write_text(md, encoding="utf-8")
                except Exception as exc:
                    tqdm.write(f"[error] {dossier_id}/{cfg.md_filename}: {exc}")


if __name__ == "__main__":
    main()
