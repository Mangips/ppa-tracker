"""
PPA Deal Tracker - Main Pipeline
Runs daily: fetches news, extracts deals, deduplicates, writes to SQLite + CSV.
"""

import os
import sys
import csv
import json
import time
import logging
import sqlite3
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path

import requests

import re as _re  
from googlenewsdecoder import gnewsdecoder

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── Config ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "ppa_deals.db"
CSV_PATH = DATA_DIR / "ppa_deals.csv"

NEWSAPI_KEY = os.environ["NEWSAPI_KEY"]
NEWSAPI_URL = "https://newsapi.org/v2/everything"

llm_KEY   = os.environ["MISTRAL_KEY"]
llm_URL   = "https://api.mistral.ai/v1/chat/completions"
llm_MODEL = os.environ.get("llm_MODEL", "mistral-small-latest")

MAX_ARTICLES = int(os.environ.get("MAX_ARTICLES") or 100000)  # Default: no limit

# Override with env var for testing, e.g. SEARCH_FROM_DATE=2026-01-01
LOOKBACK_DAYS = os.environ.get("LOOKBACK_DAYS")
SEARCH_FROM_DATE = os.environ.get("SEARCH_FROM_DATE")
SEARCH_TO_DATE = os.environ.get("SEARCH_TO_DATE")
NOTIFY_EMAIL_ENABLED = os.environ.get("NOTIFY_EMAIL_ENABLED")

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = DATA_DIR / "logs" / f"{datetime.utcnow().strftime('%Y-%m')}"
LOGS_DIR.mkdir(exist_ok=True)
ENV_NAME = os.environ.get("ENVIRONMENT", "ENV_NOT_SET")
LOG_PATH = LOGS_DIR / f"{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_from_{SEARCH_FROM_DATE}_{SEARCH_TO_DATE or 'present'}_{LOOKBACK_DAYS}_loockback_ENV_{ENV_NAME}_.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Google News RSS: one query per language.
# hl = UI language, gl = country, ceid = region:language
GOOGLE_NEWS_FEEDS = [
    # English
    ("en", "PPA signed Europe"),
    ("en", "power purchase agreement signed Europe"),
    # German
    ("de", "PPA unterzeichnet Deutschland"),
    # French
    ("fr", "PPA signé France"),
    # Spanish
    ("es", "PPA firmado Espana"),
    # Italian
    ("it", "PPA firmato Italia"),
    # Polish
    ("pl", "PPA podpisany Polen"),
    # Dutch
    ("nl", "PPA ondertekend Niederlanden"),
    # Portuguese
    ("pt", "PPA assinado Portugal"),
]

LANG_TO_CEID = {
    "en": ("en", "US", "US:en"),
    "de": ("de", "DE", "DE:de"),
    "fr": ("fr", "FR", "FR:fr"),
    "es": ("es", "ES", "ES:es"),
    "it": ("it", "IT", "IT:it"),
    "pl": ("pl", "PL", "PL:pl"),
    "nl": ("nl", "NL", "NL:nl"),
    "pt": ("pt", "PT", "PT:pt"),
}

# Direct trade press RSS feeds — European PPA/renewables coverage
EXTRA_RSS_FEEDS = [
    # Pan-European
    "https://energymonitor.ai/feed/",
    # Spain
    "https://elperiodicodelaenergia.com/feed/",
]

# ── Database ──────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS deals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_hash        TEXT UNIQUE,
            event_type       TEXT NOT NULL DEFAULT 'N', -- N=new, U=update, D=duplicate
            canonical_id     INTEGER,                   -- for U rows: points to original N row
            date_agreement   TEXT,
            date_found       TEXT,
            buyer            TEXT,
            seller           TEXT,
            capacity_mw      REAL,
            energy_gwh       REAL,
            tenure_years     REAL,
            country          TEXT,
            technology       TEXT,
            price_eur_mwh    REAL,
            source_url       TEXT,
            source_outlet    TEXT,
            publication_date TEXT,
            notes            TEXT,
            raw_snippet      TEXT
        );

        CREATE TABLE IF NOT EXISTS seen_urls (
            url     TEXT PRIMARY KEY,
            seen_at TEXT
        );

        CREATE VIEW IF NOT EXISTS latest_deals AS
        SELECT * FROM deals
        WHERE event_type != 'D'
          AND id IN (
            SELECT COALESCE(MAX(CASE WHEN event_type='U' THEN id END), MIN(id))
            FROM deals
            GROUP BY COALESCE(canonical_id, id)
          );
    """)
    conn.commit()

# ── News Fetching ─────────────────────────────────────────────────────────────

def fetch_newsapi(query: str, from_date: str, to_date: str) -> list[dict]:
    try:
        params = {
            "q":        query,
            "from":     from_date,
            "sortBy":   "publishedAt",
            "pageSize": 100,
            "apiKey":   NEWSAPI_KEY,
        }
        if to_date:
            params["to"] = to_date
            
        resp = requests.get(
            NEWSAPI_URL,
            params=params,
            timeout=15,
        )    

        resp.raise_for_status()
        data     = resp.json()
        articles = data.get("articles", [])
        log.info(f"NewsAPI '{query}': {len(articles)} results")
        if data.get("status") != "ok":
            log.warning(f"NewsAPI non-ok status: {data.get('message')}")
        return articles
    except Exception as e:
        log.warning(f"NewsAPI error for '{query}': {e}")
        return []


def fetch_google_news_rss(lang: str, query: str, from_date: str, to_date: str) -> list[dict]:
    """Fetch articles from Google News RSS. No API key required."""
    hl, gl, ceid = LANG_TO_CEID.get(lang, ("en", "US", "US:en"))

    # Append date operators to the base query
    full_query = query
    if from_date:
        full_query += f" after:{from_date}"
    if to_date:
        full_query += f" before:{to_date}"

    url = (
        f"https://news.google.com/rss/search"
        f"?q={requests.utils.quote(full_query)}"
        f"&hl={hl}&gl={gl}&ceid={ceid}"
    )
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; PPA-Tracker/1.0)"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        root  = ET.fromstring(resp.content)
        items = root.findall(".//item")
        articles = []
        for item in items:
            title   = (item.findtext("title")   or "").strip()
            link = (item.findtext("link") or "").strip()
            if link and not link.startswith("http"):
                link = f"https://news.google.com/rss/articles/{link}"
            pub     = (item.findtext("pubDate") or "").strip()
            source_el = item.find("source")
            outlet  = source_el.text if source_el is not None else ""
            # pubDate is like "Mon, 19 May 2026 10:00:00 GMT"
            try:
                pub_dt  = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z")
                pub_iso = pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                pub_iso = pub

            raw_desc = (item.findtext("description") or title).strip()
            description = _re.sub(r"<[^>]+>", " ", raw_desc).strip()

            articles.append({
                "title":       title,
                "url":         link,
                "publishedAt": pub_iso,
                "source":      {"name": outlet},
                "description": description,
            })
            
        log.info(f"Google News RSS [{lang}] {url}: {len(articles)} results")
        return articles  
    except Exception as e:
        log.warning(f"Google News RSS error [{lang}] '{query}': {e}")
        return []

def fetch_extra_rss(feed_url: str, from_date: str, to_date: str | None = None) -> list[dict]:
    """Fetch a direct RSS feed and return articles newer than from_date."""
    try:
        resp = requests.get(feed_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()

        articles = []
        from xml.etree.ElementTree import fromstring
        root = fromstring(resp.text)
        ns   = {"atom": "http://www.w3.org/2005/Atom"}

        # Support both RSS <item> and Atom <entry>
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)

        cutoff = datetime.strptime(from_date, "%Y-%m-%d").date() if from_date else None

        for item in items:
            title_el = item.find("title")
            link_el  = item.find("link") or item.find("atom:link", ns)
            date_el  = item.find("pubDate") or item.find("atom:published", ns)

            title = title_el.text.strip()         if title_el is not None else ""
            link  = link_el.text                  if link_el  is not None else ""
            if not link and link_el is not None:   # Atom <link href="..."/>
                link = link_el.get("href", "")
            pub_raw = date_el.text.strip()         if date_el  is not None else ""

            # Parse publication date for cutoff filtering
            pub_date = ""
            for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    pub_date = datetime.strptime(pub_raw, fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

            if cutoff and pub_date and pub_date < from_date:
                continue
            if to_date and pub_date and pub_date > to_date:
                continue

            # Skip obviously irrelevant articles before hitting llm
            text_lower = (title).lower()
            if not any(kw in text_lower for kw in ["ppa", "power purchase", "offtake", "rinnovab", "erneuerbar", "renovable", "renouvelable"]):
                continue

            articles.append({
                "title":       title,
                "url":         link,
                "publishedAt": pub_date,
                "source":      {"name": feed_url.split("/")[2]},  # domain as source name
                "description": "",
            })

        log.info(f"Extra RSS [{feed_url.split('/')[2]}]: {len(articles)} results")
        return articles

    except Exception as e:
        log.warning(f"Extra RSS error [{feed_url}]: {e}")
        return []


# ── Full Text Fetch ───────────────────────────────────────────────────────────

class _TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.chunks = []
        self._skip  = 0  # counter, not bool

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "nav", "footer", "header"):
            self._skip += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style", "nav", "footer", "header") and self._skip:
            self._skip -= 1

    def handle_data(self, data):
        if not self._skip:
            s = data.strip()
            if s:
                self.chunks.append(s)


def fetch_full_text(url: str) -> str | None:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        resp = requests.get(url, headers=headers, timeout=12, allow_redirects=True)
        
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            parser = _TextExtractor()
            parser.feed(resp.text)
            text  = " ".join(parser.chunks)
            words = text.split()
            return " ".join(words[:4000]) if len(words) > 4000 else text
            
        log.info(f"Full text skipped (status {resp.status_code}): {url[:80]}")
        return None
    except Exception as e:
        log.info(f"Full text fetch failed ({e}): {url[:80]}")
        return None

# ── llm Extraction ─────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """\
You are an expert energy analyst. Extract structured information about Power Purchase Agreement (PPA) deals.

Analyze the text below and:
1. Identify **ALL SIGNED/COMPLETED PPA deals** described (not rumours, tenders, negotiations or proposals).
2. For **EACH deal**, extract all fields below into a **separate JSON object**.
3. Return a **JSON array** of these objects (one per deal).
4. If **NO signed deals** are found, return an array with **ONE object** where `is_signed_deal` is `false` and all other fields are `null`.
5. If signed, was it signed in Europe? 

Return **ONLY** a valid JSON array — no markdown fences, no explanation, nothing else.
Each object must include **ALL fields** below (use `null` for missing values):

{{
  "is_signed_deal": true or false,
  "is_likely_update": true or false,
  "update_clues": "what changed, or null",
  "date_agreement": "YYYY-MM-DD, YYYY-MM, or YYYY — null if unknown",
  "buyer": "offtaker company name(s), comma-separated",
  "seller": "developer / generator / IPP name(s)",
  "capacity_mw": number or null,
  "energy_gwh": number or null,
  "tenure_years": number or null,
  "is_european":      true or false,
  "country": "delivery country",
  "technology": "solar / wind onshore / wind offshore / hydro / mixed / other",
  "price_eur_mwh": number or null,
  "notes": "project name, grid details, special terms, or null",
  "confidence": "high / medium / low"
}}

Text (any language — return all fields in English):
---
{text}"""


def extract_with_llm(text: str, title: str, outlet: str) -> dict | list | None:
    for attempt, text_limit in enumerate([6000, 3000]):
        prompt  = EXTRACTION_PROMPT.format(text=text[:text_limit])
        payload = {
            "model":       llm_MODEL,
            "temperature": 0.1,
            "max_tokens":  1024,
            "messages":    [{"role": "user", "content": prompt}],
        }
        try:
            resp = requests.post(
                llm_URL,
                headers={
                    "Authorization": f"Bearer {llm_KEY}",
                    "Content-Type":  "application/json",
                },
                json=payload,
                timeout=30,
            )
            log.info(f"LLM HTTP {resp.status_code} for: {title[:60]}")
            if resp.status_code == 429:
                wait = int(resp.headers.get("retry-after", 60))
                if wait > 120:  # daily limit exhausted, not a transient burst
                    log.warning(f"LLM daily limit exhausted (retry-after: {wait}s) — stopping run")
                    return None  # let the pipeline finish cleanly with what it has
                log.warning(f"LLM 429 — waiting {wait}s: {title[:50]}")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                log.warning(f"LLM error body: {resp.text[:300]}")
                return None

            content = resp.json()["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            parsed = json.loads(content)

            if isinstance(parsed, dict):
                log.info(
                    f"LLM extracted — signed={parsed.get('is_signed_deal')} "
                    f"confidence={parsed.get('confidence')} "
                    f"buyer={parsed.get('buyer')} seller={parsed.get('seller')} "
                    f"| {title[:50]}"
                )
            elif isinstance(parsed, list):
                signed_deals = [d for d in parsed if d.get("is_signed_deal")]
                log.info(
                    f"LLM extracted {len(parsed)} deals ({len(signed_deals)} signed) | {title[:50]}"
                )
            return parsed

        except json.JSONDecodeError as e:
            log.warning(
                f"LLM JSON parse error (attempt {attempt+1}, {outlet}): {e} "
                f"| raw: {content[:200]}"
            )
            if attempt == 0:
                log.info("Retrying with shorter input...")
                continue  # retry with 3000 chars
            return None
        except Exception as e:
            log.warning(f"LLM call failed ({outlet}): {e}")
            return None

    return None

# ── Deduplication ─────────────────────────────────────────────────────────────

COUNTRY_ALIASES = {
    "uk": "united kingdom",
    "great britain": "united kingdom",
    "britain": "united kingdom",
    "czechia": "czech republic",
    "the netherlands": "netherlands",
    "holland": "netherlands",
}

LEGAL_SUFFIXES = _re.compile(
    r"\b(ltd\.?|llc\.?|inc\.?|corp\.?|ag|sa|spa|bv|nv|gmbh|plc|oy|ab|as|group ag|supply ltd|energy ltd|renewables ltd)\b\.?",
    _re.IGNORECASE,
)

EUROPEAN_COUNTRIES = {
    "austria", "belgium", "bulgaria", "croatia", "cyprus", "czech republic",
    "denmark", "estonia", "finland", "france", "germany", "greece", "hungary",
    "ireland", "italy", "latvia", "lithuania", "luxembourg", "malta",
    "netherlands", "poland", "portugal", "romania", "slovakia", "slovenia",
    "spain", "sweden", "united kingdom", "norway", "switzerland", "ukraine",
    "serbia", "albania", "north macedonia", "montenegro", "bosnia", "iceland",
}

def _normalize_country(country: str) -> str:
    c = country.lower().strip()
    return COUNTRY_ALIASES.get(c, c)

def _normalize_entity(name: str) -> str:
    n = name.lower().strip()
    n = LEGAL_SUFFIXES.sub("", n)
    return " ".join(n.split())  # collapse whitespace

def make_deal_hash(extracted: dict) -> str:
    date = (extracted.get("date_agreement") or "")[:7]  # truncate to YYYY-MM
    parts = [
        _normalize_entity(extracted.get("buyer")   or ""),
        _normalize_entity(extracted.get("seller")  or ""),
        _normalize_country(extracted.get("country") or ""),
        str(extracted.get("capacity_mw") or 0),
        date,
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]

def is_european_deal(deal: dict) -> bool:
    country = (deal.get("country") or "").lower().strip()
    country = COUNTRY_ALIASES.get(country, country)
    return country in EUROPEAN_COUNTRIES

def find_duplicate(conn: sqlite3.Connection, deal_hash: str) -> dict | None:
    """Returns the full existing row as a dict, or None."""
    row = conn.execute(
        "SELECT id, energy_gwh, tenure_years, price_eur_mwh, technology, notes, publication_date "
        "FROM deals WHERE deal_hash = ?", (deal_hash,)
    ).fetchone()
    if not row:
        return None
    keys = ["id", "energy_gwh", "tenure_years", "price_eur_mwh", "technology", "notes", "publication_date"]
    return dict(zip(keys, row))


def classify_match(existing: dict, new_deal: dict, new_pub_date: str) -> str:
    """
    Given an existing DB row and a newly extracted deal for the same hash,
    returns 'update' if the new article adds meaningful information,
    or 'duplicate' if it's the same content from a different source.
    """
    enriching_fields = ["energy_gwh", "tenure_years", "price_eur_mwh", "technology"]
    for field in enriching_fields:
        existing_val = existing.get(field)
        new_val = new_deal.get(field)
        if new_val and not existing_val:
            return "update"

    # Longer notes = more information
    existing_notes = existing.get("notes") or ""
    new_notes = str(new_deal.get("notes") or "")
    if len(new_notes) > len(existing_notes) + 20:
        return "update"

    # Later publication date on a different source article
    existing_pub = (existing.get("publication_date") or "")[:10]
    if new_pub_date and existing_pub and new_pub_date > existing_pub:
        return "update"

    return "duplicate"

# ── Database Write ────────────────────────────────────────────────────────────

def write_deal(conn, extracted, real_url, article, full_text, match_type, canonical_id):
    """
    match_type: 'N' | 'U'
    Duplicates are skipped before reaching this function.
    """
    deal_hash = make_deal_hash(extracted)
    notes     = extracted.get("notes") or ""

    if match_type == "U":
        if extracted.get("update_clues"):
            notes = f"[UPDATE] {extracted['update_clues']} | {notes}".strip(" |")
        notes += f" | Original deal ID: {canonical_id}"
        deal_hash = deal_hash + f"_upd_{canonical_id}"

    conn.execute(
        """
        INSERT OR IGNORE INTO deals (
            deal_hash, event_type, canonical_id,
            date_agreement, date_found, buyer, seller,
            capacity_mw, energy_gwh, tenure_years, country, technology,
            price_eur_mwh, source_url, source_outlet, publication_date,
            notes, raw_snippet
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            deal_hash,
            match_type,
            canonical_id,
            extracted.get("date_agreement"),
            datetime.utcnow().strftime("%Y-%m-%d"),
            extracted.get("buyer"),
            extracted.get("seller"),
            extracted.get("capacity_mw"),
            extracted.get("energy_gwh"),
            extracted.get("tenure_years"),
            extracted.get("country"),
            extracted.get("technology"),
            extracted.get("price_eur_mwh"),
            real_url,
            article.get("source", {}).get("name"),
            (article.get("publishedAt") or "")[:10],
            notes,
            (full_text or article.get("description") or "")[:500],
        ),
    )
    conn.commit()

# ── CSV Export ────────────────────────────────────────────────────────────────

def export_csv(conn: sqlite3.Connection) -> None:
    rows = conn.execute("""
        SELECT id, event_type, canonical_id,
               date_agreement, date_found, buyer, seller,
               capacity_mw, energy_gwh, tenure_years, country, technology,
               price_eur_mwh, source_url, source_outlet, publication_date, notes
        FROM latest_deals
        ORDER BY date_found DESC, id DESC
    """).fetchall()

    headers = [
        "id", "event_type", "canonical_id",
        "date_agreement", "date_found", "buyer", "seller",
        "capacity_mw", "energy_gwh", "tenure_years", "country", "technology",
        "price_eur_mwh", "source_url", "source_outlet", "publication_date", "notes",
    ]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    log.info(f"CSV exported: {CSV_PATH} ({len(rows)} deals)")

# ── Extract full text from google news ─────────────────────────────────────────────────────────────

def resolve_google_news_url(url: str) -> str:
    """Decode Google News RSS URL to get the real article URL."""
    if not url or "news.google.com" not in url:
        return url
        
    try:
        decoded = gnewsdecoder(url)
        
        if decoded and decoded.get("status"):
            real_url = decoded.get("decoded_url")
            log.info(f"Resolved Google News URL: {real_url[:80]}")
            return real_url
            
        log.warning(f"URL decoding failed (status false): {url[:80]}")
        return url
        
    except Exception as e:
        log.warning(f"URL resolution failed ({e}): {url[:80]}")
        return url

# ── Email Notification ─────────────────────────────────────────────────────────

def send_log_email(log_path: Path, new_deals: int, updates: int) -> None:


    smtp_host     = os.environ["NOTIFY_SMTP_HOST"]
    smtp_port     = int(os.environ.get("NOTIFY_SMTP_PORT", 587))
    smtp_user     = os.environ["NOTIFY_SMTP_USER"]
    smtp_password = os.environ["NOTIFY_SMTP_PASSWORD"]
    to_addr       = os.environ["NOTIFY_EMAIL_TO"]

    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    subject  = f"[PPA Tracker] Daily run {run_date} — {new_deals} new, {updates} updates"

    try:
        log_content = log_path.read_text(encoding="utf-8")
    except Exception:
        log_content = "(log file not found)"

    msg = MIMEMultipart()
    msg["From"]    = smtp_user
    msg["To"]      = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(log_content, "plain", "utf-8"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, to_addr, msg.as_string())

    log.info(f"Run summary email sent to {to_addr}")

# ── Main Pipeline ─────────────────────────────────────────────────────────────

def run() -> None:
    log.info(f"=== PPA Tracker pipeline starting (env: {ENV_NAME}) ===")
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    from_date = SEARCH_FROM_DATE or (
        datetime.utcnow() - timedelta(days=int(LOOKBACK_DAYS or 2))
    ).strftime("%Y-%m-%d")
    to_date = SEARCH_TO_DATE
    
    log.info(f"Searching from {from_date} to {to_date or 'present'} with {LOOKBACK_DAYS} loockback days")

    # 1. Collect articles
    all_articles: list[dict] = []

    # NewsAPI (English)
    if not to_date:
        for query in [
            "PPA signed Europe renewable",
            "power purchase agreement signed Europe",
            "corporate PPA Europe signed deal",
        ]:
            all_articles.extend(fetch_newsapi(query, from_date, to_date))
            time.sleep(1)
    else:
        log.info("Skipping NewsAPI (to_date is set; Free plan does not support 'to' parameter).")
    
    # Google News RSS (all languages)
    for lang, query in GOOGLE_NEWS_FEEDS:
        all_articles.extend(fetch_google_news_rss(lang, query, from_date, to_date))
        time.sleep(2)

    log.info(f"Total raw articles collected: {len(all_articles)}")

    # Extra trade press RSS feeds
    for feed_url in EXTRA_RSS_FEEDS:
        all_articles.extend(fetch_extra_rss(feed_url, from_date, to_date))
        time.sleep(1)
    
    # 2. Deduplicate by URL
    seen_urls = set(
        row[0] for row in conn.execute("SELECT url FROM seen_urls").fetchall()
    )
    unique_articles = []
    for a in all_articles:
        url = a.get("url", "").strip()
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(a)

    log.info(f"Unique unseen articles: {len(unique_articles)}")

    # 3. Process each article through LLM
    new_deals = 0
    updates   = 0
    processed = 0

    for article in unique_articles:
        if processed >= MAX_ARTICLES:
            log.info(f"Reached MAX_ARTICLES limit ({MAX_ARTICLES}) — stopping")
            break
        
        url     = article.get("url", "")
        title   = article.get("title", "")
        snippet = article.get("description", "")
        outlet  = article.get("source", {}).get("name", "")

        log.info(f"------- Processing article n. {processed}: {title[:80]} -------")

        # combined = (title + " " + snippet).lower()
        # if not any(kw in combined for kw in ["ppa", "power purchase", "purchase agreement"]):
        #     log.info(f"Pre-filter skipped: {title[:60]}")
        #     continue
        
        # Try full text; fall back to title + snippet
        real_url            = resolve_google_news_url(url)
        log.info(f"URL: {real_url}")
        full_text           = fetch_full_text(real_url) if real_url else None
        text_for_extraction = full_text or f"{title}\n\n{snippet}"

        log.info(f"Text length: {len(text_for_extraction)} chars | source: {'full' if full_text else 'fallback'}")
        log.info(f"Text preview: {text_for_extraction[:200]}...")

        if len(text_for_extraction.strip()) < 50:  # Skip if text is too short
            log.info(f"Text too short ({len(text_for_extraction)} chars) — skipping: {title[:60]}")
            continue
        
        if not text_for_extraction.strip():
            log.warning(f"No text to extract for: {url[:80]}")
            continue

        extracted = extract_with_llm(text_for_extraction, title, outlet)

        if extracted is None:
            log.warning(f"LLM returned None — skipping: {title[:60]}")
            continue

        # Parse as array (handle both single object and array for backward compatibility)
        try:
            deals = extracted if isinstance(extracted, list) else [extracted]
        except Exception as e:
            log.warning(f"Failed to parse LLM response ({e}) — skipping: {title[:60]}")
            continue

        # Mark URL as seen (only once per article)
        conn.execute(
            "INSERT OR IGNORE INTO seen_urls VALUES (?, ?)",
            (url, datetime.utcnow().strftime("%Y-%m-%d"))
        )
        conn.commit()
        seen_urls.add(url)

        # Process each deal separately
        for deal in deals:
            # Skip non-signed deals (but still log them)
            if not deal.get("is_signed_deal"):
                log.info(f"Not a signed deal — skipping: {title[:60]}")
                continue
            
            if not is_european_deal(deal):
                log.info(f"Not a European deal — skipping: {deal.get('buyer')} / {deal.get('seller')} "
                         f"({deal.get('country')}, {deal.get('capacity_mw')} MW)")
                continue
        
            if (
                deal.get("confidence") == "low"
                and not deal.get("buyer")
                and not deal.get("seller")
            ):
                log.info(f"Low confidence, no parties — skipping: {deal.get('buyer')} / {deal.get('seller')} "
                f"({deal.get('country')}, {deal.get('capacity_mw')} MW)")
                continue
        
            deal_hash   = make_deal_hash(deal)
            existing    = find_duplicate(conn, deal_hash)
            new_pub     = (article.get("publishedAt") or "")[:10]

            if existing:
                match_type  = classify_match(existing, deal, new_pub)
                canonical_id = existing["id"]
            elif deal.get("is_likely_update", False):
                match_type  = "U"
                canonical_id = None
            else:
                match_type  = "N"
                canonical_id = None

            if match_type == "D":
                log.info(
                    f"DUPLICATE skipped: {deal.get('buyer')} / {deal.get('seller')} "
                    f"({deal.get('country')}) — same as ID: {canonical_id}"
                )
                continue

            log.info(
                f"{'NEW' if match_type == 'N' else 'UPDATE'}: "
                f"{deal.get('buyer')} / {deal.get('seller')} "
                f"({deal.get('country')}, {deal.get('capacity_mw')} MW)"
                + (f" — canonical ID: {canonical_id}" if canonical_id else "")
            )

            write_deal(conn, deal, real_url, article, full_text, match_type, canonical_id)
            processed += 1

            if match_type == "U":
                updates += 1
            else:
                new_deals += 1

            time.sleep(5)  # LLM free tier: stay well within rate limits
    
    log.info(f"Run complete. New deals: {new_deals}, Updates: {updates}")
    export_csv(conn)

    if NOTIFY_EMAIL_ENABLED:
        try:
            send_log_email(LOG_PATH, new_deals, updates)
        except Exception as e:
            log.error(f"Failed to send email: {e}")
    
    conn.close()


if __name__ == "__main__":
    run()
