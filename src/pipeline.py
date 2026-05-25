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

# ── Config ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "ppa_deals.db"
CSV_PATH = DATA_DIR / "ppa_deals.csv"

NEWSAPI_KEY = os.environ["NEWSAPI_KEY"]
NEWSAPI_URL = "https://newsapi.org/v2/everything"

MISTRAL_KEY   = os.environ["MISTRAL_KEY"]
MISTRAL_URL   = "https://api.mistral.ai/v1/chat/completions"
MISTRAL_MODEL = os.environ.get("MISTRAL_MODEL", "mistral-small-latest")

MAX_ARTICLES = int(os.environ.get("MAX_ARTICLES") or 100000)  # Default: no limit

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = DATA_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
ENV_NAME = os.environ.get("GITHUB_ENVIRONMENT", "local")
LOG_PATH = LOGS_DIR / f"pipeline_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_{ENV_NAME}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Override with env var for testing, e.g. SEARCH_FROM_DATE=2026-01-01
LOOKBACK_DAYS = os.environ.get("LOOKBACK_DAYS")
SEARCH_FROM_DATE = os.environ.get("SEARCH_FROM_DATE")
SEARCH_TO_DATE = os.environ.get("SEARCH_TO_DATE")

# Google News RSS: one query per language.
# hl = UI language, gl = country, ceid = region:language
GOOGLE_NEWS_FEEDS = [
    # English
    ("en", "PPA signed Europe renewable energy"),
    ("en", "power purchase agreement signed Europe"),
    # German
    ("de", "PPA unterzeichnet Europa erneuerbare Energie"),
    # French
    ("fr", "PPA signé Europe énergie renouvelable"),
    # Spanish
    ("es", "PPA firmado Europa energía renovable"),
    # Italian
    ("it", "PPA firmato Europa energia rinnovabile"),
    # Polish
    ("pl", "PPA podpisany Europa energia odnawialna"),
    # Dutch
    ("nl", "PPA ondertekend Europa hernieuwbare energie"),
    # Portuguese
    ("pt", "PPA assinado Europa energia renovável"),
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


# ── Database ──────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS deals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_hash        TEXT UNIQUE,
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
            is_update        INTEGER DEFAULT 0,
            original_deal_id INTEGER,
            raw_snippet      TEXT
        );
        CREATE TABLE IF NOT EXISTS seen_urls (
            url     TEXT PRIMARY KEY,
            seen_at TEXT
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
            
        log.info(f"Google News RSS [{lang}] '{query}': {len(articles)} results")
        return articles
    except Exception as e:
        log.warning(f"Google News RSS error [{lang}] '{query}': {e}")
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

# ── Mistral Extraction ─────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """\
You are an expert energy analyst. Extract structured information about Power Purchase Agreement (PPA) deals.

Analyze the text below and:
1. Identify **ALL SIGNED/COMPLETED PPA deals** described (not rumours, tenders, or proposals).
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


def extract_with_mistral(text: str, title: str, outlet: str) -> dict | None:
    prompt  = EXTRACTION_PROMPT.format(text=text[:6000])
    payload = {
        "model":       MISTRAL_MODEL,
        "temperature": 0.1,
        "messages":    [{"role": "user", "content": prompt}],
    }
    try:
        resp = requests.post(
            MISTRAL_URL,
            headers={
                "Authorization": f"Bearer {MISTRAL_KEY}",
                "Content-Type":  "application/json",
            },
            json=payload,
            timeout=30,
        )
        log.info(f"Mistral HTTP {resp.status_code} for: {title[:60]}")
        if resp.status_code == 429:
            log.warning(f"Mistral 429 — waiting 60s: {title[:50]}")
            time.sleep(60)
            return None  # will be retried next run since URL won't be marked seen
        if resp.status_code != 200:
            log.warning(f"Mistral error body: {resp.text[:300]}")
            return None

        content = resp.json()["choices"][0]["message"]["content"].strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        parsed = json.loads(content)
        log.info(
            f"Mistral extracted — signed={parsed.get('is_signed_deal')} "
            f"european={parsed.get('is_european')} "
            f"confidence={parsed.get('confidence')} "
            f"buyer={parsed.get('buyer')} seller={parsed.get('seller')} "
            f"| {title[:50]}"
        )
        return parsed

    except json.JSONDecodeError as e:
        log.warning(f"Mistral JSON parse error ({outlet}): {e} | raw: {content[:200]}")
        return None
    except Exception as e:
        log.warning(f"Mistral call failed ({outlet}): {e}")
        return None

# ── Deduplication ─────────────────────────────────────────────────────────────

def make_deal_hash(extracted: dict) -> str:
    parts = [
        (extracted.get("buyer")   or "").lower().strip(),
        (extracted.get("seller")  or "").lower().strip(),
        (extracted.get("country") or "").lower().strip(),
        str(round(extracted.get("capacity_mw") or 0, -1)),
        (extracted.get("date_agreement") or "")[:7],
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def find_duplicate(conn: sqlite3.Connection, deal_hash: str) -> int | None:
    row = conn.execute(
        "SELECT id FROM deals WHERE deal_hash = ?", (deal_hash,)
    ).fetchone()
    return row[0] if row else None


# ── Database Write ────────────────────────────────────────────────────────────

def write_deal(conn, extracted, article, full_text, is_update, original_id):
    deal_hash = make_deal_hash(extracted)
    notes     = extracted.get("notes") or ""
    if is_update and extracted.get("update_clues"):
        notes = f"[UPDATE] {extracted['update_clues']} | {notes}".strip(" |")
        if original_id:
            notes += f" | Original deal id: {original_id}"

    conn.execute(
        """
        INSERT OR IGNORE INTO deals (
            deal_hash, date_agreement, date_found, buyer, seller,
            capacity_mw, energy_gwh, tenure_years, country, technology,
            price_eur_mwh, source_url, source_outlet, publication_date,
            notes, is_update, original_deal_id, raw_snippet
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            deal_hash,
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
            article.get("url"),
            article.get("source", {}).get("name"),
            (article.get("publishedAt") or "")[:10],
            notes,
            1 if is_update else 0,
            original_id,
            (full_text or article.get("description") or "")[:500],
        ),
    )
    conn.commit()


# ── CSV Export ────────────────────────────────────────────────────────────────

def export_csv(conn: sqlite3.Connection) -> None:
    rows = conn.execute("""
        SELECT id, date_agreement, date_found, buyer, seller,
               capacity_mw, energy_gwh, tenure_years, country, technology,
               price_eur_mwh, source_url, source_outlet, publication_date,
               notes, is_update, original_deal_id
        FROM deals
        ORDER BY date_found DESC, id DESC
    """).fetchall()

    headers = [
        "id", "date_agreement", "date_found", "buyer", "seller",
        "capacity_mw", "energy_gwh", "tenure_years", "country", "technology",
        "price_eur_mwh", "source_url", "source_outlet", "publication_date",
        "notes", "is_update", "original_deal_id",
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

    # 3. Process each article through Mistral
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

        extracted = extract_with_mistral(text_for_extraction, title, outlet)

        if extracted is None:
            log.warning(f"Mistral returned None — skipping: {title[:60]}")
            continue

        # Parse as array (handle both single object and array for backward compatibility)
        try:
            deals = extracted if isinstance(extracted, list) else [extracted]
        except Exception as e:
            log.warning(f"Failed to parse Mistral response ({e}) — skipping: {title[:60]}")
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
            
            if not extracted.get("is_european"):
                log.info(f"Not a European deal — skipping: {title[:60]}")
                continue
        
            if (
                deal.get("confidence") == "low"
                and not deal.get("buyer")
                and not deal.get("seller")
            ):
                log.info(f"Low confidence, no parties — skipping: {title[:60]}")
                continue
        
            deal_hash = make_deal_hash(deal)
            existing_id = find_duplicate(conn, deal_hash)
            is_update = existing_id is not None or deal.get("is_likely_update", False)
        
            write_deal(conn, deal, article, full_text, is_update, existing_id)
            processed += 1
        
            if is_update:
                updates += 1
                log.info(f"UPDATE recorded: {deal.get('buyer')} / {deal.get('seller')} ({deal.get('country')})")
            else:
                new_deals += 1
                log.info(
                    f"NEW deal: {deal.get('buyer')} / {deal.get('seller')} "
                    f"({deal.get('country')}, {deal.get('capacity_mw')} MW)"
                )
        
            time.sleep(5)  # Mistral free tier: stay well within rate limits
    
    log.info(f"Run complete. New deals: {new_deals}, Updates: {updates}")
    export_csv(conn)
    conn.close()


if __name__ == "__main__":
    run()
