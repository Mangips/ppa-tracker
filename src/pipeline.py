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

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_PATH = DATA_DIR / "pipeline.log"

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
LOOKBACK_DAYS    = 2
SEARCH_FROM_DATE = os.environ.get("SEARCH_FROM_DATE")

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

def fetch_newsapi(query: str, from_date: str) -> list[dict]:
    try:
        resp = requests.get(
            NEWSAPI_URL,
            params={
                "q":        query,
                "from":     from_date,
                "sortBy":   "publishedAt",
                "pageSize": 100,
                "apiKey":   NEWSAPI_KEY,
            },
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


def fetch_google_news_rss(lang: str, query: str) -> list[dict]:
    """Fetch articles from Google News RSS. No API key required."""
    hl, gl, ceid = LANG_TO_CEID.get(lang, ("en", "US", "US:en"))
    url = (
        f"https://news.google.com/rss/search"
        f"?q={requests.utils.quote(query)}"
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
            link    = (item.findtext("link")    or "").strip()
            pub     = (item.findtext("pubDate") or "").strip()
            source_el = item.find("source")
            outlet  = source_el.text if source_el is not None else ""
            # pubDate is like "Mon, 19 May 2026 10:00:00 GMT"
            try:
                pub_dt  = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z")
                pub_iso = pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                pub_iso = pub

            articles.append({
                "title":       title,
                "url":         link,
                "publishedAt": pub_iso,
                "source":      {"name": outlet},
                "description": title,  # RSS rarely has a snippet; title is enough
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
        self._skip  = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "nav", "footer", "header"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style", "nav", "footer", "header"):
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            s = data.strip()
            if s:
                self.chunks.append(s)


def fetch_full_text(url: str) -> str | None:
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=12)
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

Analyze the text below and answer:
1. Does it describe a SIGNED/COMPLETED PPA deal? (Not a rumour, tender, or proposal.)
2. If yes, extract all available fields.
3. Does it look like an UPDATE to a deal reported earlier?

Return ONLY a JSON object — no markdown fences, no explanation, nothing else:
{{
  "is_signed_deal":   true or false,
  "is_likely_update": true or false,
  "update_clues":     "what changed, or null",
  "date_agreement":   "YYYY-MM-DD, YYYY-MM, or YYYY — null if unknown",
  "buyer":            "offtaker company name(s), comma-separated",
  "seller":           "developer / generator / IPP name(s)",
  "capacity_mw":      number or null,
  "energy_gwh":       number or null,
  "tenure_years":     number or null,
  "country":          "delivery country",
  "technology":       "solar / wind onshore / wind offshore / hydro / mixed / other",
  "price_eur_mwh":    number or null,
  "notes":            "project name, grid details, special terms, or null",
  "confidence":       "high / medium / low"
}}

Text (any language — return all fields in English):
---
{text}
---"""


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
        # Google News RSS <link> tags contain the real URL in the <guid> tag
        # but we only have the link here. Use the decoding endpoint instead.
        resp = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                )
            },
            timeout=10,
            allow_redirects=True,
        )
        # Look for the real URL in the response HTML
        import re
        match = re.search(r'<a href="(https?://(?!news\.google\.com)[^"]+)"', resp.text)
        if match:
            real = match.group(1)
            log.info(f"Resolved Google News URL: {real[:80]}")
            return real
        return url
    except Exception as e:
        log.warning(f"URL resolution failed ({e}): {url[:80]}")
        return url

# ── Main Pipeline ─────────────────────────────────────────────────────────────

def run() -> None:
    log.info("=== PPA Tracker pipeline starting ===")
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    from_date = SEARCH_FROM_DATE or (
        datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")
    log.info(f"Searching from {from_date}")

    # 1. Collect articles
    all_articles: list[dict] = []

    # NewsAPI (English)
    for query in [
        "PPA signed Europe renewable",
        "power purchase agreement signed Europe",
        "corporate PPA Europe signed deal",
    ]:
        all_articles.extend(fetch_newsapi(query, from_date))
        time.sleep(1)

    # Google News RSS (all languages)
    for lang, query in GOOGLE_NEWS_FEEDS:
        all_articles.extend(fetch_google_news_rss(lang, query))
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

    for article in unique_articles:
        url     = article.get("url", "")
        title   = article.get("title", "")
        snippet = article.get("description", "")
        outlet  = article.get("source", {}).get("name", "")

        log.info(f"Processing: {title[:80]}")

        # combined = (title + " " + snippet).lower()
        # if not any(kw in combined for kw in ["ppa", "power purchase", "purchase agreement"]):
        #     log.info(f"Pre-filter skipped: {title[:60]}")
        #     continue
        
        # Try full text; fall back to title + snippet
        real_url            = resolve_google_news_url(url)
        full_text           = fetch_full_text(real_url) if real_url else None
        text_for_extraction = full_text or f"{title}\n\n{snippet}"

        log.info(f"Text length: {len(text_for_extraction)} chars | source: {'full' if full_text else 'snippet'}")
        log.info(f"Text preview: {text_for_extraction[:200]}")
        
        if not text_for_extraction.strip():
            log.warning(f"No text to extract for: {url[:80]}")
            continue

        extracted = extract_with_mistral(text_for_extraction, title, outlet)

        if extracted is None:
            log.warning(f"Mistral returned None — skipping: {title[:60]}")
            continue

        # Mark as seen only after a valid Mistral response (not on API errors)
        conn.execute(
            "INSERT OR IGNORE INTO seen_urls VALUES (?, ?)",
            (url, datetime.utcnow().strftime("%Y-%m-%d"))
        )
        conn.commit()

        if not extracted.get("is_signed_deal"):
            log.info(f"Not a signed deal — skipping: {title[:60]}")
            continue

        if (
            extracted.get("confidence") == "low"
            and not extracted.get("buyer")
            and not extracted.get("seller")
        ):
            log.info(f"Low confidence, no parties — skipping: {title[:60]}")
            continue

        deal_hash   = make_deal_hash(extracted)
        existing_id = find_duplicate(conn, deal_hash)
        is_update   = existing_id is not None or extracted.get("is_likely_update", False)

        write_deal(conn, extracted, article, full_text, is_update, existing_id)

        if is_update:
            updates += 1
            log.info(
                f"UPDATE recorded: {extracted.get('buyer')} / "
                f"{extracted.get('seller')} ({extracted.get('country')})"
            )
        else:
            new_deals += 1
            log.info(
                f"NEW deal: {extracted.get('buyer')} / "
                f"{extracted.get('seller')} ({extracted.get('country')}, "
                f"{extracted.get('capacity_mw')} MW)"
            )

        time.sleep(5)  # Mistral free tier: stay well within rate limits

    log.info(f"Run complete. New deals: {new_deals}, Updates: {updates}")
    export_csv(conn)
    conn.close()


if __name__ == "__main__":
    run()
