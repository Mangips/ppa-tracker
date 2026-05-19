"""
PPA Deal Tracker - Main Pipeline
Runs daily: fetches news, extracts deals, deduplicates, writes to SQLite + CSV.
"""

import os
import sys
import json
import time
import logging
import sqlite3
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "ppa_deals.db"
CSV_PATH = DATA_DIR / "ppa_deals.csv"

NEWSAPI_KEY = os.environ["NEWSAPI_KEY"]
GEMINI_KEY = os.environ["GEMINI_KEY"]

NEWSAPI_URL = "https://newsapi.org/v2/everything"
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-1.5-flash:generateContent"
)

# Search queries per language.
# PPA is widely used as-is; we add local-language synonyms where meaningful.
SEARCH_QUERIES = {
    "en": ["PPA signed Europe", "power purchase agreement signed Europe", "corporate PPA deal Europe"],
    "de": ["PPA unterzeichnet Europa", "Stromabnahmevertrag unterzeichnet"],
    "fr": ["PPA signé Europe", "accord achat énergie signé"],
    "es": ["PPA firmado Europa", "contrato compra energía firmado"],
    "it": ["PPA firmato Europa", "contratto acquisto energia firmato"],
    "pl": ["PPA podpisany Europa", "umowa zakupu energii podpisana"],
    "nl": ["PPA ondertekend Europa", "energieafnameovereenkomst ondertekend"],
    "pt": ["PPA assinado Europa", "contrato compra energia assinado"],
}

SEARCH_FROM_DATE = "2026-05-07" # hardcoded for testing purposes

# How many days back to search (overlap intentional to catch delayed indexing)
LOOKBACK_DAYS = 2
SEARCH_FROM_DATE = os.environ.get("SEARCH_FROM_DATE")  # e.g. "2026-01-01"


# ── Database ──────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS deals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_hash       TEXT UNIQUE,          -- dedup key
            date_agreement  TEXT,                 -- date of the deal itself
            date_found      TEXT,                 -- date our tool found it
            buyer           TEXT,
            seller          TEXT,
            capacity_mw     REAL,
            energy_gwh      REAL,
            tenure_years    REAL,
            country         TEXT,
            technology      TEXT,
            price_eur_mwh   REAL,
            source_url      TEXT,
            source_outlet   TEXT,
            publication_date TEXT,
            notes           TEXT,
            is_update       INTEGER DEFAULT 0,    -- 1 if this row is an update
            original_deal_id INTEGER,             -- FK to deals.id if is_update=1
            raw_snippet     TEXT
        );

        CREATE TABLE IF NOT EXISTS seen_urls (
            url     TEXT PRIMARY KEY,
            seen_at TEXT
        );
    """)
    conn.commit()


# ── News Fetching ─────────────────────────────────────────────────────────────

def fetch_newsapi(query: str, from_date: str) -> list[dict]:
    """Fetch articles from NewsAPI for a given query."""
    try:
        resp = requests.get(
            NEWSAPI_URL,
            params={
                "q": query,
                "from": from_date,
                "sortBy": "publishedAt",
                "pageSize": 100,
                "apiKey": NEWSAPI_KEY,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        articles = data.get("articles", [])
        log.info(f"NewsAPI '{query}': {len(articles)} results")
        return articles
    except Exception as e:
        log.warning(f"NewsAPI error for '{query}': {e}")
        return []


def fetch_gdelt(query: str, from_date: str) -> list[dict]:
    for attempt in range(3):
        try:
            resp = requests.get(
                GDELT_URL,
                params={
                    "query": query,
                    "mode": "artlist",
                    "maxrecords": 75,
                    "startdatetime": from_date.replace("-", "") + "000000",
                    "format": "json",
                },
                timeout=20,
            )
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                log.warning(f"GDELT 429, waiting {wait}s before retry...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("articles", [])
            log.info(f"GDELT '{query}': {len(articles)} results")
            return [
                {
                    "title": a.get("title", ""),
                    "url": a.get("url", ""),
                    "publishedAt": a.get("seendate", ""),
                    "source": {"name": a.get("domain", "")},
                    "description": a.get("title", ""),
                }
                for a in articles
            ]
        except Exception as e:
            log.warning(f"GDELT error for '{query}': {e}")
            time.sleep(30)
    return []


def fetch_full_text(url: str) -> str | None:
    """Attempt to fetch full article text. Returns None if blocked/error."""
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            # Rough text extraction: strip tags
            from html.parser import HTMLParser

            class _TextExtractor(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.chunks = []
                    self._skip = False

                def handle_starttag(self, tag, attrs):
                    if tag in ("script", "style", "nav", "footer", "header"):
                        self._skip = True

                def handle_endtag(self, tag):
                    if tag in ("script", "style", "nav", "footer", "header"):
                        self._skip = False

                def handle_data(self, data):
                    if not self._skip:
                        stripped = data.strip()
                        if stripped:
                            self.chunks.append(stripped)

            parser = _TextExtractor()
            parser.feed(resp.text)
            text = " ".join(parser.chunks)
            # Truncate to ~4000 words to stay within Gemini context
            words = text.split()
            return " ".join(words[:4000]) if len(words) > 4000 else text
        return None
    except Exception:
        return None


# ── Gemini Extraction ─────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """You are an expert analyst extracting information about Power Purchase Agreement (PPA) deals from news articles.

Analyze the following article and determine:
1. Does this article describe a SIGNED/COMPLETED PPA deal (not a rumor, proposal, or tender)?
2. If yes, extract all available structured information.
3. If this looks like an UPDATE to a previously reported deal, note that clearly.

Return ONLY a JSON object with exactly these fields (use null for missing values):
{{
  "is_signed_deal": true or false,
  "is_likely_update": true or false,
  "update_clues": "string describing what changed if is_likely_update is true, else null",
  "date_agreement": "YYYY-MM-DD or YYYY-MM or YYYY if partial, null if unknown",
  "buyer": "company name(s), comma separated if multiple",
  "seller": "company name(s) / developer / IPP",
  "capacity_mw": number or null,
  "energy_gwh": number or null,
  "tenure_years": number or null,
  "country": "country where the energy will be delivered",
  "technology": "solar / wind onshore / wind offshore / hydro / mixed / other",
  "price_eur_mwh": number or null,
  "notes": "any other relevant detail: project name, grid connection, special terms, etc.",
  "confidence": "high / medium / low"
}}

Article text (may be in any language - extract and return fields in English):
---
{text}
---

Return ONLY the JSON object, no markdown, no explanation."""


def extract_with_gemini(text: str, outlet: str) -> dict | None:
    """Call Gemini Flash to extract deal fields from article text."""
    prompt = EXTRACTION_PROMPT.format(text=text[:6000])
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 1000},
    }
    try:
        resp = requests.post(
            GEMINI_URL,
            params={"key": GEMINI_KEY},
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()
        content = raw["candidates"][0]["content"]["parts"][0]["text"].strip()
        # Strip markdown fences if present
        if content.startswith("```"):
            content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        return json.loads(content)
    except json.JSONDecodeError as e:
        log.warning(f"Gemini JSON parse error ({outlet}): {e}")
        return None
    except Exception as e:
        log.warning(f"Gemini API error ({outlet}): {e}")
        return None


# ── Deduplication ─────────────────────────────────────────────────────────────

def make_deal_hash(extracted: dict) -> str:
    """
    Create a stable hash from the core deal identity.
    Buyer + seller + country + approximate capacity is usually unique enough.
    """
    parts = [
        (extracted.get("buyer") or "").lower().strip(),
        (extracted.get("seller") or "").lower().strip(),
        (extracted.get("country") or "").lower().strip(),
        str(round(extracted.get("capacity_mw") or 0, -1)),  # round to 10 MW
        (extracted.get("date_agreement") or "")[:7],         # year-month
    ]
    key = "|".join(parts)
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def find_duplicate(conn: sqlite3.Connection, deal_hash: str) -> int | None:
    """Return existing deal id if hash matches, else None."""
    row = conn.execute(
        "SELECT id FROM deals WHERE deal_hash = ?", (deal_hash,)
    ).fetchone()
    return row[0] if row else None


# ── Database Write ────────────────────────────────────────────────────────────

def write_deal(
    conn: sqlite3.Connection,
    extracted: dict,
    article: dict,
    full_text: str | None,
    is_update: bool,
    original_id: int | None,
) -> None:
    deal_hash = make_deal_hash(extracted)
    notes = extracted.get("notes") or ""
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
            article.get("publishedAt", "")[:10],
            notes,
            1 if is_update else 0,
            original_id,
            (full_text or article.get("description") or "")[:500],
        ),
    )
    conn.commit()


# ── CSV Export ────────────────────────────────────────────────────────────────

def export_csv(conn: sqlite3.Connection) -> None:
    import csv

    rows = conn.execute("""
        SELECT
            id, date_agreement, date_found, buyer, seller,
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


# ── Main Pipeline ─────────────────────────────────────────────────────────────

def run() -> None:
    log.info("=== PPA Tracker pipeline starting ===")
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    from_date = SEARCH_FROM_DATE or (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    log.info(f"Searching from {from_date}")

    # 1. Collect all articles
    all_articles: list[dict] = []

    # NewsAPI - English queries only
    for query in SEARCH_QUERIES["en"]:
        all_articles.extend(fetch_newsapi(query, from_date))
        time.sleep(0.5)

    # GDELT - all languages
    for lang, queries in SEARCH_QUERIES.items():
        for query in queries:
            all_articles.extend(fetch_gdelt(query, from_date))
            time.sleep(5)

    log.info(f"Total raw articles collected: {len(all_articles)}")

    # 2. Deduplicate by URL
    seen_urls = set(
        row[0] for row in conn.execute("SELECT url FROM seen_urls").fetchall()
    )
    unique_articles = []
    for a in all_articles:
        url = a.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(a)

    log.info(f"Unique unseen articles: {len(unique_articles)}")

    # Mark URLs as seen immediately
    conn.executemany(
        "INSERT OR IGNORE INTO seen_urls VALUES (?, ?)",
        [(a["url"], datetime.utcnow().strftime("%Y-%m-%d")) for a in unique_articles if a.get("url")],
    )
    conn.commit()

    # 3. Process each article
    new_deals = 0
    updates = 0

    for article in unique_articles:
        url = article.get("url", "")
        title = article.get("title", "")
        snippet = article.get("description", "")
        outlet = article.get("source", {}).get("name", "")

        # Quick pre-filter: skip if title/snippet has no PPA signal
        combined = (title + " " + snippet).lower()
        if not any(kw in combined for kw in ["ppa", "power purchase", "purchase agreement", "offtake", "stromabnahme", "achat énergie", "compra energía", "acquisto energia"]):
            continue

        # Try full text, fall back to snippet
        full_text = fetch_full_text(url) if url else None
        text_for_extraction = full_text or f"{title}\n\n{snippet}"

        # Gemini extraction
        extracted = extract_with_gemini(text_for_extraction, outlet)
        if not extracted:
            continue

        if not extracted.get("is_signed_deal"):
            log.info(f"Skipped (not signed): {title[:60]}")
            continue

        if extracted.get("confidence") == "low" and not extracted.get("buyer") and not extracted.get("seller"):
            log.info(f"Skipped (low confidence, no parties): {title[:60]}")
            continue

        # Deduplication
        deal_hash = make_deal_hash(extracted)
        existing_id = find_duplicate(conn, deal_hash)
        is_update = existing_id is not None or extracted.get("is_likely_update", False)

        write_deal(conn, extracted, article, full_text, is_update, existing_id)

        if is_update:
            updates += 1
            log.info(f"UPDATE recorded: {extracted.get('buyer')} / {extracted.get('seller')} ({extracted.get('country')})")
        else:
            new_deals += 1
            log.info(f"NEW deal: {extracted.get('buyer')} / {extracted.get('seller')} ({extracted.get('country')}, {extracted.get('capacity_mw')} MW)")

        # Gemini free tier: be gentle
        time.sleep(1.5)

    log.info(f"Run complete. New deals: {new_deals}, Updates: {updates}")

    # 4. Export CSV
    export_csv(conn)
    conn.close()


if __name__ == "__main__":
    run()
