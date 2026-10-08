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
from pathlib import Path

import requests
import trafilatura
from rapidfuzz import fuzz

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

# ── LLM Providers ─────────────────────────────────────────────────────────────
LLM_PROVIDERS = [
    {
        "name": "groq-120b",
        "provider": "groq",
        "key": os.environ.get("GROQ_KEY"),
        "url": "https://api.groq.com/openai/v1/chat/completions",
        "model": os.environ.get("GROQ_MODEL", "openai/gpt-oss-120b"),
        "min_delay": 15,
        "timeout": 30,
    },
    {
        "name": "groq-qwen",
        "provider": "groq",
        "key": os.environ.get("GROQ_KEY"),
        "url": "https://api.groq.com/openai/v1/chat/completions",
        "model": "qwen/qwen3.8-27b",
        "min_delay": 15,
        "timeout": 30,
    },
    {
        "name": "groq-20b",
        "provider": "groq",
        "key": os.environ.get("GROQ_KEY"),
        "url": "https://api.groq.com/openai/v1/chat/completions",
        "model": "openai/gpt-oss-20b",
        "min_delay": 15,
        "timeout": 30,
    },
    {
        "name": "gemini-31",
        "provider": "gemini",
        "key": os.environ.get("GEMINI_KEY"),
        "model": "gemini-3.1-flash-lite",
        "min_delay": 5,
        "timeout": 20,
    },
    {
        "name": "gemini-35",
        "provider": "gemini",
        "key": os.environ.get("GEMINI_KEY"),
        "model": "gemini-3.5-flash-lite",
        "min_delay": 5,
        "timeout": 20,
    },
    {
        "name": "gemma-26b",
        "provider": "gemini",
        "key": os.environ.get("GEMINI_KEY"),
        "model": "gemma-4-26b-a4b-it",
        "min_delay": 5,
        "timeout": 20,
    },
    {
        "name": "gemma-31b",
        "provider": "gemini",
        "key": os.environ.get("GEMINI_KEY"),
        "model": "gemma-4-31b-it",
        "min_delay": 5,
        "timeout": 20,
    },
    {
        "name": "openrouter-qwen",
        "provider": "openrouter",
        "key": os.environ.get("OPENROUTER_KEY"),
        "url": "https://openrouter.ai/api/v1/chat/completions",
        "model": "qwen/qwen3.8-27b:free",
        "min_delay": 5,
        "timeout": 30,
    },
    {
        "name": "openrouter-gpt20b",
        "provider": "openrouter",
        "key": os.environ.get("OPENROUTER_KEY"),
        "url": "https://openrouter.ai/api/v1/chat/completions",
        "model": "openai/gpt-oss-20b:free",
        "min_delay": 5,
        "timeout": 30,
    },
    {
        "name": "openrouter-free",
        "provider": "openrouter",
        "key": os.environ.get("OPENROUTER_KEY"),
        "url": "https://openrouter.ai/api/v1/chat/completions",
        "model": "openrouter/free",
        "min_delay": 5,
        "timeout": 30,
    },
    {
        "name": "mistral-small",
        "provider": "mistral",
        "key": os.environ.get("MISTRAL_KEY"),
        "url": "https://api.mistral.ai/v1/chat/completions",
        "model": "mistral-small-latest",
        "min_delay": 10,
        "timeout": 20,
    },
    {
        "name": "mistral-8b",
        "provider": "mistral",
        "key": os.environ.get("MISTRAL_KEY"),
        "url": "https://api.mistral.ai/v1/chat/completions",
        "model": "ministral-8b-latest",
        "min_delay": 10,
        "timeout": 20,
    },
    {
        "name": "mistral-3b",
        "provider": "mistral",
        "key": os.environ.get("MISTRAL_KEY"),
        "url": "https://api.mistral.ai/v1/chat/completions",
        "model": "ministral-3b-latest",
        "min_delay": 10,
        "timeout": 20,
    },
]

disabled_llm_providers = set()
last_llm_request = {}

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
    # English — two strong queries
    ("en", "power purchase agreement signed Europe"),
    ("en", "corporate PPA Europe renewable energy"),
    # German — natural phrasing
    ("de", "Stromliefervertrag unterzeichnet"),
    ("de", "PPA erneuerbare Energie Deutschland"),
    # French
    ("fr", "contrat d'achat d'électricité signé"),
    ("fr", "PPA énergie renouvelable France"),
    # Spanish
    ("es", "acuerdo de compra de energía firmado"),
    ("es", "PPA energía renovable España"),
    # Italian
    ("it", "accordo di acquisto energia rinnovabile"),
    ("it", "PPA firmato energia rinnovabile Italia"),
    # Polish
    ("pl", "umowa zakupu energii podpisana"),
    # Dutch
    ("nl", "stroomafnameovereenkomst ondertekend"),
    ("nl", "PPA hernieuwbare energie Nederland"),
    # Portuguese
    ("pt", "acordo de compra de energia assinado"),
    ("pt", "PPA energia renovável Portugal"),
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

def fetch_full_text(url: str) -> str | None:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        resp = requests.get(url, headers=headers, timeout=12, allow_redirects=True)

        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            # trafilatura isolates the main article body and drops boilerplate
            # (related-article teasers, nav, ads) far more reliably than a
            # manual tag-skip parser — favor_precision trims aggressively
            # rather than risk pulling in unrelated headline lists.
            text = trafilatura.extract(
                resp.text,
                url=url,
                include_comments=False,
                include_tables=False,
                favor_precision=True,
            )
            if not text:
                log.info(f"Full text extraction empty: {url[:80]}")
                return None
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
3. Return ONE top-level JSON object containing a "deals" array.
4. If NO signed deals are found, return:
{{"deals":[{{"is_signed_deal":false,...}}]}}
5. If signed, `is_european` must reflect where the ENERGY IS DELIVERED, not where the companies are based.
6. If an article describes multiple individual deals, extract EACH separately with its own capacity. Do NOT also extract an aggregate/summary entry. If you cannot determine the capacity of an individual deal, use null — but never create a summary row that combines multiple deals into one.
7. Distinguish a PPA deal from an M&A / asset transaction. A signed PPA deal is a NEW offtake contract in which a buyer agrees to purchase electricity, capacity, or certificates from a seller under specific terms (price, volume, or tenure). It is NOT a plant/portfolio acquisition, divestment, financing round, refinancing, or equity/company sale — even if the acquired asset already has a PPA attached, and even if the article mentions "PPA" and a capacity in MW. If the core event described is a change of ownership of the plant, project, or company rather than a newly negotiated offtake agreement, set `is_signed_deal` to `false` and `transaction_type` to `"acquisition"`.
8. A PPA is specifically an ELECTRICITY (or renewable energy certificate) offtake agreement. Do NOT extract gas, LNG, hydrogen, or other non-electricity commodity supply contracts, even if the source article loosely uses the word "PPA" or describes a long-term energy supply agreement. If the agreement is not for electricity/certificates, set `is_signed_deal` to `false` and `transaction_type` to `"other"`.

Return ONLY valid JSON — no markdown fences, no explanation, nothing else.
The top-level JSON object MUST contain a "deals" array.
Each object must include **ALL fields** below (use `null` for missing values):

{{
  "is_signed_deal": true or false,
  "transaction_type": "ppa_signed" / "acquisition" / "tender_or_rumour" / "other",
  "is_likely_update": true or false,
  "update_clues": "what changed, or null",
  "date_agreement": "YYYY-MM-DD, YYYY-MM, or YYYY — null if unknown",
  "buyer": "offtaker company name(s), comma-separated",
  "seller": "developer / generator / IPP name(s)",
  "capacity_mw": number or null,
  "energy_gwh": number or null,
  "tenure_years": number or null,
  "is_european": true or false — is the energy DELIVERED in Europe? Base this on the country field above, not on where the buyer/seller is headquartered. A US company buying European energy is still European. Oman, UAE, USA, Australia, India etc. are NOT European regardless of who is involved.,
  "country": "delivery country of the energy — use the full country name in English, e.g. 'United Kingdom' not 'UK' or 'England'. If multiple countries, comma-separate them.",
  "technology": "solar / wind onshore / wind offshore / hydro / mixed / other",
  "price_eur_mwh": number or null,
  "notes": "project name, grid details, special terms, or null",
  "confidence": "high / medium / low"
}}

Text (any language — return all fields in English):
---
{text}"""

def _llm_wait(provider: dict) -> None:
    """Keep requests spaced out to avoid hitting per-minute limits."""
    name = provider["name"]
    delay = provider["min_delay"]
    last = last_llm_request.get(name)

    if last is not None:
        elapsed = time.monotonic() - last
        if elapsed < delay:
            time.sleep(delay - elapsed)


def _llm_disable(provider: dict, reason: str) -> None:
    name = provider["name"]
    disabled_llm_providers.add(name)
    log.warning(f"LLM provider DISABLED for this run: {name} — {reason}")


def _llm_request(provider: dict, prompt: str, title: str):
    """Return (JSON text, error_type). JSON text is always a string."""
    name = provider["name"]
    provider_type = provider["provider"]

    _llm_wait(provider)
    last_llm_request[name] = time.monotonic()

    try:
        # ---------------------------------------------------------------
        # Google Gemini / Gemma native API
        # ---------------------------------------------------------------
        if provider_type == "gemini":
            url = (
                "https://generativelanguage.googleapis.com/v1beta/"
                f"models/{provider['model']}:generateContent"
            )

            payload = {
                "contents": [
                    {
                        "parts": [
                            {"text": prompt}
                        ]
                    }
                ],
                "generationConfig": {
                    "maxOutputTokens": 1024,
                    "responseMimeType": "application/json",
                    "thinkingConfig": {
                        "thinkingLevel": "minimal"
                    },
                },
            }

            resp = requests.post(
                url,
                params={"key": provider["key"]},
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=provider["timeout"],
            )

            log.info(
                f"LLM [{name}/{provider['model']}] HTTP "
                f"{resp.status_code} for: {title[:60]}"
            )

            if resp.status_code == 200:
                data = resp.json()
                content = (
                    data["candidates"][0]
                    ["content"]["parts"][0]["text"]
                )
                return content, None

        # ---------------------------------------------------------------
        # Groq / OpenRouter / Mistral OpenAI-compatible APIs
        # ---------------------------------------------------------------
        else:
            payload = {
                "model": provider["model"],
                "max_tokens": 1024,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
            }

            if provider_type in ("groq", "openrouter", "mistral"):
                payload["response_format"] = {
                    "type": "json_object"
                }

            # Groq-specific reasoning settings
            if provider_type == "groq":
                if provider["model"] == "qwen/qwen3.8-27b":
                    payload["reasoning_effort"] = "none"
                else:
                    payload["reasoning_effort"] = "low"

            resp = requests.post(
                provider["url"],
                headers={
                    "Authorization": f"Bearer {provider['key']}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=provider["timeout"],
            )

            log.info(
                f"LLM [{name}/{provider['model']}] HTTP "
                f"{resp.status_code} for: {title[:60]}"
            )

            if resp.status_code == 200:
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
                return content, None

        # ---------------------------------------------------------------
        # Rate limit / quota
        # ---------------------------------------------------------------
        if resp.status_code == 429:
            _llm_disable(
                provider,
                f"HTTP 429 rate/quota limit"
            )
            return None, "quota"

        # ---------------------------------------------------------------
        # Authentication
        # ---------------------------------------------------------------
        if resp.status_code in (401, 403):
            _llm_disable(
                provider,
                f"authentication/permission HTTP {resp.status_code}"
            )
            return None, "disabled"

        # ---------------------------------------------------------------
        # Temporary server failure
        # ---------------------------------------------------------------
        if resp.status_code >= 500:
            log.warning(
                f"LLM [{name}] server error {resp.status_code}"
            )
            return None, "transient"

        log.warning(
            f"LLM [{name}] unexpected HTTP {resp.status_code}: "
            f"{resp.text[:300]}"
        )
        return None, "error"

    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as e:
        log.warning(f"LLM [{name}] malformed response: {e}")
        _llm_disable(provider, "malformed API response")
        return None, "error"

    except requests.Timeout:
        log.warning(f"LLM [{name}] request timeout")

        # Gemini/Gemma timeout: do not try that model again this run.
        if provider_type == "gemini":
            _llm_disable(provider, "timeout")
            return None, "disabled"

        return None, "transient"

    except requests.RequestException as e:
        log.warning(f"LLM [{name}] request failed: {e}")
        return None, "transient"


def _parse_llm_response(content, title: str, outlet: str):
    """Parse normalized JSON text from any LLM provider."""
    try:
        if not isinstance(content, str):
            raise TypeError("LLM content is not a string")

        content = content.strip()

        if content.startswith("```"):
            lines = content.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            content = "\n".join(lines).strip()

        parsed = json.loads(content)

        if isinstance(parsed, dict) and isinstance(parsed.get("deals"), list):
            deals = parsed["deals"]
        elif isinstance(parsed, list):
            deals = parsed
        elif isinstance(parsed, dict) and "is_signed_deal" in parsed:
            deals = [parsed]
        else:
            raise ValueError("Unexpected JSON structure")

        log.info(
            f"LLM extracted {len(deals)} deals | {title[:50]}"
        )
        return deals

    except (TypeError, ValueError, json.JSONDecodeError) as e:
        log.warning(
            f"LLM JSON/response parse error ({outlet}): {e}"
        )
        return None

def extract_with_llm(text: str, title: str, outlet: str):
    """
    Try all available LLM providers in priority order.

    If all providers are exhausted, FAIL the pipeline.
    Never silently return None after provider exhaustion.
    """

    for attempt, text_limit in enumerate([6000, 3000]):

        available = [
            p for p in LLM_PROVIDERS
            if p["key"]
            and p["name"] not in disabled_llm_providers
        ]

        if not available:
            raise RuntimeError(
                "All LLM providers exhausted for this run"
            )

        prompt = EXTRACTION_PROMPT.format(
            text=text[:text_limit]
        )

        for provider in available:

            if provider["name"] in disabled_llm_providers:
                continue

            log.info(
                f"Trying LLM provider: {provider['name']} "
                f"(model={provider['model']})"
            )

            content, error_type = _llm_request(
                provider,
                prompt,
                title,
            )

            if content is None:
                log.warning(
                    f"LLM provider {provider['name']} failed "
                    f"({error_type}); trying next provider"
                )
                continue

            parsed = _parse_llm_response(
                content,
                title,
                outlet,
            )

            if parsed is not None:
                return parsed

            log.warning(
                f"LLM provider {provider['name']} returned invalid JSON; "
                f"trying next provider"
            )

        # Retry the same article once with the shorter text
        # ONLY if at least one provider is still available.
        if attempt == 0:
            remaining = [
                p for p in LLM_PROVIDERS
                if p["key"]
                and p["name"] not in disabled_llm_providers
            ]

            if remaining:
                log.info(
                    "LLM providers remain available; "
                    "retrying article with shorter input"
                )
            else:
                raise RuntimeError(
                    "All LLM providers exhausted for this run"
                )

    raise RuntimeError(
        f"All LLM attempts failed for article: {title[:100]}"
    )

# ── Deduplication ─────────────────────────────────────────────────────────────

COUNTRY_ALIASES = {
    "uk": "united kingdom",
    "great britain": "united kingdom",
    "britain": "united kingdom",
    "czechia": "czech republic",
    "the netherlands": "netherlands",
    "holland": "netherlands",
}

# Generic corporate/legal descriptors that don't distinguish one company from
# another once stripped (e.g. "Solaria" vs "Solaria Energía y Medio Ambiente, S.A.").
LEGAL_SUFFIXES = _re.compile(
    r"\b(ltd\.?|llc\.?|inc\.?|corp\.?|ag|sa|spa|bv|nv|gmbh|plc|oy|ab|as|properties|"
    r"energia|energía|y medio ambiente|group ag|supply ltd|energy ltd|renewables ltd)\b\.?",
    _re.IGNORECASE,
)

# Local-subsidiary / geo qualifiers appended to a parent company's name
# (e.g. "Iberdrola España" is the same company as "Iberdrola" for our purposes).
GEO_SUFFIXES = _re.compile(
    r"\b(españa|espana|spain|italia|italy|iberia|portugal|france|francia|"
    r"deutschland|germany|uk|europe|international)\b",
    _re.IGNORECASE,
)

# Buyer/seller similarity threshold (rapidfuzz token_set_ratio, 0-100).
# token_set_ratio is used because it stays high when one name is a superset
# of the other's words ("RWE" vs "RWE Renewables Iberia" still scores 100),
# which exact/tag-sort matching does not handle.
ENTITY_MATCH_THRESHOLD = 85

# Words too generic to count as a project-identifying token when comparing
# two deals' `notes` fields (used only as a tie-breaker, see find_similar_duplicate).
PROJECT_TOKEN_STOPWORDS = {
    "the", "ppa", "mw", "gwh", "spain", "italy", "portugal", "france", "germany",
    "uk", "europe", "european", "project", "plant", "farm", "solar", "wind",
    "energy", "energia", "energía", "group", "corp", "first", "new", "data",
    "center", "centre", "power", "renewable", "renewables", "agreement", "deal",
    "company", "for", "with", "initial",
}

# Deals with generic, non-specific counterparty names — usually a sign the LLM
# failed to identify the actual named entity from the article text and
# substituted a vague description instead. Flagged, not auto-rejected: some
# of these are legitimate (an article can genuinely withhold a buyer's name).
GENERIC_ENTITY_PATTERNS = _re.compile(
    r"^(local |several |various |multiple |unnamed |unspecified )?"
    r"(electricity providers?|companies|developers?( of.*)?|utilities|"
    r"portfolio.*|consortium( of.*)?)$",
    _re.IGNORECASE,
)

def _normalize_country(country: str) -> str:
    c = country.lower().strip()
    return COUNTRY_ALIASES.get(c, c)

def _normalize_entity(name: str) -> str:
    n = (name or "").lower().strip()
    n = LEGAL_SUFFIXES.sub("", n)
    n = GEO_SUFFIXES.sub("", n)
    n = _re.sub(r"[^a-z0-9 ]", " ", n)
    return " ".join(n.split())  # collapse whitespace

def _country_overlap(a: str, b: str) -> bool:
    """True if the two (possibly comma-separated, multi-country) fields share
    at least one country in common, after alias normalization."""
    sa = {_normalize_country(c.strip()) for c in (a or "").split(",") if c.strip()}
    sb = {_normalize_country(c.strip()) for c in (b or "").split(",") if c.strip()}
    return bool(sa & sb)

def _capacity_close(a, b, tol: float = 0.15):
    """True/False if both capacities are known (within `tol` relative
    tolerance); None if either is missing — i.e. "ambiguous, can't rule
    out or confirm on capacity alone"."""
    if a is None or b is None:
        return None
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return None
    if a == 0 and b == 0:
        return True
    return abs(a - b) / max(a, b) <= tol

def _parse_date_loose(d: str):
    if not d:
        return None
    d = d[:10]
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(d, fmt)
        except ValueError:
            continue
    return None

def _date_close(a: str, b: str, days: int = 120):
    """True/False if both dates parse (within `days`); None if either is
    unparseable/missing."""
    da, db = _parse_date_loose(a), _parse_date_loose(b)
    if da is None or db is None:
        return None
    return abs((da - db).days) <= days

def extract_project_tokens(notes: str) -> set:
    """Pulls out capitalized, non-generic words from a notes field — a cheap
    proxy for project/place names ('Ginosa', 'Castaño', 'Arasur') without
    needing embeddings. Used only as a tie-breaker when capacity is unknown
    on one or both sides of a comparison."""
    if not notes:
        return set()
    words = _re.findall(r"[A-ZÀ-Ý][a-zà-ÿ]{3,}", notes)
    return {w.lower() for w in words if w.lower() not in PROJECT_TOKEN_STOPWORDS}

def make_deal_hash(extracted: dict) -> str:
    """Deterministic fingerprint stored alongside each row for auditing/export
    purposes. No longer used for duplicate lookup (see find_similar_duplicate) —
    exact-match hashing was too brittle against name variants, date-granularity
    differences, and capacity-rounding differences across sources."""
    date = (extracted.get("date_agreement") or "")[:7]

    buyer = _normalize_entity(extracted.get("buyer") or "")
    seller = _normalize_entity(extracted.get("seller") or "")
    parties = sorted([buyer, seller])

    country = _normalize_country(extracted.get("country") or "")
    country_parts = sorted([c.strip() for c in country.split(",")])
    country_normalized = ",".join(country_parts)

    capacity = extracted.get("capacity_mw")
    capacity_str = str(round(float(capacity))) if capacity else "unknown"

    parts = [
        parties[0],
        parties[1],
        country_normalized,
        capacity_str,
        date,
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]

def find_similar_duplicate(existing_rows: list[dict], extracted: dict) -> dict | None:
    """
    Fuzzy dedup against an in-memory list of already-stored deals (loaded once
    per run, updated as new deals are written — see run()).

    Matches on:
      - buyer AND seller name similarity (token_set_ratio, robust to legal
        suffixes, geo-subsidiary qualifiers, and small typos like
        "Zelestra"/"Zalestra")
      - at least one country in common
      - capacity: if both sides have a capacity_mw, it must be within 15% of
        each other, AND — if both sides' notes name specific project tokens —
        those tokens must not be entirely disjoint (guards against two
        different real projects for the same recurring buyer/seller pair
        coincidentally landing within tolerance, e.g. a 110 MW and a 105 MW
        solar farm for the same offtaker). If capacity is missing on one/both
        sides, fall back to requiring one of: a shared project-name token in
        `notes`, dates within ~4 months where BOTH dates have at least
        month-level precision (a bare year like "2024" is too imprecise to
        trust on its own), or a matching contract tenure (years) when neither
        notes nor dates give a usable signal.

    Once buyer/seller/country/capacity all agree closely, date proximity is
    NOT required — the same signed deal routinely gets re-reported over
    months as it moves through signing/construction/commissioning coverage.
    """
    buyer_n = _normalize_entity(extracted.get("buyer") or "")
    seller_n = _normalize_entity(extracted.get("seller") or "")
    if not buyer_n or not seller_n:
        return None

    country = extracted.get("country") or ""
    capacity = extracted.get("capacity_mw")
    date_agreement = extracted.get("date_agreement") or ""
    new_tokens = extract_project_tokens(extracted.get("notes") or "")

    for row in existing_rows:
        row_buyer_n = _normalize_entity(row.get("buyer") or "")
        row_seller_n = _normalize_entity(row.get("seller") or "")

        if fuzz.token_set_ratio(buyer_n, row_buyer_n) < ENTITY_MATCH_THRESHOLD:
            continue
        if fuzz.token_set_ratio(seller_n, row_seller_n) < ENTITY_MATCH_THRESHOLD:
            continue
        if not _country_overlap(country, row.get("country") or ""):
            continue

        cap_result = _capacity_close(capacity, row.get("capacity_mw"))
        if cap_result is False:
            continue  # capacities clearly differ -> different deal, not a re-report

        row_tokens = extract_project_tokens(row.get("notes") or "")

        if cap_result is True:
            # Capacity looks close, but two different real projects between the
            # same recurring buyer/seller pair can still coincidentally land
            # within tolerance (e.g. a 110 MW and a 105 MW solar farm for the
            # same offtaker). If both sides name specific, non-overlapping
            # project tokens, that's conflicting evidence — don't merge.
            if new_tokens and row_tokens and not (new_tokens & row_tokens):
                continue

        if cap_result is None:
            # Capacity unknown on one/both sides: same recurring buyer/seller
            # pair could easily be two unrelated deals (see Amazon/Iberdrola
            # false-positive case), so require corroborating evidence.
            tokens_match = bool(new_tokens and row_tokens and (new_tokens & row_tokens))

            # Bare years ("2024") parse to Jan 1st, which can look artificially
            # "close" to a precise month elsewhere — only trust the date
            # fallback when both sides have at least month-level precision.
            row_date = row.get("date_agreement") or ""
            dates_precise = len(date_agreement) >= 7 and len(row_date) >= 7
            dates_corroborate = dates_precise and _date_close(date_agreement, row_date) is True

            # Matching, non-null contract terms (tenure) are also meaningful
            # corroboration when neither notes nor dates give a signal —
            # e.g. two sparse-notes reports of the same deal that both cite
            # the same 10-year term.
            new_tenure, row_tenure = extracted.get("tenure_years"), row.get("tenure_years")
            tenure_corroborates = (
                new_tenure is not None
                and row_tenure is not None
                and abs(float(new_tenure) - float(row_tenure)) < 0.01
            )

            if not tokens_match and not dates_corroborate and not tenure_corroborates:
                continue

        return row

    return None

def _drop_aggregate_rows(deals: list[dict]) -> list[dict]:
    """
    Guards against the model returning both itemized deals AND their combined
    total in the same response, despite the prompt instructing otherwise
    (observed e.g. wind 49.5 MW + solar 212 MW + a 257 MW "combined" entry,
    all from one article). If one deal's capacity is within 5% of the sum of
    all the others', it's an aggregate row layered on top of the itemized
    ones — drop it and keep the itemized rows.
    """
    if len(deals) < 2:
        return deals

    caps = [d.get("capacity_mw") for d in deals]
    if any(c is None for c in caps):
        return deals  # can't safely reason about sums with missing values

    total = sum(caps)
    kept = []
    for d, c in zip(deals, caps):
        other_sum = total - c
        if other_sum > 0 and abs(c - other_sum) / max(c, other_sum) <= 0.05:
            log.info(
                f"Dropping aggregate deal (capacity {c} MW ≈ sum of the other "
                f"{other_sum} MW in the same article): {d.get('buyer')} / {d.get('seller')}"
            )
            continue
        kept.append(d)
    return kept or deals  # never let this guard empty out a real single-deal list

def _flag_if_vague(field_name: str, value: str, title: str) -> None:
    """Soft warning only — logs for manual review, doesn't drop the deal,
    since a genuinely undisclosed counterparty is a legitimate real case."""
    if value and GENERIC_ENTITY_PATTERNS.match(value.strip()):
        log.warning(
            f"Vague {field_name} extracted ('{value}') — possibly a missed "
            f"named entity, worth a manual check: {title[:60]}"
        )

def classify_match(existing: dict, new_deal: dict, new_pub_date: str) -> str:
    """
    Given an existing DB row and a newly extracted deal matched to it by
    find_similar_duplicate, returns 'U' if the new article adds meaningful
    information, or 'D' if it's the same content from a different source.
    """
    enriching_fields = ["energy_gwh", "tenure_years", "price_eur_mwh", "technology"]
    for field in enriching_fields:
        existing_val = existing.get(field)
        new_val = new_deal.get(field)
        if new_val and not existing_val:
            return "U"

    # Longer notes = more information
    existing_notes = existing.get("notes") or ""
    new_notes = str(new_deal.get("notes") or "")
    if len(new_notes) > len(existing_notes) + 20:
        return "U"

    # Later publication date on a different source article
    existing_pub = (existing.get("publication_date") or "")[:10]
    if new_pub_date and existing_pub and new_pub_date > existing_pub:
        return "U"

    return "D"

# ── Database Write ────────────────────────────────────────────────────────────

def write_deal(conn, extracted, real_url, article, full_text, match_type, canonical_id):
    """
    match_type: 'N' | 'U'
    Duplicates are skipped before reaching this function.
    Returns the new row's id (needed so run() can add it to the in-memory
    dedup pool for the rest of the current run).
    """
    deal_hash = make_deal_hash(extracted)
    notes     = extracted.get("notes") or ""

    if match_type == "U":
        if extracted.get("update_clues"):
            notes = f"[UPDATE] {extracted['update_clues']} | {notes}".strip(" |")
        notes += f" | Original deal ID: {canonical_id}"
        deal_hash = deal_hash + f"_upd_{canonical_id}"

    cursor = conn.execute(
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
    return cursor.lastrowid

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
    """Decode a Google News URL to get the real article URL."""
    if not url or "news.google.com" not in url:
        return url

    try:
        decoded = gnewsdecoder(
            url,
            interval=1,
            timeout=10,
        )

        real_url = decoded.get("decoded_url")
        if decoded.get("success") and real_url:
            log.info(f"Resolved Google News URL: {real_url[:100]}")
            return real_url

        log.warning(
            f"URL decoding failed: {decoded.get('message', 'unknown error')} "
            f"| {url[:100]}"
        )
        return url

    except Exception as e:
        log.warning(f"URL resolution failed ({e}): {url[:100]}")
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

    # Load all existing deals into memory once, for fuzzy dedup lookups
    # (see find_similar_duplicate). Newly written rows are appended to this
    # list during the run so later articles in the same run are checked
    # against them too, not just what was in the DB at the start.
    existing_rows = [
        dict(zip(
            ["id", "buyer", "seller", "country", "capacity_mw", "date_agreement",
             "energy_gwh", "tenure_years", "price_eur_mwh", "technology",
             "notes", "publication_date"],
            row,
        ))
        for row in conn.execute(
            "SELECT id, buyer, seller, country, capacity_mw, date_agreement, "
            "energy_gwh, tenure_years, price_eur_mwh, technology, notes, publication_date "
            "FROM deals"
        ).fetchall()
    ]
    log.info(f"Loaded {len(existing_rows)} existing deals for dedup matching")

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

        # Drop any combined/aggregate entry the model returned alongside the
        # itemized deals for the same article (see _drop_aggregate_rows).
        deals = _drop_aggregate_rows(deals)

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

            # Belt-and-suspenders: even if is_signed_deal slipped through true,
            # never store acquisitions/M&A as PPA deals.
            if deal.get("transaction_type") not in (None, "ppa_signed"):
                log.info(
                    f"Not a PPA deal (transaction_type={deal.get('transaction_type')}) — "
                    f"skipping: {title[:60]}"
                )
                continue

            # Belt-and-suspenders: reject non-electricity energy agreements
            # (gas/LNG/hydrogen supply deals loosely described as "PPA" in
            # source text) that have neither a power capacity nor an energy
            # volume and fall back to the catch-all "other" technology —
            # e.g. LNG import Heads of Agreement mistakenly extracted as PPAs.
            if (
                deal.get("technology") == "other"
                and deal.get("capacity_mw") is None
                and deal.get("energy_gwh") is None
            ):
                log.info(f"Non-electricity energy deal — skipping: {title[:60]}")
                continue

            if not deal.get("is_european"):
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

            _flag_if_vague("buyer", deal.get("buyer") or "", title)
            _flag_if_vague("seller", deal.get("seller") or "", title)

            existing    = find_similar_duplicate(existing_rows, deal)
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

            new_id = write_deal(conn, deal, real_url, article, full_text, match_type, canonical_id)
            processed += 1

            # Add to the in-memory pool so later articles in this same run
            # are matched against it too, not just what was in the DB at start.
            existing_rows.append({
                "id": new_id,
                "buyer": deal.get("buyer"),
                "seller": deal.get("seller"),
                "country": deal.get("country"),
                "capacity_mw": deal.get("capacity_mw"),
                "date_agreement": deal.get("date_agreement"),
                "energy_gwh": deal.get("energy_gwh"),
                "tenure_years": deal.get("tenure_years"),
                "price_eur_mwh": deal.get("price_eur_mwh"),
                "technology": deal.get("technology"),
                "notes": deal.get("notes"),
                "publication_date": new_pub,
            })

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
