"""
RSS Feed Tester
Runs from GitHub Actions — trigger manually from the UI.
Tests each feed: fetches articles, attempts full text retrieval,
reports whether the feed is usable for PPA extraction.
"""

import os
import sys
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from html.parser import HTMLParser

ARTICLES_TO_TEST = 3
MIN_USEFUL_CHARS = 500  # below this, full text fetch is considered failed

DEFAULT_FEEDS = [
    "https://www.pv-magazine.com/feed/",
    "https://renews.biz/feed/",
    "https://www.theenergyst.com/feed/",
    "https://www.montelnews.com/rss",
    "https://www.evwind.es/feed",
    "https://www.solarpaces.org/feed/",
    # already in pipeline — included as baseline
    "https://energymonitor.ai/feed/",
    "https://elperiodicodelaenergia.com/feed/",
]


class _TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.chunks = []
        self._skip = 0

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


def fetch_full_text(url: str) -> tuple[str | None, int]:
    """Returns (text, char_count). text is None if fetch failed."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        resp = requests.get(url, headers=headers, timeout=12, allow_redirects=True)
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            parser = _TextExtractor()
            parser.feed(resp.text)
            text = " ".join(parser.chunks)
            return text, len(text)
        return None, 0
    except Exception as e:
        return None, 0


def fetch_rss(feed_url: str) -> list[dict]:
    try:
        resp = requests.get(feed_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)

        articles = []
        for item in items:
            title_el = item.find("title")
            link_el = item.find("link") or item.find("atom:link", ns)
            title = title_el.text.strip() if title_el is not None else ""
            link = link_el.text if link_el is not None else ""
            if not link and link_el is not None:
                link = link_el.get("href", "")
            articles.append({"title": title, "url": link})

        return articles
    except Exception as e:
        return []


def test_feed(feed_url: str) -> dict:
    print(f"\n{'='*60}")
    print(f"FEED: {feed_url}")
    print(f"{'='*60}")

    articles = fetch_rss(feed_url)
    if not articles:
        print("  ✗ RSS fetch failed or returned no articles")
        return {"feed": feed_url, "status": "RSS_FAILED", "articles_tested": 0}

    print(f"  RSS OK — {len(articles)} articles found")

    results = []
    for i, article in enumerate(articles[:ARTICLES_TO_TEST]):
        title = article["title"][:80]
        url = article["url"]
        print(f"\n  Article {i+1}: {title}")
        print(f"  URL: {url[:100]}")

        text, char_count = fetch_full_text(url)
        time.sleep(2)

        if char_count >= MIN_USEFUL_CHARS:
            status = "FULL_TEXT_OK"
            print(f"  ✓ Full text: {char_count} chars")
            print(f"  Preview: {(text or '')[:200]}...")
        elif char_count > 0:
            status = "PARTIAL"
            print(f"  ~ Partial text: {char_count} chars (below {MIN_USEFUL_CHARS} threshold)")
            print(f"  Preview: {(text or '')[:200]}...")
        else:
            status = "FAILED"
            print(f"  ✗ Full text fetch failed (paywall / 403 / timeout)")

        results.append({"title": title, "url": url, "chars": char_count, "status": status})

    ok_count = sum(1 for r in results if r["status"] == "FULL_TEXT_OK")
    partial_count = sum(1 for r in results if r["status"] == "PARTIAL")
    avg_chars = sum(r["chars"] for r in results) // max(len(results), 1)

    if ok_count == ARTICLES_TO_TEST:
        verdict = "USABLE"
    elif ok_count + partial_count >= 2:
        verdict = "PARTIAL"
    else:
        verdict = "NOT_USABLE"

    print(f"\n  VERDICT: {verdict} ({ok_count}/{ARTICLES_TO_TEST} full text OK, avg {avg_chars} chars)")
    return {
        "feed": feed_url,
        "status": verdict,
        "articles_tested": len(results),
        "full_text_ok": ok_count,
        "avg_chars": avg_chars,
    }


def main():
    feeds_input = os.environ.get("FEEDS_INPUT", "").strip()
    if feeds_input:
        feeds = [f.strip() for f in feeds_input.split(",") if f.strip()]
    else:
        feeds = DEFAULT_FEEDS

    print(f"Testing {len(feeds)} RSS feeds")
    print(f"Articles per feed: {ARTICLES_TO_TEST}")
    print(f"Min useful chars: {MIN_USEFUL_CHARS}")

    summaries = []
    for feed_url in feeds:
        result = test_feed(feed_url)
        summaries.append(result)
        time.sleep(3)

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for s in summaries:
        icon = "✓" if s["status"] == "USABLE" else "~" if s["status"] == "PARTIAL" else "✗"
        print(f"  {icon} [{s['status']:12}] {s['feed']}")

    usable = [s for s in summaries if s["status"] == "USABLE"]
    print(f"\n{len(usable)}/{len(summaries)} feeds are fully usable")
    if usable:
        print("Add these to EXTRA_RSS_FEEDS in pipeline.py:")
        for s in usable:
            print(f'  "{s["feed"]}",')


if __name__ == "__main__":
    main()
