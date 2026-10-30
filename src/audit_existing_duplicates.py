#!/usr/bin/env python3
"""Read-only audit of historical PPA records and duplicate candidates."""

import csv
import itertools
import os
import sqlite3
import sys
from collections import defaultdict
from functools import lru_cache
from pathlib import Path

# Import pipeline helpers without calling run() or making API requests.
os.environ.setdefault("NEWSAPI_KEY", "AUDIT_MODE_NO_API_CALLS")
os.environ.setdefault("ENVIRONMENT", "AUDIT")
SRC_DIR = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
sys.path.insert(0, str(SRC_DIR))
import pipeline as p  # noqa: E402

OUTPUT_DIR = p.DATA_DIR / "duplicate_audit"

# User-supplied October test cases plus the related Google/Fortum canonical group
# discovered during the first audit. The extra IDs are included to make the
# suspicious canonical group visible in the next report.
KNOWN_IDS = sorted({
    1986, 1979, 1978, 1977, 1947,
    1900, 1899, 1939, 1917, 203, 89,
    793, 252, 1270, 1199, 1926, 1844, 1843, 1834,
    1991, 1863, 1884, 1885, 1867, 1897, 1891,
    1862, 1864, 1868, 1875, 1925, 1965, 2021,
})

PEXAPARK_IDS = [1986, 1979, 1978, 1977, 1947]
APPARENTLY_SIMILAR_IDS = [793, 252, 1270, 1199, 1926, 1844, 1843, 1834]
INCOMPLETE_IDS = [1991, 1863, 1884]
GOOGLE_FORTUM_IDS = [1862, 1864, 1868, 1875, 1925, 1965, 1991, 2021]

REQUIRED_REVIEW_PAIRS = {}
for pair in itertools.combinations(PEXAPARK_IDS, 2):
    REQUIRED_REVIEW_PAIRS[tuple(sorted(pair))] = (
        "PEXAPARK_PROTECTION: these rows relate to multiple distinct projects/PPAs; "
        "do not merge the group together. Verify each project's capacity and identity"
    )
for pair in itertools.combinations(APPARENTLY_SIMILAR_IDS, 2):
    REQUIRED_REVIEW_PAIRS[tuple(sorted(pair))] = (
        "USER_REVIEW_GROUP: apparently related October records; relationship not confirmed"
    )
for pair in itertools.combinations(INCOMPLETE_IDS, 2):
    REQUIRED_REVIEW_PAIRS[tuple(sorted(pair))] = (
        "INCOMPLETE_RECORDS: compare missing fields and assess whether each row adds useful evidence"
    )
for pair in itertools.combinations(GOOGLE_FORTUM_IDS, 2):
    REQUIRED_REVIEW_PAIRS[tuple(sorted(pair))] = (
        "GOOGLE_FORTUM_CANONICAL_REVIEW: inspect the group for mixed PPAs, wrong counterparties, "
        "or incorrect links; do not assume every row is the same contract"
    )
REQUIRED_REVIEW_PAIRS.update({
    tuple(sorted((1900, 1899))): "USER_EXPECTED_SAME_DEAL: verify whether the current matcher detects this pair",
    tuple(sorted((1939, 1917))): "USER_SUSPECTED_DUPLICATE: possible 1,000x capacity/locale discrepancy; do not auto-convert values",
    tuple(sorted((203, 89))): "USER_SUSPECTED_SAME_DEAL: verify both source articles actually describe Saint-Gobain/TotalEnergies PPAs",
    tuple(sorted((1885, 1867))): "USER_REVIEW_ONLY: may describe two deals or an incomplete/merged counterparty extraction",
    tuple(sorted((1897, 1891))): "USER_EXPECTED_SAME_DEAL: compare sparse and fuller Salzgitter/Zelestra record",
    tuple(sorted((1978, 1979))): "DISTINCT_PROJECT_PROTECTION: Valkyria (150 MW) and Trujillo (170 MW) are different named projects; never auto-merge",
})

PAIR_FIELDS = [
    "id_a", "id_b", "event_type_a", "event_type_b",
    "canonical_id_a", "canonical_id_b", "canonical_root_a", "canonical_root_b",
    "canonical_status_a", "canonical_status_b", "same_canonical_group",
    "buyer_a", "buyer_b", "seller_a", "seller_b", "country_a", "country_b",
    "capacity_mw_a", "capacity_mw_b", "capacity_ratio", "capacity_scale_anomaly",
    "energy_gwh_a", "energy_gwh_b", "tenure_years_a", "tenure_years_b",
    "date_agreement_a", "date_agreement_b", "date_found_a", "date_found_b",
    "source_outlet_a", "source_outlet_b", "source_url_a", "source_url_b",
    "notes_a", "notes_b", "raw_snippet_a", "raw_snippet_b",
    "project_tokens_a", "project_tokens_b", "project_identity_conflict",
    "source_party_support_a", "source_party_support_b",
    "matcher_match_direct_before_guard", "matcher_match_reversed_before_guard",
    "current_match_direct", "current_match_reversed", "review_reasons",
]

KNOWN_FIELDS = [
    "requested_id", "found_in_database", "event_type", "canonical_id",
    "resolved_canonical_id", "canonical_chain_status", "same_group_ids",
    "candidate_match_ids", "review_match_ids", "date_agreement", "date_found",
    "buyer", "seller", "capacity_mw", "energy_gwh", "tenure_years", "country",
    "technology", "price_eur_mwh", "source_url", "source_outlet",
    "publication_date", "notes", "raw_snippet",
]

ROW_FIELDS = [
    "event_type", "canonical_id", "date_agreement", "date_found", "buyer", "seller",
    "capacity_mw", "energy_gwh", "tenure_years", "country", "technology",
    "price_eur_mwh", "source_url", "source_outlet", "publication_date",
    "notes", "raw_snippet",
]

CANONICAL_FIELDS = [
    "id", "event_type", "canonical_id", "resolved_canonical_id",
    "canonical_chain_status", "buyer", "seller", "country", "capacity_mw",
    "source_url", "notes",
]

ARTICLE_GROUP_FIELDS = [
    "source_url", "source_outlet", "record_count", "record_ids",
    "canonical_roots", "buyers", "sellers", "countries", "capacities_mw",
    "project_names_from_notes", "review_reason",
]

SCALE_FIELDS = [
    "id_a", "id_b", "buyer_a", "buyer_b", "seller_a", "seller_b",
    "country_a", "country_b", "capacity_mw_a", "capacity_mw_b", "capacity_ratio",
    "date_agreement_a", "date_agreement_b", "source_url_a", "source_url_b",
    "notes_a", "notes_b", "review_reason",
]


def csv_write(path, fields, rows):
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def missing(value):
    return value is None or str(value).strip() == ""


def numeric(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=100000)
def normalize_entity(value):
    return p._normalize_entity(value or "")


def similarity(left, right):
    left_n, right_n = normalize_entity(left), normalize_entity(right)
    if not left_n or not right_n:
        return 0
    return int(p.fuzz.token_set_ratio(left_n, right_n))


def project_tokens(row):
    if "_project_tokens" in row:
        return row["_project_tokens"]
    return p.extract_project_tokens(
        row.get("notes") or "", row.get("buyer") or "", row.get("seller") or ""
    )


def capacity_ratio(a, b):
    cap_a, cap_b = numeric(a), numeric(b)
    if cap_a is None or cap_b is None or min(cap_a, cap_b) <= 0:
        return None
    return max(cap_a, cap_b) / min(cap_a, cap_b)


def source_party_support(row):
    """Check whether both stored parties are visible in the stored 500-char excerpt.

    This is only an evidence warning. A short excerpt may omit a party that is
    present later in the article, so it must never independently reject a row.
    """
    snippet = str(row.get("raw_snippet") or "")
    if not snippet.strip():
        return "unknown"
    lower = normalize_entity(snippet)
    found = 0
    checked = 0
    for party in (row.get("buyer"), row.get("seller")):
        party_n = normalize_entity(party or "")
        if not party_n:
            continue
        checked += 1
        # A compact normalized name or its meaningful tokens is enough for this
        # warning; the test is intentionally not used as an auto-match criterion.
        tokens = [t for t in party_n.split() if len(t) >= 4]
        if party_n in lower or (tokens and any(t in lower.split() for t in tokens)):
            found += 1
    if checked == 0:
        return "unknown"
    if found == checked:
        return "both parties visible"
    if found == 0:
        return "neither party visible"
    return "only one party visible"


def resolve_canonical(row_id, rows_by_id):
    current = int(row_id)
    visited = []
    while True:
        if current in visited:
            return current, "ERROR: canonical-link cycle"
        visited.append(current)
        row = rows_by_id.get(current)
        if row is None:
            return current, f"ERROR: canonical parent ID {current} not found"

        event_type = str(row.get("event_type") or "N").strip().upper()
        parent = row.get("canonical_id")
        if missing(parent):
            if event_type not in ("N", "NEW"):
                return current, f"WARNING: {event_type} row has no canonical_id"
            return current, "OK"

        if event_type in ("N", "NEW"):
            return current, "WARNING: N row unexpectedly has canonical_id"
        try:
            parent_id = int(parent)
        except (TypeError, ValueError):
            return current, "ERROR: invalid canonical_id"
        if parent_id not in rows_by_id:
            return parent_id, f"ERROR: canonical parent ID {parent_id} not found"
        current = parent_id


def build_pair_analysis(a, b, roots, canonical_status):
    buyer_direct = similarity(a.get("buyer"), b.get("buyer"))
    seller_direct = similarity(a.get("seller"), b.get("seller"))
    buyer_reverse = similarity(a.get("buyer"), b.get("seller"))
    seller_reverse = similarity(a.get("seller"), b.get("buyer"))
    threshold = p.ENTITY_MATCH_THRESHOLD
    direct_parties = buyer_direct >= threshold and seller_direct >= threshold
    reversed_parties = buyer_reverse >= threshold and seller_reverse >= threshold

    country_a, country_b = a.get("country") or "", b.get("country") or ""
    country_overlap = p._country_overlap(country_a, country_b)
    country_missing = not country_a or not country_b
    countries_compatible_for_review = country_overlap or country_missing

    tokens_a, tokens_b = project_tokens(a), project_tokens(b)
    project_conflict = bool(tokens_a and tokens_b and not (tokens_a & tokens_b))
    ratio = capacity_ratio(a.get("capacity_mw"), b.get("capacity_mw"))
    scale_anomaly = ratio is not None and ratio >= 100

    matcher_direct = False
    matcher_reversed = False
    if direct_parties and country_overlap:
        matcher_direct = p.find_similar_duplicate([a], b) is not None
    if reversed_parties and country_overlap:
        swapped = dict(b)
        swapped["buyer"], swapped["seller"] = b.get("seller"), b.get("buyer")
        matcher_reversed = p.find_similar_duplicate([a], swapped) is not None

    # Candidate classification is stricter than a raw matcher hit: distinct
    # project identities and the known Pexapark multi-project set are protected.
    in_pexapark_group = int(a["id"]) in PEXAPARK_IDS and int(b["id"]) in PEXAPARK_IDS
    direct_match = matcher_direct and not project_conflict and not in_pexapark_group
    reverse_match = matcher_reversed and not project_conflict and not in_pexapark_group

    reasons = []
    parties_align = direct_parties or reversed_parties
    if parties_align and countries_compatible_for_review:
        if project_conflict:
            reasons.append(
                "DISTINCT_PROJECT_IDENTITY: both notes contain named project/location tokens but none overlap; do not auto-merge"
            )
        if in_pexapark_group:
            reasons.append(
                "PEXAPARK_PROTECTION: related announcement contains distinct PPA projects; keep separate unless a specific pair is source-verified"
            )
        if reverse_match or (matcher_reversed and not matcher_direct):
            reasons.append("Possible reversed buyer/seller orientation; review source before linking")
        if missing(a.get("capacity_mw")) or missing(b.get("capacity_mw")):
            reasons.append("Matching parties but capacity missing on at least one row")
        if scale_anomaly:
            reasons.append(
                f"Possible capacity locale/unit discrepancy: capacities differ by {ratio:.1f}x; do not auto-convert"
            )
        if ratio is not None and not scale_anomaly and p._capacity_close(a.get("capacity_mw"), b.get("capacity_mw")) is False and tokens_a & tokens_b:
            reasons.append("Different capacities but project tokens overlap; verify source wording and units")
        if not country_overlap and country_missing:
            reasons.append("Country missing on at least one row; country match cannot be confirmed")

    support_a, support_b = source_party_support(a), source_party_support(b)
    if support_a in ("neither party visible", "only one party visible"):
        reasons.append(f"Source excerpt A party check: {support_a}; verify extraction against full article")
    if support_b in ("neither party visible", "only one party visible"):
        reasons.append(f"Source excerpt B party check: {support_b}; verify extraction against full article")

    root_a, root_b = roots[int(a["id"])], roots[int(b["id"])]
    base = {
        "id_a": a.get("id"), "id_b": b.get("id"),
        "event_type_a": a.get("event_type"), "event_type_b": b.get("event_type"),
        "canonical_id_a": a.get("canonical_id"), "canonical_id_b": b.get("canonical_id"),
        "canonical_root_a": root_a, "canonical_root_b": root_b,
        "canonical_status_a": canonical_status[int(a["id"])],
        "canonical_status_b": canonical_status[int(b["id"])],
        "same_canonical_group": "yes" if root_a == root_b else "no",
        "buyer_similarity_direct": buyer_direct,
        "seller_similarity_direct": seller_direct,
        "buyer_seller_similarity_reverse": buyer_reverse,
        "seller_buyer_similarity_reverse": seller_reverse,
        "capacity_ratio": round(ratio, 3) if ratio is not None else "",
        "capacity_scale_anomaly": "yes" if scale_anomaly else "no",
        "project_tokens_a": ", ".join(sorted(tokens_a)),
        "project_tokens_b": ", ".join(sorted(tokens_b)),
        "project_identity_conflict": "yes" if project_conflict else "no",
        "source_party_support_a": support_a,
        "source_party_support_b": support_b,
        "matcher_match_direct_before_guard": "yes" if matcher_direct else "no",
        "matcher_match_reversed_before_guard": "yes" if matcher_reversed else "no",
        "current_match_direct": "yes" if direct_match else "no",
        "current_match_reversed": "yes" if reverse_match else "no",
    }
    for field in ROW_FIELDS:
        base[f"{field}_a"] = a.get(field) or ""
        base[f"{field}_b"] = b.get(field) or ""
    return {
        "base": base,
        "direct_match": direct_match,
        "reverse_match": reverse_match,
        "matcher_direct_before_guard": matcher_direct,
        "matcher_reversed_before_guard": matcher_reversed,
        "direct_parties": direct_parties,
        "reversed_parties": reversed_parties,
        "countries_compatible": countries_compatible_for_review,
        "project_conflict": project_conflict,
        "scale_anomaly": scale_anomaly,
        "reasons": reasons,
    }


def main():
    db_path = p.DB_PATH.resolve()
    if not db_path.is_file():
        raise FileNotFoundError(f"Database not found in repository checkout: {db_path}")

    # SQLite URI mode=ro prevents this audit from writing to the database.
    db_uri = db_path.as_uri() + "?mode=ro"
    with sqlite3.connect(db_uri, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "deals" not in tables:
            raise RuntimeError("The database does not contain the expected 'deals' table.")
        columns = {r[1] for r in conn.execute("PRAGMA table_info(deals)")}
        required_columns = {"id", "event_type", "canonical_id", "buyer", "seller", "country", "capacity_mw"}
        missing_columns = sorted(required_columns - columns)
        if missing_columns:
            raise RuntimeError(f"Missing expected database columns: {', '.join(missing_columns)}")
        rows = [dict(r) for r in conn.execute("SELECT * FROM deals ORDER BY id").fetchall()]

    rows_by_id = {int(row["id"]): row for row in rows}
    # Cache project identity extraction once per record; the pairwise comparison
    # below can otherwise repeat the same text processing millions of times.
    for row in rows:
        row["_project_tokens"] = p.extract_project_tokens(
            row.get("notes") or "", row.get("buyer") or "", row.get("seller") or ""
        )
    roots, canonical_status = {}, {}
    for row_id in rows_by_id:
        roots[row_id], canonical_status[row_id] = resolve_canonical(row_id, rows_by_id)

    candidate_rows = []
    review_by_pair = {}
    scale_rows = []

    def add_review(a, b, reason, analysis=None):
        key = tuple(sorted((int(a["id"]), int(b["id"]))))
        if analysis is None:
            analysis = build_pair_analysis(a, b, roots, canonical_status)
        existing = review_by_pair.get(key)
        if existing is None:
            existing = dict(analysis["base"])
            existing["review_reasons"] = reason
            review_by_pair[key] = existing
        else:
            old = existing.get("review_reasons", "")
            parts = [part for part in old.split(" || ") if part]
            if reason not in parts:
                parts.append(reason)
            existing["review_reasons"] = " || ".join(parts)

    print(f"Read {len(rows)} rows from deals (SQLite read-only mode).")
    for i, a in enumerate(rows):
        for b in rows[i + 1:]:
            analysis = build_pair_analysis(a, b, roots, canonical_status)
            root_a, root_b = roots[int(a["id"])], roots[int(b["id"])]

            # Only direct-orientation matches are promoted to the candidate CSV.
            # All other suspicious cases remain in the manual-review report.
            if analysis["direct_match"] and root_a != root_b:
                entry = dict(analysis["base"])
                entry["review_reasons"] = (
                    "Current matcher finds a likely pair in different canonical groups; "
                    "still requires human source review before any database change"
                )
                if entry["source_party_support_a"] != "both parties visible" or entry["source_party_support_b"] != "both parties visible":
                    entry["review_reasons"] += " || At least one stored 500-character excerpt does not show both counterparties"
                candidate_rows.append(entry)

            if analysis["reasons"]:
                add_review(a, b, " || ".join(analysis["reasons"]), analysis)

            if analysis["scale_anomaly"] and (analysis["direct_parties"] or analysis["reversed_parties"]):
                scale_rows.append({
                    "id_a": a.get("id"), "id_b": b.get("id"),
                    "buyer_a": a.get("buyer"), "buyer_b": b.get("buyer"),
                    "seller_a": a.get("seller"), "seller_b": b.get("seller"),
                    "country_a": a.get("country"), "country_b": b.get("country"),
                    "capacity_mw_a": a.get("capacity_mw"), "capacity_mw_b": b.get("capacity_mw"),
                    "capacity_ratio": round(capacity_ratio(a.get("capacity_mw"), b.get("capacity_mw")), 3),
                    "date_agreement_a": a.get("date_agreement"), "date_agreement_b": b.get("date_agreement"),
                    "source_url_a": a.get("source_url"), "source_url_b": b.get("source_url"),
                    "notes_a": a.get("notes"), "notes_b": b.get("notes"),
                    "review_reason": "Large capacity scale difference; interpret original source notation and units manually",
                })

    # Force the specifically reported October and follow-up examples into the
    # review report, even when the automated matcher does not connect them.
    for pair, reason in REQUIRED_REVIEW_PAIRS.items():
        id_a, id_b = pair
        a, b = rows_by_id.get(id_a), rows_by_id.get(id_b)
        if a is not None and b is not None:
            add_review(a, b, reason)
        else:
            review_by_pair[pair] = {
                "id_a": id_a, "id_b": id_b,
                "event_type_a": a.get("event_type", "") if a else "NOT_FOUND",
                "event_type_b": b.get("event_type", "") if b else "NOT_FOUND",
                "canonical_id_a": a.get("canonical_id", "") if a else "",
                "canonical_id_b": b.get("canonical_id", "") if b else "",
                "canonical_root_a": roots.get(id_a, "") if a else "",
                "canonical_root_b": roots.get(id_b, "") if b else "",
                "canonical_status_a": canonical_status.get(id_a, "ID_NOT_FOUND") if a else "ID_NOT_FOUND",
                "canonical_status_b": canonical_status.get(id_b, "ID_NOT_FOUND") if b else "ID_NOT_FOUND",
                "same_canonical_group": "unknown",
                "current_match_direct": "not tested - missing ID",
                "current_match_reversed": "not tested - missing ID",
                "review_reasons": reason,
            }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    candidate_rows.sort(key=lambda r: (int(r["id_a"]), int(r["id_b"])))
    review_rows = [review_by_pair[k] for k in sorted(review_by_pair)]
    scale_rows.sort(key=lambda r: (int(r["id_a"]), int(r["id_b"])))

    csv_write(OUTPUT_DIR / "candidate_duplicates.csv", PAIR_FIELDS, candidate_rows)
    csv_write(OUTPUT_DIR / "review_candidates.csv", PAIR_FIELDS, review_rows)
    csv_write(OUTPUT_DIR / "capacity_scale_anomalies.csv", SCALE_FIELDS, scale_rows)

    canonical_issue_rows = []
    for row_id, row in sorted(rows_by_id.items()):
        status = canonical_status[row_id]
        if status != "OK":
            item = {field: row.get(field) or "" for field in ROW_FIELDS}
            item.update({
                "id": row_id,
                "resolved_canonical_id": roots[row_id],
                "canonical_chain_status": status,
            })
            canonical_issue_rows.append(item)
    csv_write(OUTPUT_DIR / "canonical_issues.csv", CANONICAL_FIELDS, canonical_issue_rows)

    # Multiple DB rows with the same source URL are potential multi-deal
    # extractions from one article. The report keeps them visible together.
    by_url = defaultdict(list)
    for row in rows:
        url = (row.get("source_url") or "").strip()
        if url:
            by_url[url].append(row)
    article_group_rows = []
    for url, group in sorted(by_url.items()):
        if len(group) < 2:
            continue
        unique_projects = set()
        for row in group:
            unique_projects.update(project_tokens(row))
        article_group_rows.append({
            "source_url": url,
            "source_outlet": "; ".join(sorted({str(r.get("source_outlet") or "") for r in group if r.get("source_outlet")})),
            "record_count": len(group),
            "record_ids": ",".join(str(r["id"]) for r in group),
            "canonical_roots": ",".join(str(roots[int(r["id"])]) for r in group),
            "buyers": " | ".join(str(r.get("buyer") or "") for r in group),
            "sellers": " | ".join(str(r.get("seller") or "") for r in group),
            "countries": " | ".join(str(r.get("country") or "") for r in group),
            "capacities_mw": " | ".join(str(r.get("capacity_mw") or "") for r in group),
            "project_names_from_notes": ", ".join(sorted(unique_projects)),
            "review_reason": "Same article URL has multiple stored deal rows; verify each represents a distinct PPA and check for an aggregate/summary row",
        })
    csv_write(OUTPUT_DIR / "source_article_groups.csv", ARTICLE_GROUP_FIELDS, article_group_rows)

    # Link known IDs to candidate/review matches and canonical groups.
    candidates_by_id, reviews_by_id = {}, {}
    for row in candidate_rows:
        candidates_by_id.setdefault(int(row["id_a"]), set()).add(int(row["id_b"]))
        candidates_by_id.setdefault(int(row["id_b"]), set()).add(int(row["id_a"]))
    for row in review_rows:
        a_id, b_id = row.get("id_a"), row.get("id_b")
        if str(a_id).isdigit() and str(b_id).isdigit():
            reviews_by_id.setdefault(int(a_id), set()).add(int(b_id))
            reviews_by_id.setdefault(int(b_id), set()).add(int(a_id))

    known_rows = []
    for requested_id in KNOWN_IDS:
        row = rows_by_id.get(requested_id)
        if row is None:
            known_rows.append({
                "requested_id": requested_id, "found_in_database": "no",
                "canonical_chain_status": "ID_NOT_FOUND_IN_CURRENT_DB",
                "candidate_match_ids": "", "review_match_ids": "",
            })
            continue
        root = roots[requested_id]
        same_group_ids = sorted(
            other_id for other_id, other_root in roots.items()
            if other_id != requested_id and other_root == root
        )
        out = {
            "requested_id": requested_id,
            "found_in_database": "yes",
            "resolved_canonical_id": root,
            "canonical_chain_status": canonical_status[requested_id],
            "same_group_ids": ",".join(map(str, same_group_ids)),
            "candidate_match_ids": ",".join(map(str, sorted(candidates_by_id.get(requested_id, set())))),
            "review_match_ids": ",".join(map(str, sorted(reviews_by_id.get(requested_id, set())))),
        }
        for field in ROW_FIELDS:
            out[field] = row.get(field) or ""
        known_rows.append(out)
    csv_write(OUTPUT_DIR / "known_ids.csv", KNOWN_FIELDS, known_rows)

    summary = [
        "PPA Tracker historical duplicate audit (read-only)",
        f"Database rows inspected: {len(rows)}",
        f"Candidate pairs after project-identity and Pexapark safeguards: {len(candidate_rows)}",
        f"Pairs in manual review report: {len(review_rows)}",
        f"Large capacity-scale anomalies: {len(scale_rows)}",
        f"Canonical-link issues: {len(canonical_issue_rows)}",
        f"Source URLs with multiple stored rows: {len(article_group_rows)}",
        f"Requested IDs present in database: {sum(1 for r in known_rows if r.get('found_in_database') == 'yes')} / {len(KNOWN_IDS)}",
        "Capacity values were not automatically converted or guessed.",
        "Database opened with SQLite mode=ro; no database rows were modified.",
        "Candidate and review reports are investigation aids, not automatic merge instructions.",
    ]
    (OUTPUT_DIR / "audit_summary.txt").write_text("\n".join(summary) + "\n", encoding="utf-8")
    print("\n".join(summary))
    print(f"Reports written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
