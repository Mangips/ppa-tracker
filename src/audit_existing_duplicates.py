#!/usr/bin/env python3
"""Read-only audit of historical PPA records and duplicate candidates."""

import csv
import itertools
import os
import sqlite3
import sys
from functools import lru_cache
from pathlib import Path

# Import pipeline helpers without calling run() or making API requests.
os.environ.setdefault("NEWSAPI_KEY", "AUDIT_MODE_NO_API_CALLS")
os.environ.setdefault("ENVIRONMENT", "AUDIT")
SRC_DIR = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
(REPO_ROOT / "data" / "logs").mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(SRC_DIR))
import pipeline as p  # noqa: E402

OUTPUT_DIR = p.DATA_DIR / "duplicate_audit"

KNOWN_IDS = [
    1986, 1979, 1978, 1977, 1947,
    1900, 1899, 1939, 1917, 203, 89,
    793, 252, 1270, 1199, 1926, 1844, 1843, 1834,
    1991, 1863, 1884, 1885, 1867, 1897, 1891,
]

PEXAPARK_IDS = [1986, 1979, 1978, 1977, 1947]
APPARENTLY_SIMILAR_IDS = [793, 252, 1270, 1199, 1926, 1844, 1843, 1834]
INCOMPLETE_IDS = [1991, 1863, 1884]

# User-supplied cases that must be visible in the review report even if the
# automated matcher does not connect them.
REQUIRED_REVIEW_PAIRS = {}
for pair in itertools.combinations(PEXAPARK_IDS, 2):
    REQUIRED_REVIEW_PAIRS[tuple(sorted(pair))] = (
        "PEXAPARK_PROTECTION: reported as two distinct deals (150 MW and 31 MW); "
        "review capacities and do not merge this whole group into one deal"
    )
for pair in itertools.combinations(APPARENTLY_SIMILAR_IDS, 2):
    REQUIRED_REVIEW_PAIRS[tuple(sorted(pair))] = (
        "USER_REVIEW_GROUP: apparently similar October records; relationship not confirmed"
    )
for pair in itertools.combinations(INCOMPLETE_IDS, 2):
    REQUIRED_REVIEW_PAIRS[tuple(sorted(pair))] = (
        "INCOMPLETE_RECORDS: compare missing fields and decide whether each record is useful"
    )
REQUIRED_REVIEW_PAIRS.update({
    tuple(sorted((1900, 1899))): "USER_EXPECTED_SAME_DEAL: verify the current matcher detects this pair",
    tuple(sorted((1939, 1917))): "USER_SUSPECTED_DUPLICATE: check possible 1,000x capacity-unit discrepancy",
    tuple(sorted((203, 89))): "USER_SUSPECTED_SAME_DEAL: verify against source descriptions",
    tuple(sorted((1885, 1867))): "USER_REVIEW_ONLY: may be two deals; do not assume they are duplicates",
    tuple(sorted((1897, 1891))): "USER_EXPECTED_SAME_DEAL: compare sparse and fuller record",
})

PAIR_FIELDS = [
    "id_a", "id_b", "event_type_a", "event_type_b",
    "canonical_id_a", "canonical_id_b", "canonical_root_a", "canonical_root_b",
    "canonical_status_a", "canonical_status_b", "same_canonical_group",
    "buyer_a", "buyer_b", "seller_a", "seller_b", "country_a", "country_b",
    "capacity_mw_a", "capacity_mw_b", "energy_gwh_a", "energy_gwh_b",
    "tenure_years_a", "tenure_years_b", "date_agreement_a", "date_agreement_b",
    "date_found_a", "date_found_b", "source_outlet_a", "source_outlet_b",
    "source_url_a", "source_url_b", "notes_a", "notes_b",
    "raw_snippet_a", "raw_snippet_b",
    "buyer_similarity_direct", "seller_similarity_direct",
    "buyer_seller_similarity_reverse", "seller_buyer_similarity_reverse",
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
    "price_eur_mwh", "source_url", "source_outlet", "publication_date", "notes",
    "raw_snippet",
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
        parent = row.get("canonical_id")
        if missing(parent):
            event_type = row.get("event_type")
            if event_type in ("U", "D"):
                return current, f"WARNING: {event_type} row has no canonical_id"
            return current, "OK"
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

    # The current matcher requires an actual country overlap; missing-country
    # cases are review-only and are never treated as a current-rule match.
    direct_match = False
    reverse_match = False
    if direct_parties and country_overlap:
        direct_match = p.find_similar_duplicate([a], b) is not None
    if reversed_parties and country_overlap:
        swapped = dict(b)
        swapped["buyer"], swapped["seller"] = b.get("seller"), b.get("buyer")
        reverse_match = p.find_similar_duplicate([a], swapped) is not None

    cap_a, cap_b = numeric(a.get("capacity_mw")), numeric(b.get("capacity_mw"))
    ratio = None
    if cap_a is not None and cap_b is not None and min(cap_a, cap_b) > 0:
        ratio = max(cap_a, cap_b) / min(cap_a, cap_b)

    reasons = []
    parties_align = direct_parties or reversed_parties
    if parties_align and countries_compatible_for_review:
        if reverse_match and not direct_match:
            reasons.append("Possible reversed buyer/seller orientation; current matcher is directional")
        if missing(a.get("capacity_mw")) or missing(b.get("capacity_mw")):
            reasons.append("Matching parties but capacity missing on at least one row")
        if ratio is not None and ratio >= 100:
            reasons.append(f"Possible capacity-unit error: capacities differ by {ratio:.1f}x")
        notes_tokens_a = p.extract_project_tokens(a.get("notes") or "")
        notes_tokens_b = p.extract_project_tokens(b.get("notes") or "")
        shared_project_token = bool(notes_tokens_a & notes_tokens_b)
        cap_close = p._capacity_close(a.get("capacity_mw"), b.get("capacity_mw"))
        if cap_close is False and shared_project_token:
            reasons.append("Different capacities but project-name tokens overlap; verify extraction/units")

    root_a, root_b = roots[int(a["id"])], roots[int(b["id"]) ]
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
        "direct_parties": direct_parties,
        "reversed_parties": reversed_parties,
        "countries_compatible": countries_compatible_for_review,
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
    roots, canonical_status = {}, {}
    for row_id in rows_by_id:
        roots[row_id], canonical_status[row_id] = resolve_canonical(row_id, rows_by_id)

    candidate_rows = []
    review_by_pair = {}

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

    print(f"Read {len(rows)} rows from deals (read-only mode).")
    for i, a in enumerate(rows):
        for b in rows[i + 1:]:
            analysis = build_pair_analysis(a, b, roots, canonical_status)
            root_a, root_b = roots[int(a["id"])], roots[int(b["id"]) ]
            if analysis["direct_match"] and root_a != root_b:
                entry = dict(analysis["base"])
                entry["review_reasons"] = "Current matcher matches direct buyer/seller order, but records belong to different canonical groups"
                candidate_rows.append(entry)

            if analysis["reasons"]:
                add_review(a, b, " || ".join(analysis["reasons"]), analysis)

    # Force the specifically reported October examples into the report, even if
    # current fields/rules fail to connect them. Missing IDs are reported below.
    for pair, reason in REQUIRED_REVIEW_PAIRS.items():
        id_a, id_b = pair
        a, b = rows_by_id.get(id_a), rows_by_id.get(id_b)
        if a is not None and b is not None:
            add_review(a, b, reason)
        else:
            # Use a lightweight report row so missing IDs are still visible in review.
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
                "buyer_a": a.get("buyer", "") if a else "", "buyer_b": b.get("buyer", "") if b else "",
                "seller_a": a.get("seller", "") if a else "", "seller_b": b.get("seller", "") if b else "",
                "country_a": a.get("country", "") if a else "", "country_b": b.get("country", "") if b else "",
                "capacity_mw_a": a.get("capacity_mw", "") if a else "", "capacity_mw_b": b.get("capacity_mw", "") if b else "",
                "current_match_direct": "not tested - missing ID", "current_match_reversed": "not tested - missing ID",
                "review_reasons": reason,
            }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    candidate_rows.sort(key=lambda r: (int(r["id_a"]), int(r["id_b"])))
    review_rows = [review_by_pair[k] for k in sorted(review_by_pair)]

    csv_write(OUTPUT_DIR / "candidate_duplicates.csv", PAIR_FIELDS, candidate_rows)
    csv_write(OUTPUT_DIR / "review_candidates.csv", PAIR_FIELDS, review_rows)

    # Link all known IDs to existing rows, including D rows hidden by latest_deals.
    candidates_by_id, reviews_by_id = {}, {}
    for row in candidate_rows:
        candidates_by_id.setdefault(int(row["id_a"]), set()).add(int(row["id_b"]))
        candidates_by_id.setdefault(int(row["id_b"]), set()).add(int(row["id_a"]))
    for row in review_rows:
        a_id, b_id = row.get("id_a"), row.get("id_b")
        if isinstance(a_id, int) or (isinstance(a_id, str) and a_id.isdigit()):
            reviews_by_id.setdefault(int(a_id), set()).add(int(b_id))
        if isinstance(b_id, int) or (isinstance(b_id, str) and b_id.isdigit()):
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
        f"Candidate duplicate pairs in different canonical groups: {len(candidate_rows)}",
        f"Pairs in manual review report: {len(review_rows)}",
        f"Requested IDs present in database: {sum(1 for r in known_rows if r.get('found_in_database') == 'yes')} / {len(KNOWN_IDS)}",
        "Database opened using SQLite mode=ro; no database rows were modified.",
        "Review candidates are leads for human inspection, not automatic merge instructions.",
    ]
    (OUTPUT_DIR / "audit_summary.txt").write_text("\n".join(summary) + "\n", encoding="utf-8")
    print("\n".join(summary))
    print(f"Reports written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
