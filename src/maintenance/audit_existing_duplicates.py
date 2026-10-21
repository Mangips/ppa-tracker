
import csv
import sys
from pathlib import Path

# Make src/pipeline.py importable when this script runs in GitHub Actions.
SRC_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SRC_DIR))

import pipeline as p


OUTPUT_PATH = p.DATA_DIR / "duplicate_audit_report.csv"

COLUMNS = [
    "earlier_id",
    "later_id",
    "buyer_earlier",
    "buyer_later",
    "seller_earlier",
    "seller_later",
    "country_earlier",
    "country_later",
    "capacity_mw_earlier",
    "capacity_mw_later",
    "date_agreement_earlier",
    "date_agreement_later",
    "earlier_source_url",
    "later_source_url",
    "earlier_source_outlet",
    "later_source_outlet",
    "earlier_notes",
    "later_notes",
]


def main():
    if not p.DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {p.DB_PATH}"
        )

    conn = p.sqlite3.connect(p.DB_PATH)
    conn.row_factory = p.sqlite3.Row

    try:
        rows = [
            dict(row)
            for row in conn.execute("""
                SELECT *
                FROM latest_deals
                ORDER BY id
            """).fetchall()
        ]
    finally:
        conn.close()

    print(f"Loaded {len(rows)} latest deal records.")
    matches = []

    # Compare every pair. The existing matching function is reused unchanged.
    # This script only reads the database and writes a separate CSV report.
    for i, later in enumerate(rows):
        for earlier in rows[:i]:
            if p.find_similar_duplicate([earlier], later) is None:
                continue

            matches.append({
                "earlier_id": earlier["id"],
                "later_id": later["id"],
                "buyer_earlier": earlier.get("buyer"),
                "buyer_later": later.get("buyer"),
                "seller_earlier": earlier.get("seller"),
                "seller_later": later.get("seller"),
                "country_earlier": earlier.get("country"),
                "country_later": later.get("country"),
                "capacity_mw_earlier": earlier.get("capacity_mw"),
                "capacity_mw_later": later.get("capacity_mw"),
                "date_agreement_earlier": earlier.get("date_agreement"),
                "date_agreement_later": later.get("date_agreement"),
                "earlier_source_url": earlier.get("source_url"),
                "later_source_url": later.get("source_url"),
                "earlier_source_outlet": earlier.get("source_outlet"),
                "later_source_outlet": later.get("source_outlet"),
                "earlier_notes": earlier.get("notes"),
                "later_notes": later.get("notes"),
            })

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(matches)

    print(f"Audit complete. Candidate pairs: {len(matches)}")
    print(f"Report created: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
