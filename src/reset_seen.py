import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "ppa_deals.db"
conn = sqlite3.connect(DB_PATH)
conn.execute("DELETE FROM seen_urls")
conn.commit()
count = conn.execute("SELECT COUNT(*) FROM seen_urls").fetchone()[0]
print(f"seen_urls cleared. Remaining rows: {count}")
conn.close()
