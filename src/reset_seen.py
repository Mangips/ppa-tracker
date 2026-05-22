import sqlite3
from pathlib import Path
import os

DB_PATH = Path(__file__).parent.parent / "data" / "ppa_deals.db"
CSV_PATH = Path(__file__).parent.parent / "data" / "ppa_deals.csv"

conn = sqlite3.connect(DB_PATH)
conn.execute("DELETE FROM seen_urls")  
conn.execute("DELETE FROM deals")    
conn.execute("DELETE FROM sqlite_sequence WHERE name='deals'")
conn.commit()

# ✅ NEW: Delete CSV
if CSV_PATH.exists():
    os.remove(CSV_PATH)
    print(f"Deleted CSV: {CSV_PATH}")

count = conn.execute("SELECT COUNT(*) FROM seen_urls").fetchone()[0]
print(f"seen_urls cleared. Remaining rows: {count}")
conn.close()
