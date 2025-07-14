import os
from io import StringIO
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import pandas as pd
import psycopg2

# ─── CONFIG ──────────────────────────────────────────────────────────────
BASE_DIR    = Path(r"C:/Users/KinshukGoswami/OneDrive - Sinhal Udyog pvt ltd/Desktop/Lakhipur_bigD")
FILE_NAMES  = [f"CT LS {i}.xlsx" for i in range(1, 8)]
SHEET_NAME  = 0

DB_NAME     = "goalpara"
DB_USER     = "postgres"
DB_PASSWORD = "Kinshuk2214"
DB_HOST     = "localhost"
DB_PORT     = "5432"

TARGET_TABLE        = "meter_mapping"
COLUMNS             = ["meter_id", "time_interval"]
TIME_INTERVAL_VALUE = 30
# ─────────────────────────────────────────────────────────────────────────

def load_ids(path: Path, sheet_name=0):
    df = pd.read_excel(
        path,
        sheet_name=sheet_name,
        usecols=["Meter_id"],
        dtype={"Meter_id": str},
    )
    return df["Meter_id"].dropna().str.strip().unique().tolist()

def insert_new_ids(unique_ids):
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, host=DB_HOST,
        port=DB_PORT
    )
    try:
        with conn:
            with conn.cursor() as cur:
                # 1) fetch what's already there
                cur.execute(f"SELECT meter_id FROM {TARGET_TABLE}")
                existing = {row[0] for row in cur.fetchall()}

                # 2) compute only the new ones
                to_insert = sorted(set(unique_ids) - existing)
                if not to_insert:
                    print("ℹ️  No new meter_ids to insert.")
                    return

                # 3) prepare in-memory CSV of just new rows
                buf = StringIO()
                writer = __import__('csv').writer(buf)
                for mid in to_insert:
                    writer.writerow((mid, TIME_INTERVAL_VALUE))
                buf.seek(0)

                # 4) bulk-load them
                cur.copy_from(
                    buf,
                    TARGET_TABLE,
                    sep=",",
                    null="",
                    columns=COLUMNS
                )
                print(f"✅ Inserted {len(to_insert)} new meter_ids.")
    finally:
        conn.close()

def main():
    paths = [BASE_DIR / name for name in FILE_NAMES]
    all_ids = set()

    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(load_ids, p, SHEET_NAME): p for p in paths}
        for fut in as_completed(futures):
            p = futures[fut]
            try:
                ids = fut.result()
                print(f"📄 {p.name}: {len(ids)} IDs")
                all_ids.update(ids)
            except Exception as e:
                print(f"❌ {p.name}: {e}")

    print(f"🔢 Total unique Meter_id count: {len(all_ids)}")
    insert_new_ids(all_ids)

if __name__ == "__main__":
    main()
