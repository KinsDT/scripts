import re
from io import StringIO
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import psycopg2
from psycopg2 import sql

# ─── CONFIG ──────────────────────────────────────────────────────────────
BASE_DIR = Path(r"C:/Users/KinshukGoswami/OneDrive - Sinhal Udyog pvt ltd/Desktop/Lakhipur_bigD_2")

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "meter_info",
    "user":     "postgres",
    "password": "Kinshuk2214"
}

# exact columns to pull from Excel and to define in each table
COLUMNS = [
    "Meter_id", "Time",
    "Rcurrent", "Ycurrent", "Bcurrent",
    "Rvoltage", "Yvoltage", "Bvoltage",
    "Rpowerfac", "Ypowerfac", "Bpowerfac",
    "CumEimportKwh", "CumEexportKwh",
    "CumEimportKvah", "CumEexportKvah",
    "Kvarh Q1", "Kvarh Q2", "Kvarh Q3", "Kvarh Q4"
]

# map pandas dtypes to Postgres column types
PG_TYPES = {
    "Meter_id":      "VARCHAR(20)",
    "Time":          "TIMESTAMP",
    **{col: "DOUBLE PRECISION" for col in COLUMNS if col not in ("Meter_id","Time")}
}
# ─────────────────────────────────────────────────────────────────────────

def sanitize_table_name(meter_id: str) -> str:
    safe = re.sub(r'\W+', '_', meter_id)
    return f"meter_{safe.lower()}"

def load_file(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, usecols=COLUMNS, parse_dates=["Time"], engine="openpyxl")
    return df[df["Meter_id"].notna()]

def create_table_and_load(conn, meter_id: str, df: pd.DataFrame):
    table_name = sanitize_table_name(meter_id)
    with conn.cursor() as cur:
        # create table if it doesn’t exist
        cols_defs = [
            sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(PG_TYPES[c]))
            for c in COLUMNS
        ]
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});")
               .format(sql.Identifier(table_name), sql.SQL(", ").join(cols_defs))
        )

        # bulk load via COPY
        buf = StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        cur.copy_expert(
            sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, COLUMNS))
            ),
            buf
        )
    conn.commit()
    print(f"✅ Loaded {len(df)} rows into {table_name}")

def main():
    # 1) Sanity-check folder contents
    print("Looking in:", BASE_DIR.resolve())
    print("Contents:")
    for p in sorted(BASE_DIR.iterdir()):
        print("  ", p.name)
    print()

    # 2) Glob for CT LS 1.xlsx … CT LS 7.xlsx
    files = sorted(
        BASE_DIR.glob("ct-*.xlsx"),
        key=lambda p: int(re.search(r"ct-(\d+)\.xlsx", p.name).group(1))
    )
    if not files:
        print("❌ No CT LS *.xlsx files found. Check BASE_DIR.")
        return

    print("Will process:")
    for f in files:
        print("  -", f.name)
    print()

    # 3) Load in parallel
    dfs = []
    with ProcessPoolExecutor() as exe:
        future_map = {exe.submit(load_file, f): f for f in files}
        for fut in as_completed(future_map):
            f = future_map[fut]
            try:
                df = fut.result()
                print(f"📄 {f.name}: {len(df)} rows")
                dfs.append(df)
            except Exception as e:
                print(f"❌ {f.name}: {e}")

    if not dfs:
        print("❌ No data loaded—aborting.")
        return

    # 4) Concatenate & group by Meter_id
    full_df = pd.concat(dfs, ignore_index=True)
    for meter_id, group in full_df.groupby("Meter_id"):
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            create_table_and_load(conn, meter_id, group)
        finally:
            conn.close()

if __name__ == "__main__":
    main()
