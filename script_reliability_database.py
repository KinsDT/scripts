import pandas as pd
import psycopg2

# === CONFIG ===
XLSX_PATH = "reliability_data.xlsx"   # ← your Excel file
SHEET     = "Sheet1"                  # ← your sheet name
DB_CONFIG = {
    "dbname":   "meter_info",
    "user":     "postgres",
    "password": "Kinshuk2214",
    "host":     "10.21.3.13",
    "port":     "5432",
}

# === 1. LOAD & NORMALIZE ===
df = pd.read_excel(XLSX_PATH, sheet_name=SHEET)

# Normalize headers: strip whitespace & lowercase
df.columns = df.columns.str.strip().str.lower()

# === 2. SELECT & VERIFY COLUMNS ===
wanted = [
    "meter_id",
    "ca", "cb", "cc",
    "la", "lb", "lc",
    "na", "nb", "nc",
    "ma", "mb", "mc",
    "da", "db", "dc",
    "ta", "tb", "tc",
    "area"
]

missing = [c for c in wanted if c not in df.columns]
if missing:
    raise KeyError(f"Missing columns in Excel sheet: {missing}")

insert_df = df[wanted].copy()

# === 3. CONNECT & INSERT ===
conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()

# Prepare data as list of tuples
records = [tuple(row) for row in insert_df.to_numpy()]

# Bulk‐insert
cur.executemany("""
    INSERT INTO reliability_database (
      meter_id,
      ca, cb, cc,
      la, lb, lc,
      na, nb, nc,
      ma, mb, mc,
      da, db, dc,
      ta, tb, tc,
      area
    ) VALUES (
      %s,  %s, %s, %s,
           %s, %s, %s,
           %s, %s, %s,
           %s, %s, %s,
           %s, %s, %s,
           %s, %s, %s,
           %s
    )
""", records)

conn.commit()
cur.close()
conn.close()

print(f"✅ Inserted {len(records)} rows into reliability_database.")
