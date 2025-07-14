#!/usr/bin/env python3
"""
Batch‐update public.reliability_indices from an Excel file, with diagnostics.

— Edit the CONFIG section below —
EXCEL_FILE : path to your .xlsx
SHEET_NAME : sheet name or index (default 0)
SKIPROWS   : rows to skip before the header
DB_•••     : your Postgres connection settings
— Then run:
      python update_reliability.py
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

# === CONFIGURATION ===
EXCEL_FILE = "reliability_data.xlsx"    # ← your Excel file
SHEET_NAME = "Sheet1"                     # ← sheet name or index
SKIPROWS   = 0                     # ← if your headers aren’t on row 0, set this to e.g. 1
DB_HOST    = "10.21.3.13"
DB_PORT    = 5432
DB_NAME    = "goalpara"
DB_USER    = "postgres"
DB_PASS    = "Kinshuk2214"
# =====================

def main():
    # 1) Load Excel, force all to string so leading zeros / letters survive
    df = pd.read_excel(
        EXCEL_FILE,
        sheet_name=SHEET_NAME,
        skiprows=SKIPROWS,
        dtype=str
    )

    # 2) Normalize headers
    df.columns = (
        df.columns
          .str.strip()
          .str.replace('\ufeff', '', regex=False)
          .str.lower()
    )

    # 3) meter_id must exist
    if "meter_id" not in df.columns:
        raise RuntimeError(f"Columns found: {df.columns.tolist()}\n"
                           "No 'meter_id'—check SKIPROWS/header row.")
    # 4) Strip whitespace from meter_id values
    df["meter_id"] = df["meter_id"].str.strip()

    # 5) Determine update columns
    update_cols = [c for c in df.columns if c != "meter_id"]
    if not update_cols:
        raise RuntimeError("No update columns found (only meter_id).")

    # 6) Connect to Postgres
    conn = psycopg2.connect(
        host     = DB_HOST,
        port     = DB_PORT,
        dbname   = DB_NAME,
        user     = DB_USER,
        password = DB_PASS
    )
    with conn:
        with conn.cursor() as cur:
            # 7) Fetch existing meter_ids for diagnostics
            cur.execute("SELECT meter_id FROM public.reliability_indices")
            existing = {r[0] for r in cur.fetchall()}

            df_ids   = set(df["meter_id"])
            match_ids   = df_ids & existing
            missing_ids = df_ids - existing

            print(f"Found in Excel: {len(df_ids)} unique meter_ids")
            print(f" → {len(match_ids)} match rows in DB")
            if missing_ids:
                print(f" → {len(missing_ids)} missing in DB, examples:", list(missing_ids)[:5])

            # 8) Prepare UPDATE for only matching rows
            to_update = df[df["meter_id"].isin(match_ids)]
            records   = to_update.to_dict(orient="records")

            set_clause = ", ".join(f"{col} = %({col})s" for col in update_cols)
            sql = f"""
                UPDATE public.reliability_indices
                   SET {set_clause}
                 WHERE meter_id = %(meter_id)s
            """

            if records:
                execute_batch(cur, sql, records, page_size=100)
                print(f"✔ Updated {cur.rowcount} rows.")
            else:
                print("⚠️  No records to update (no matching meter_ids).")

    conn.close()

if __name__ == "__main__":
    main()
