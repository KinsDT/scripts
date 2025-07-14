import os, csv
from io import StringIO
from pathlib import Path
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import psycopg2

# ─── CONFIG ──────────────────────────────────────────────────────────────
BASE_DIR = Path(r"C:/Users/KinshukGoswami/OneDrive - Sinhal Udyog pvt ltd/Desktop/Lakhipur_bigD_2")
FILE_NAMES   = [f"ct-{i}.xlsx" for i in range(1, 7)]
SHEET_NAME   = 0

DB_NAME      = "goalpara"
DB_USER      = "postgres"
DB_PASSWORD  = "Kinshuk2214" 
DB_HOST      = "localhost"
DB_PORT      = "5432"
TARGET_TABLE = "operational_template"

COLUMNS = [
    "meter_id", "kva_rating",
    "active_md_import", "active_md_import_time",
    "active_md_export", "active_md_export_time",
    "apparent_md_import", "apparent_md_import_time",
    "apparent_md_export", "apparent_md_export_time"
]
# ─────────────────────────────────────────────────────────────────────────

def load_and_compute(path: Path, sheet_name=0):
    # 1. Read & filter
    cols = [
      'Meter_id', 'Time',
      'CumEimportKwh', 'CumEexportKwh',
      'CumEimportKvah', 'CumEexportKvah'
    ]
    df = pd.read_excel(
        path, sheet_name=sheet_name,
        usecols=cols,
        parse_dates=['Time'],
        date_parser=lambda x: pd.to_datetime(x, format='%d:%m:%Y %H:%M:%S')
    )
    df = df[(df['Time'].dt.year == 2025) & (df['Time'].dt.month == 5)]

    # 2. Find the idxmax for each metric per meter
    grp = df.groupby('Meter_id')
    idx_ai = grp['CumEimportKwh'].idxmax()
    idx_ae = grp['CumEexportKwh'].idxmax()
    idx_pi = grp['CumEimportKvah'].idxmax()
    idx_pe = grp['CumEexportKvah'].idxmax()

    # 3. Build a DataFrame of results
    out = pd.DataFrame({
      'meter_id':    idx_ai.index,
      'kva_rating':  None,
      'active_md_import':        df.loc[idx_ai, 'CumEimportKwh'].values,
      'active_md_import_time':   df.loc[idx_ai, 'Time'].values,
      'active_md_export':        df.loc[idx_ae, 'CumEexportKwh'].values,
      'active_md_export_time':   df.loc[idx_ae, 'Time'].values,
      'apparent_md_import':      df.loc[idx_pi, 'CumEimportKvah'].values,
      'apparent_md_import_time': df.loc[idx_pi, 'Time'].values,
      'apparent_md_export':      df.loc[idx_pe, 'CumEexportKvah'].values,
      'apparent_md_export_time': df.loc[idx_pe, 'Time'].values,
    })

    # 4. Return list of tuples ready for CSV/INSERT
    return list(out.itertuples(index=False, name=None))


def insert_via_copy(records):
    # Dump to in-memory CSV
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerows(records)
    buf.seek(0)

    conn = psycopg2.connect(
      dbname=DB_NAME, user=DB_USER,
      password=DB_PASSWORD, host=DB_HOST,
      port=DB_PORT
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE {TARGET_TABLE};")
                cur.copy_from(
                  buf,
                  TARGET_TABLE,
                  sep=',',
                  null='', 
                  columns=COLUMNS
                )
        print(f"✅ Inserted {len(records)} rows via COPY")
    finally:
        conn.close()


def main():
    paths = [BASE_DIR / name for name in FILE_NAMES]
    all_recs = []

    # Use processes instead of threads
    with ProcessPoolExecutor(max_workers=min(len(paths), cpu_count())) as exe:
        futures = {exe.submit(load_and_compute, p, SHEET_NAME): p for p in paths}
        for fut in as_completed(futures):
            p = futures[fut]
            try:
                recs = fut.result()
                print(f"📄 {p.name} → {len(recs)} meters")
                all_recs.extend(recs)
            except Exception as e:
                print(f"❌ {p.name}: {e}")

    # Single bulk load via COPY
    insert_via_copy(all_recs)


if __name__ == "__main__":
    main()

'''BEGIN;

-- 1) Build a temp table of exactly one row per meter
CREATE TEMP TABLE tmp_operational_template AS
WITH meters AS (
  SELECT DISTINCT meter_id
  FROM public.operational_template
)
SELECT
  m.meter_id,

  imp.max_import           AS active_md_import,
  imp.max_import_time      AS active_md_import_time,

  exp.max_export           AS active_md_export,
  exp.max_export_time      AS active_md_export_time,

  aim.max_app_import       AS apparent_md_import,
  aim.max_app_import_time  AS apparent_md_import_time,

  aex.max_app_export       AS apparent_md_export,
  aex.max_app_export_time  AS apparent_md_export_time

FROM meters m

-- highest active_md_import
LEFT JOIN LATERAL (
  SELECT
    active_md_import      AS max_import,
    active_md_import_time AS max_import_time
  FROM public.operational_template o
  WHERE o.meter_id = m.meter_id
  ORDER BY active_md_import DESC
  LIMIT 1
) imp ON TRUE

-- highest active_md_export
LEFT JOIN LATERAL (
  SELECT
    active_md_export      AS max_export,
    active_md_export_time AS max_export_time
  FROM public.operational_template o
  WHERE o.meter_id = m.meter_id
  ORDER BY active_md_export DESC
  LIMIT 1
) exp ON TRUE

-- highest apparent_md_import
LEFT JOIN LATERAL (
  SELECT
    apparent_md_import       AS max_app_import,
    apparent_md_import_time  AS max_app_import_time
  FROM public.operational_template o
  WHERE o.meter_id = m.meter_id
  ORDER BY apparent_md_import DESC
  LIMIT 1
) aim ON TRUE

-- highest apparent_md_export
LEFT JOIN LATERAL (
  SELECT
    apparent_md_export       AS max_app_export,
    apparent_md_export_time  AS max_app_export_time
  FROM public.operational_template o
  WHERE o.meter_id = m.meter_id
  ORDER BY apparent_md_export DESC
  LIMIT 1
) aex ON TRUE
;

-- 2) Wipe out all the old detail rows
TRUNCATE TABLE public.operational_template;

-- 3) Insert the consolidated one-row-per-meter back into the real table
INSERT INTO public.operational_template (
  meter_id,
  active_md_import,           active_md_import_time,
  active_md_export,           active_md_export_time,
  apparent_md_import,         apparent_md_import_time,
  apparent_md_export,         apparent_md_export_time
)
SELECT
  meter_id,
  active_md_import,           active_md_import_time,
  active_md_export,           active_md_export_time,
  apparent_md_import,         apparent_md_import_time,
  apparent_md_export,         apparent_md_export_time
FROM tmp_operational_template
ORDER BY meter_id
;

COMMIT;
'''