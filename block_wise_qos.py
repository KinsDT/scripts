import os
from pathlib import Path
from io import StringIO
import csv
import pandas as pd
import psycopg2
from datetime import datetime
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed

# ─── CONFIG ──────────────────────────────────────────────────────────────
BASE_DIR = Path(r"C:/Users/KinshukGoswami/OneDrive - Sinhal Udyog pvt ltd/Desktop/Lakhipur_bigD_2")
FILE_NAMES = [f for f in os.listdir(BASE_DIR) if f.endswith(".xlsx")]
SHEET_NAME = 0

DB_NAME = "goalpara"
DB_USER = "postgres"
DB_PASSWORD = "Kinshuk2214"
DB_HOST = "localhost"
DB_PORT = "5432"
TARGET_TABLE = "block_wise_qos_template"

COLUMNS = [
    'date', 'block', 'pfavg3ph', 'pfph_a', 'pfph_b', 'pfph_c',
    'v3ph_avg_percent', 'va_avg_percent', 'vb_avg_percent', 'vc_avg_percent',
    'v3ph_max_percent', 'va_max_percent', 'vb_max_percent', 'vc_max_percent',
    'vu_percent', 'iu_percent', 'meter_id'
]
# ─────────────────────────────────────────────────────────────────────────


def load_and_compute(file_path: Path):
    df = pd.read_excel(file_path)
    df['Time'] = pd.to_datetime(df['Time'])
    df['date'] = df['Time'].dt.date
    df['block'] = df['Time'].dt.hour * 2 + df['Time'].dt.minute // 30 + 1

    df.rename(columns={
        'Meter_id': 'meter_id',
        'Rcurrent': 'Ir',
        'Ycurrent': 'Iy',
        'Bcurrent': 'Ib',
        'Rvoltage': 'Vr',
        'Yvoltage': 'Vy',
        'Bvoltage': 'Vb',
        'Rpowerfac': 'PFr',
        'Ypowerfac': 'PFy',
        'Bpowerfac': 'PFb'
    }, inplace=True)

    df['pfavg3ph'] = df[['PFr', 'PFy', 'PFb']].mean(axis=1)
    df['v3ph_avg'] = df[['Vr', 'Vy', 'Vb']].mean(axis=1)
    df['v3ph_max'] = 0

    nominal_voltage = 240
    df['v3ph_avg_percent'] = ((df['v3ph_avg'] - nominal_voltage) / nominal_voltage) * 100
    df['va_avg_percent'] = ((df['Vr'] - nominal_voltage) / nominal_voltage) * 100
    df['vb_avg_percent'] = ((df['Vy'] - nominal_voltage) / nominal_voltage) * 100
    df['vc_avg_percent'] = ((df['Vb'] - nominal_voltage) / nominal_voltage) * 100

    df['va_max_percent'] = 0
    df['vb_max_percent'] = 0
    df['vc_max_percent'] = 0

    df['va_dev'] = ((df["Vr"] - df["v3ph_avg"]) / df["v3ph_avg"]) * 100
    df['vb_dev'] = ((df["Vy"] - df["v3ph_avg"]) / df["v3ph_avg"]) * 100
    df['vc_dev'] = ((df["Vb"] - df["v3ph_avg"]) / df["v3ph_avg"]) * 100
    df['vu_percent'] = df[['va_dev', 'vb_dev', 'vc_dev']].abs().max(axis=1)

    df['iu_percent'] = df[['Ir', 'Iy', 'Ib']].apply(
        lambda row: max(abs((row - row.mean()) / row.mean()) * 100) if row.mean() != 0 else 0,
        axis=1
    )

    final_df = df[[
        'date', 'block', 'pfavg3ph', 'PFr', 'PFy', 'PFb',
        'v3ph_avg_percent', 'va_avg_percent', 'vb_avg_percent', 'vc_avg_percent',
        'v3ph_max', 'va_max_percent', 'vb_max_percent', 'vc_max_percent',
        'vu_percent', 'iu_percent', 'meter_id'
    ]]
    return list(final_df.itertuples(index=False, name=None))


def insert_via_copy(records):
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
    all_records = []

    with ProcessPoolExecutor(max_workers=min(len(paths), cpu_count())) as executor:
        futures = {executor.submit(load_and_compute, path): path.name for path in paths}
        for future in as_completed(futures):
            name = futures[future]
            try:
                recs = future.result()
                print(f"📄 Processed {name} → {len(recs)} records")
                all_records.extend(recs)
            except Exception as e:
                print(f"❌ Error processing {name}: {e}")

    insert_via_copy(all_records)


if __name__ == "__main__":
    main()
