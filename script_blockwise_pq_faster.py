import pandas as pd
import numpy as np
import math
import psycopg2
import io

# === CONFIGURATION ===
CSV_PATH   = "CT-LS.csv"      # ← replace with your CSV file path
TABLE_NAME = "block_wise_pq_template"
DB_CONFIG  = {
    'dbname': 'goalpara',
    'user': 'postgres',
    'password': 'Kinshuk2214',
    'host': '10.21.3.13',
    'port': '5432'
}
CHUNK_SIZE = 200_000  # fits easily in 32 GB RAM; tune higher if you like

# Precompute the ideal 120° phasor multipliers
a  = complex(-0.5,  math.sqrt(3)/2)   # e^(j*120°)
a2 = complex(-0.5, -math.sqrt(3)/2)   # e^(-j*120°)

# Open DB connection once
conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()

# Stream CSV in chunks
for chunk in pd.read_csv(
        CSV_PATH,
        chunksize=CHUNK_SIZE,
        parse_dates=['RTC'],  # your timestamp column
        dayfirst=True,
        dtype={'Meter_id': str, 'Vr': float, 'Vy': float, 'Vb': float}
    ):
    # 1) Extract record_date and block_name
    chunk['record_date'] = chunk['RTC'].dt.date
    chunk['block_name']  = chunk['RTC'].dt.strftime('%H:%M')

    # 2) Vectorized phasors for ideal 120° separation
    Va = chunk['Vr'].values
    Vb = chunk['Vy'].values * a2
    Vc = chunk['Vb'].values * a

    # 3) Compute symmetrical components
    V0 = (Va + Vb + Vc) / 3
    V1 = (Va + a * Vb + a2 * Vc) / 3
    V2 = (Va + a2 * Vb + a * Vc) / 3

    # 4) Store magnitudes & VUF
    chunk['v0']  = np.abs(V0)
    chunk['v1']  = np.abs(V1)
    chunk['v2']  = np.abs(V2)
    chunk['vuf'] = np.where(chunk['v1'] > 0, (chunk['v2'] / chunk['v1']) * 100, 0)

    # 5) Prepare only the columns you insert, in the right order
    insert_df = chunk[[
        'record_date', 'Meter_id',
        'Vr', 'Vy', 'Vb',
        'v1', 'v2', 'v0', 'vuf',
        'block_name'
    ]].copy()
    insert_df.columns = [
        'record_date', 'meter_id',
        'voltage_pha', 'voltage_phb', 'voltage_phc',
        'v1', 'v2', 'v0', 'vuf',
        'block_name'
    ]

    # 6) Bulk-load via COPY
    buf = io.StringIO()
    insert_df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_from(
        buf,
        TABLE_NAME,
        sep=',',
        columns=insert_df.columns.tolist()
    )
    conn.commit()  # commit per chunk

cur.close()
conn.close()
print("✅ All chunks loaded into block_wise_pq_template.")
