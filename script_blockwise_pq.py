import pandas as pd
import numpy as np
import math
import psycopg2

# === CONFIGURATION ===
EXCEL_PATH = "CT-LS.xlsx"  # Replace with actual file path
SHEET_NAME = "CT-LS"                # Replace with actual sheet name
DB_CONFIG = {
    'dbname': 'goalpara',
    'user': 'postgres',
    'password': 'Kinshuk2214',
    'host': '10.21.3.13',
    'port': '5432'
}

# === LOAD DATA ===
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

# Rename or verify datetime column
df.rename(columns={'RTC Time': 'RTC'}, inplace=True)  # Adjust if needed
df['RTC'] = pd.to_datetime(df['RTC'], dayfirst=True)

# === DERIVED FIELDS ===
df['record_date'] = df['RTC'].dt.date
df['block_name'] = df['RTC'].dt.strftime('%H:%M')

# Ideal 3-phase unit phasors
a  = complex(-0.5,  math.sqrt(3)/2)   # e^(j*120°)
a2 = complex(-0.5, -math.sqrt(3)/2)   # e^(-j*120°)

# Compute phasors
Va = df['Vr'].astype(float)
Vb = df['Vy'].astype(float) * a2
Vc = df['Vb'].astype(float) * a

# Symmetrical components
V0 = (Va + Vb + Vc) / 3
V1 = (Va + a * Vb + a2 * Vc) / 3
V2 = (Va + a2 * Vb + a * Vc) / 3

# Store magnitudes
df['v0'] = np.abs(V0)
df['v1'] = np.abs(V1)
df['v2'] = np.abs(V2)
df['vuf'] = np.where(df['v1'] > 0, (df['v2'] / df['v1']) * 100, 0)

# === PREPARE DATA FOR INSERT ===
insert_df = df[['record_date', 'Meter_id', 'Vr', 'Vy', 'Vb', 'v1', 'v2', 'v0', 'vuf', 'block_name']].copy()
insert_df.columns = ['record_date', 'meter_id', 'voltage_pha', 'voltage_phb', 'voltage_phc', 'v1', 'v2', 'v0', 'vuf', 'block_name']

# === POSTGRESQL BULK INSERT ===
print("🔄 Preparing to insert data into PostgreSQL...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    rows = [tuple(x) for x in insert_df.to_numpy()]
    cur.executemany("""
        INSERT INTO block_wise_pq_template (
            record_date, meter_id,
            voltage_pha, voltage_phb, voltage_phc,
            v1, v2, v0, vuf,
            block_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, rows)

    conn.commit()
    print(f"✅ Inserted {len(rows)} rows into block_wise_pq_template.")
except Exception as e:
    print("❌ Database error:", e)
finally:
    if 'cur' in locals(): cur.close()
    if 'conn' in locals(): conn.close()
