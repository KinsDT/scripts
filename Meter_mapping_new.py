import pandas as pd
import psycopg2
from datetime import datetime

# ─── Load Excel ─────────────────────────────────────────────────────────────
df = pd.read_excel("meter_averages_with_faults.xlsx")  # Replace with your actual file path

# ─── Normalize & Rename Columns ─────────────────────────────────────────────
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

# Rename 'dtcapacity' to 'dt_capacity'
df = df.rename(columns={'dtcapacity': 'dt_capacity'})

# ─── Map CT Ratios Based on dt_capacity ─────────────────────────────────────
def map_ct_ratios(cap):
    mapping = {
        25:  (5, 5),
        63:  (100, 5),
        100: (200, 5),
        250: (400, 5),
    }
    if pd.isna(cap):
        return pd.Series({'e_ct_primary': 200, 'e_ct_secondary': 5})  # default
    return pd.Series(dict(zip(['e_ct_primary', 'e_ct_secondary'], mapping.get(int(cap), (200, 5)))))

df[['e_ct_primary', 'e_ct_secondary']] = df['dt_capacity'].apply(map_ct_ratios)

# ─── Add Required Columns ───────────────────────────────────────────────────
df['m_ct_primary'] = 5
df['m_ct_secondary'] = 5
df['vt'] = 1
df['from_date'] = pd.to_datetime("2025-01-01")
df['to_date'] = pd.NaT
df['time_interval'] = 30  # default value

# ─── Connect to PostgreSQL ──────────────────────────────────────────────────
conn = psycopg2.connect(
    dbname="goalpara",
    user="postgres",
    password="Kinshuk2214",  # Replace with your real password
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# ─── Insert Query ───────────────────────────────────────────────────────────
insert_query = """
INSERT INTO meter_mapping (
    meter_id, time_interval, area, lat, long, dt_code, dt_capacity,
    e_ct_primary, e_ct_secondary, m_ct_primary, m_ct_secondary,
    vt, from_date, to_date
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# ─── Insert Data Row-by-Row ─────────────────────────────────────────────────
for _, row in df.iterrows():
    try:
        cur.execute(insert_query, (
            row['meter_id'],
            row['time_interval'],
            row['area'],
            row['lat'],
            row['long'],
            str(row['dt_code']),
            None if pd.isna(row['dt_capacity']) else int(row['dt_capacity']),
            None if pd.isna(row['e_ct_primary']) else int(row['e_ct_primary']),
            None if pd.isna(row['e_ct_secondary']) else int(row['e_ct_secondary']),
            row['m_ct_primary'],
            row['m_ct_secondary'],
            row['vt'],
            row['from_date'],
            None if pd.isna(row['to_date']) else row['to_date']
        ))

    except Exception as e:
        print(f"❌ Failed on row: {row.to_dict()}")
        raise e

# ─── Finalize ───────────────────────────────────────────────────────────────
conn.commit()
cur.close()
conn.close()
print("✅ Data inserted successfully.")
