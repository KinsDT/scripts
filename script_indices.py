import pandas as pd
import numpy as np
import psycopg2

# ————————— CONFIG: update these to your environment —————————
SRC_DB = {
    "dbname":   "meter_info",
    "user":     "postgres",
    "password": "Kinshuk2214",
    "host":     "10.21.3.13",
    "port":     "5432",
}
DST_DB = {
    "dbname":   "goalpara",
    "user":     "postgres",
    "password": "Kinshuk2214",
    "host":     "10.21.3.13",
    "port":     "5432",
}
SRC_TABLE = "reliability_database"
DST_TABLE = "reliability_indices"
# ——————————————————————————————————————————————————————————————————————

# 1) LOAD RAW DATA FROM meter_info
src_conn = psycopg2.connect(**SRC_DB)
df = pd.read_sql(f"SELECT * FROM {SRC_TABLE}", src_conn)
src_conn.close()

# Ensure numeric & fill NaNs
for col in ['ca','cb','cc','la','lb','lc',
            'na','nb','nc','ma','mb','mc',
            'da','db','dc','ta','tb','tc','area']:
    if col not in df.columns:
        raise KeyError(f"Expected column '{col}' not found in source table")
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

# 2) COMPUTE BASE TOTALS
df['ct'] = df['ca'] + df['cb'] + df['cc']        # total customers
df['lt'] = df['la'] + df['lb'] + df['lc']        # total load
df['tt'] = df['ta'] + df['tb'] + df['tc']        # total minutes interrupted

# Avoid division by zero
eps = 1e-9
df['ct'] = df['ct'].replace(0, eps)
df['lt'] = df['lt'].replace(0, eps)

# 3) COMPUTE INDICES

# Consumer-basis
num_cons = df['na']*df['ca'] + df['nb']*df['cb'] + df['nc']*df['cc']
dur_cons = df['da']*df['ca'] + df['db']*df['cb'] + df['dc']*df['cc']

df['saifi_cons']  = (num_cons / df['ct']).astype(float)
df['saidi_cons']  = (dur_cons / df['ct']).astype(float)
df['caifi_cons']  = (num_cons / (df['ca']+df['cb']+df['cc']+eps)).astype(float)
df['caidi_cons']  = (dur_cons / (df['ca']+df['cb']+df['cc']+eps)).astype(float)
df['ciii_cons']   = (df['saidi_cons'] / (df['saifi_cons']+eps)).astype(float)
df['asai_cons']   = ((1 - df['saidi_cons'] / (df['ct'] * 24 * 60)) * 100).astype(float)
df['maifi_cons']  = ((df['ma']*df['ca'] + df['mb']*df['cb'] + df['mc']*df['cc']) / (df['ct'] * df['tt'] + eps)).astype(float)
df['maidi_cons']  = ((df['ta']*df['ca'] + df['tb']*df['cb'] + df['tc']*df['cc']) / df['ct']).astype(float)

# Load-basis
num_load = df['na']*df['la'] + df['nb']*df['lb'] + df['nc']*df['lc']
dur_load = df['da']*df['la'] + df['db']*df['lb'] + df['dc']*df['lc']

df['saifi_load']  = (num_load / df['lt']).astype(float)
df['saidi_load']  = (dur_load / df['lt']).astype(float)
df['caifi_load']  = (num_load / (df['la']+df['lb']+df['lc']+eps)).astype(float)
df['caidi_load']  = (dur_load / (df['la']+df['lb']+df['lc']+eps)).astype(float)
df['ciii_load']   = (df['saidi_load'] / (df['saifi_load']+eps)).astype(float)
df['asai_load']   = ((1 - df['saidi_load'] / (df['lt'] * 24 * 60)) * 100).astype(float)
df['maifi_load']  = ((df['ma']*df['la'] + df['mb']*df['lb'] + df['mc']*df['lc']) / (df['lt'] * df['tt'] + eps)).astype(float)
df['maidi_load']  = ((df['ta']*df['la'] + df['tb']*df['lb'] + df['tc']*df['lc']) / df['lt']).astype(float)

# Energy-basis
df['ens']  = (((df['la']*df['da']) + (df['lb']*df['db']) + (df['lc']*df['dc'])) / 60).astype(float)
df['aens'] = (df['ens'] / df['ct']).astype(float)
df['ors']  = ((1 - df['ens'] / (df['lt'] * 24 * 60)) * 100).astype(float)

# 4) PREPARE OUTPUT DataFrame
out_cols = [
    'meter_id','area',
    'saifi_cons','saidi_cons','caifi_cons','caidi_cons','ciii_cons','asai_cons','maifi_cons','maidi_cons',
    'saifi_load','saidi_load','caifi_load','caidi_load','ciii_load','asai_load','maifi_load','maidi_load',
    'ens','aens','ors'
]
out_df = df[out_cols].copy()

# Build list of pure-Python tuples
records = [tuple(row.tolist()) for _, row in out_df.iterrows()]

# 5) BULK-INSERT INTO goalpara.reliability_indices
dst_conn = psycopg2.connect(**DST_DB)
cur      = dst_conn.cursor()

placeholders = ",".join(["%s"] * len(out_cols))
columns      = ",".join(out_cols)

cur.executemany(
    f"INSERT INTO {DST_TABLE} ({columns}) VALUES ({placeholders})",
    records
)

dst_conn.commit()
cur.close()
dst_conn.close()

print(f"✅ Computed and inserted {len(records)} rows into {DST_TABLE}.")
