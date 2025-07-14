import pandas as pd
from sqlalchemy import create_engine, text, inspect

# === CONFIGURATION ===
excel_path = 'DT MI data with lat long 1.xlsx'
db_url = 'postgresql+psycopg2://postgres:Kinshuk2214@localhost/goalpara'
table_name = 'meter_mapping'

# === STEP 1: Read Excel ===
df_excel = pd.read_excel(excel_path, dtype=str)

# Drop rows with missing dtcode or dtname
df_excel = df_excel.dropna(subset=['dtcode', 'dtname'])

# Clean up: strip spaces and remove leading zeros from dtcode
df_excel['dtcode'] = df_excel['dtcode'].str.strip().str.lstrip('0')
df_excel['dtname'] = df_excel['dtname'].str.strip()

# Rename columns to match database
df_excel = df_excel.rename(columns={'dtcode': 'dt_code', 'dtname': 'dt_name'})

# === STEP 2: Connect to DB ===
engine = create_engine(db_url)
with engine.connect() as conn:

    # === STEP 3: Add column if not exists ===
    inspector = inspect(engine)
    columns = [col['name'] for col in inspector.get_columns(table_name)]

    if 'dt_name' not in columns:
        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN dt_name TEXT"))

    # === STEP 4: Update table using data from Excel ===
    for _, row in df_excel.iterrows():
        conn.execute(
            text(f"""
                UPDATE {table_name}
                SET dt_name = :dt_name
                WHERE dt_code = :dt_code
            """),
            {"dt_name": row['dt_name'], "dt_code": row['dt_code']}
        )

    conn.commit()
