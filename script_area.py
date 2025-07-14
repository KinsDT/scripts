import pandas as pd
import psycopg2

# --- Configuration ---
excel_file = 'LOCATION.xlsx'  # Path to your Excel file
sheet_name = 0  # Sheet index or name
db_params = {
    'host': '10.21.3.13',
    'port': '5432',
    'dbname': 'goalpara',
    'user': 'postgres',
    'password': 'Kinshuk2214'
}
table_name = 'public.reliability_indices'

# --- Area mapping ---
area_map = {
    'Lakhipur_bec': 11,
    'Bijni': 12,
    'Gossaigaon': 13
}

# --- Load Excel Data ---
df = pd.read_excel(excel_file, sheet_name=sheet_name)
df.columns = [col.strip().upper() for col in df.columns]  # Clean and uppercase column names for matching

# Ensure the column names match your Excel structure
# Adjust if your actual Excel headers differ in case or spelling
for col in ['AREA', 'METER_ID']:
    if col not in df.columns:
        raise ValueError(f'Missing expected column: {col}')

# Map area names to codes
df['AREA_CODE'] = df['AREA'].map(area_map)

# --- Connect to PostgreSQL ---
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# --- Update Rows ---
for _, row in df.iterrows():
    area_code = row['AREA_CODE']
    meter_id = row['METER_ID']
    if pd.notnull(area_code) and pd.notnull(meter_id):
        update_sql = f'''
            UPDATE {table_name}
            SET area = %s
            WHERE meter_id = %s
        '''
        cur.execute(update_sql, (int(area_code), str(meter_id)))

conn.commit()
cur.close()
conn.close()

print("Area codes updated for all matching meter_ids.")
