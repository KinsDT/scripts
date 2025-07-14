import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text
 
# --- Update your database connection here ---
engine = create_engine("postgresql+psycopg2://postgres:Kinshuk2214@localhost:5432/meter_info")
 
# --- Function to compute neutral current vectorially ---
def calculate_neutral_current(row):
    currents = [row['Rcurrent'], row['Ycurrent'], row['Bcurrent']]
    powerfacs = [row['Rpowerfac'], row['Ypowerfac'], row['Bpowerfac']]
    angles_deg = [0, -120, 120]
 
    try:
        phasors = []
        for I, pf, angle in zip(currents, powerfacs, angles_deg):
            if pd.isna(I) or pd.isna(pf):
                return None
            theta = np.arccos(np.clip(pf, -1.0, 1.0))
            total_angle = np.deg2rad(angle) + theta
            phasor = I * np.exp(1j * total_angle)
            phasors.append(phasor)
 
        neutral_current = np.abs(sum(phasors))
        return float(neutral_current)
    except:
        return None
 
# --- Get list of all meter_sc* tables ---
inspector = inspect(engine)
tables = [t for t in inspector.get_table_names() if t.startswith("meter_sc")]
 
# --- Process each table ---
for table in tables:
    print(f"Processing table: {table}")
    with engine.begin() as conn:
 
        try:
            df = pd.read_sql(f"SELECT *, ctid FROM {table}", conn)
 
            required_cols = {'Rcurrent', 'Ycurrent', 'Bcurrent', 'Rpowerfac', 'Ypowerfac', 'Bpowerfac'}
            if not required_cols.issubset(df.columns):
                print(f"Skipping {table}: missing required columns.")
                continue
 
            df['neutral_current_upd'] = df.apply(calculate_neutral_current, axis=1)
 
            # Update the column in the DB
            for _, row in df.iterrows():
                conn.execute(
                    text(f"""
                        UPDATE {table}
                        SET neutral_current_upd = :val
                        WHERE ctid = :ctid
                    """),
                    {"val": row['neutral_current_upd'], "ctid": row['ctid']}
                )
        except Exception as e:
            print(f"Skipping table {table} due to error: {e}")
 