import psycopg2
from sqlalchemy import create_engine, inspect
import pandas as pd
from datetime import datetime

# Config
METER_INFO_DB = "meter_info"
GOALPARA_DB = "goalpara"
USER = "postgres"
PASSWORD = "Kinshuk2214"
HOST = "localhost"
PORT = "5432"

def calculate_block(time_str):
    time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return time.hour * 2 + (1 if time.minute >= 30 else 0) + 1

# Connections
meter_engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{METER_INFO_DB}")
goalpara_engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{GOALPARA_DB}")

meter_conn = meter_engine.connect()
goalpara_conn = goalpara_engine.connect()

# Fetch all meter_info tables
inspector = inspect(meter_engine)
tables = inspector.get_table_names(schema="public")

for table in tables:
    try:
        query = f'SELECT "Meter_id", "Time", "CumEimportKvah", "CumEexportKvah" FROM "{table}"'
        df = pd.read_sql(query, meter_conn)

        df["Time"] = pd.to_datetime(df["Time"])
        df["date"] = df["Time"].dt.strftime('%Y-%m-%d')
        df["block"] = df["Time"].apply(lambda x: calculate_block(x.strftime('%Y-%m-%d %H:%M:%S')))

        df.rename(columns={
            "Meter_id": "meter_id",
            "CumEimportKvah": "cum_eimport_kvah",
            "CumEexportKvah": "cum_eexport_kvah"
        }, inplace=True)

        # Calculate load
        df["load"] = (df["cum_eimport_kvah"] - df["cum_eexport_kvah"]) * 2

        # Fill other columns with defaults
        df["dt_capacity"] = 0
        df["percentage_load"] = 0
        df["flag"] = 0

        # Final column order
        df = df[[
            "meter_id", "date", "block",
            "cum_eimport_kvah", "cum_eexport_kvah", "load",
            "dt_capacity", "percentage_load", "flag"
        ]]

        # Append to goalpara.public.loading
        df.to_sql("loading", goalpara_engine, if_exists='append', index=False)

        print(f"✔ Inserted from table: {table}")
    except Exception as e:
        print(f"❌ Error in {table}: {e}")

# Close
meter_conn.close()
goalpara_conn.close()

'''
-- Step 1: Update using corrected block-to-time conversion
UPDATE public.loading AS l
SET 
    ct_vt_ratio = 
        (CAST(mm.e_ct_primary AS numeric) / mm.e_ct_secondary) *
        (CAST(mm.m_ct_primary AS numeric) / mm.m_ct_secondary) *
        mm.vt,
    final_load = l.load * 
        ( (CAST(mm.e_ct_primary AS numeric) / mm.e_ct_secondary) *
          (CAST(mm.m_ct_primary AS numeric) / mm.m_ct_secondary) *
          mm.vt ),
    dt_capacity = mm.dt_capacity,
    percentage_load = CASE 
        WHEN mm.dt_capacity > 0 THEN 
            (l.load * 
            ( (CAST(mm.e_ct_primary AS numeric) / mm.e_ct_secondary) *
              (CAST(mm.m_ct_primary AS numeric) / mm.m_ct_secondary) *
              mm.vt ) / mm.dt_capacity) * 100
        ELSE 0
    END
FROM public.meter_mapping AS mm
WHERE 
    l.meter_id = mm.meter_id
    AND (
        (l.date + ((l.block - 1) * interval '30 minutes')) > mm.from_date
    )
    AND (
        mm.to_date IS NULL OR
        (l.date + ((l.block - 1) * interval '30 minutes')) <= mm.to_date
    );

-- Step 2: Round values to 2 decimal places
UPDATE public.loading
SET
    percentage_load = ROUND(percentage_load, 2),
    ct_vt_ratio = ROUND(ct_vt_ratio, 2),
    final_load = ROUND(final_load, 2);
'''