import pandas as pd
from datetime import datetime
import psycopg2
from collections import defaultdict

# Load Excel data
df = pd.read_excel(
    'events data.xlsx',
    sheet_name='outage-1'
)
df['RtcDateTime'] = pd.to_datetime(df['RtcDateTime'], format='%d-%m-%Y %H:%M:%S')
df = df.sort_values(by=['MeterNo', 'RtcDateTime'])

# Track counters for each (date, meter_id)
running_counters = defaultdict(lambda: {'cut': 0, 'outage': 0})

# Prepare rows to insert
rows_to_insert = []

for meter_id, group in df.groupby('MeterNo'):
    events = group.reset_index(drop=True)
    i = 0
    while i < len(events) - 1:
        row1 = events.loc[i]     # occurrence (power OFF)
        row2 = events.loc[i + 1] # restoration (power ON)

        if row1['EventId'] == 101 and row2['EventId'] == 102:
            t1 = row1['RtcDateTime']
            t2 = row2['RtcDateTime']
            diff_minutes = (t2 - t1).total_seconds() / 60
            date = t1.date()
            key = (date, meter_id)

            # Determine cut vs. outage
            if 1 < diff_minutes <= 3:
                running_counters[key]['cut'] += 1
                rows_to_insert.append((
                    date,
                    meter_id,
                    running_counters[key]['cut'],
                    round(diff_minutes, 2),
                    0,
                    0,
                    t1.time(),
                    t2.time(),
                ))
            elif diff_minutes > 3:
                running_counters[key]['outage'] += 1
                rows_to_insert.append((
                    date,
                    meter_id,
                    0,
                    0,
                    running_counters[key]['outage'],
                    round(diff_minutes, 2),
                    t1.time(),
                    t2.time(),
                ))

            i += 2
        else:
            i += 1

# === Insert into PostgreSQL in Batches ===
conn = psycopg2.connect(
    dbname="goalpara",
    user="postgres",
    password="Kinshuk2214",
    host="10.21.3.13",
    port="5432"
)
cur = conn.cursor()

# Batch insert
insert_query = """
    INSERT INTO daily_qos_cut_outage (
        date, meter_id,
        cut_count, cut_duration,
        outage_count, outage_duration,
        occurrence_time, restoration_time
    ) VALUES %s
"""

# Use psycopg2.extras.execute_values for batch insertion
from psycopg2.extras import execute_values
execute_values(cur, insert_query, rows_to_insert)

conn.commit()
cur.close()
conn.close()

print(f"Insertion complete with {len(rows_to_insert)} rows.")
