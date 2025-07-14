import pandas as pd
from datetime import datetime
import psycopg2
from collections import defaultdict

# Load the Excel data
df = pd.read_excel('events data.xlsx', sheet_name='under Voltage')

# Convert datetime
df['RtcDateTime'] = pd.to_datetime(df['RtcDateTime'], format='%d-%m-%Y %H:%M:%S')
df = df.sort_values(by=['MeterNo', 'RtcDateTime'])

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="goalpara",
    user="postgres",
    password="Kinshuk2214",
    host="10.21.3.13",
    port="5432"
)
cur = conn.cursor()

# Cumulative count tracking per meter/date per phase
cumulative_counts = defaultdict(lambda: {'pha': 0, 'phb': 0, 'phc': 0})

# Process each meter
for meter_id, group in df.groupby('MeterNo'):
    events = group.reset_index(drop=True)
    i = 0
    while i < len(events) - 1:
        row1 = events.loc[i]
        row2 = events.loc[i + 1]

        # Detect valid 9 → 10 pair
        if row1['EventId'] == 9 and row2['EventId'] == 10:
            t1 = row1['RtcDateTime']
            t2 = row2['RtcDateTime']
            duration = round((t2 - t1).total_seconds() / 60, 2)
            date = t1.date()
            key = (meter_id, date)

            # Initialize output values
            uv_count_pha = uv_count_phb = uv_count_phc = 0
            uv_duration_pha = uv_duration_phb = uv_duration_phc = 0

            # Check each phase
            if row1['RPhaseVoltage'] < 180:
                cumulative_counts[key]['pha'] += 1
                uv_count_pha = cumulative_counts[key]['pha']
                uv_duration_pha = duration

            if row1['YPhaseVoltage'] < 180:
                cumulative_counts[key]['phb'] += 1
                uv_count_phb = cumulative_counts[key]['phb']
                uv_duration_phb = duration

            if row1['BPhaseVoltage'] < 180:
                cumulative_counts[key]['phc'] += 1
                uv_count_phc = cumulative_counts[key]['phc']
                uv_duration_phc = duration

            # Insert only if any undervoltage was detected
            if uv_count_pha or uv_count_phb or uv_count_phc:
                cur.execute("""
                    INSERT INTO daily_qos_undervoltage (
                        meter_id, date,
                        uv_count_pha, uv_duration_pha,
                        uv_count_phb, uv_duration_phb,
                        uv_count_phc, uv_duration_phc
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    meter_id, date,
                    uv_count_pha, uv_duration_pha,
                    uv_count_phb, uv_duration_phb,
                    uv_count_phc, uv_duration_phc
                ))
            i += 2
        else:
            i += 1

# Finalize DB
conn.commit()
cur.close()
conn.close()

print("Undervoltage events inserted successfully with cumulative count and event durations.")
