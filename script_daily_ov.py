import pandas as pd
from datetime import datetime
import psycopg2
from collections import defaultdict

# Load the Excel data
df = pd.read_excel('events data.xlsx', sheet_name='over voltage')

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

# Track cumulative counts per meter/date/phase
cumulative_counts = defaultdict(lambda: {'pha': 0, 'phb': 0, 'phc': 0})
threshold = 275  # Overvoltage threshold

# Process each meter group
for meter_id, group in df.groupby('MeterNo'):
    events = group.reset_index(drop=True)
    i = 0
    while i < len(events) - 1:
        row1 = events.loc[i]
        row2 = events.loc[i + 1]

        if row1['EventId'] == 7 and row2['EventId'] == 8:
            t1 = row1['RtcDateTime']
            t2 = row2['RtcDateTime']
            duration = round((t2 - t1).total_seconds() / 60, 2)
            date = t1.date()
            key = (meter_id, date)

            # Initialize insert values
            ov_count_pha = ov_count_phb = ov_count_phc = 0
            ov_duration_pha = ov_duration_phb = ov_duration_phc = 0

            # R Phase (Pha)
            if row1['RPhaseVoltage'] >= threshold:
                cumulative_counts[key]['pha'] += 1
                ov_count_pha = cumulative_counts[key]['pha']
                ov_duration_pha = duration

            # Y Phase (Phb)
            if row1['YPhaseVoltage'] >= threshold:
                cumulative_counts[key]['phb'] += 1
                ov_count_phb = cumulative_counts[key]['phb']
                ov_duration_phb = duration

            # B Phase (Phc)
            if row1['BPhaseVoltage'] >= threshold:
                cumulative_counts[key]['phc'] += 1
                ov_count_phc = cumulative_counts[key]['phc']
                ov_duration_phc = duration

            # Insert if any overvoltage detected
            if ov_count_pha or ov_count_phb or ov_count_phc:
                cur.execute("""
                    INSERT INTO daily_qos_overvoltage (
                        meter_id, date,
                        ov_count_pha, ov_duration_pha,
                        ov_count_phb, ov_duration_phb,
                        ov_count_phc, ov_duration_phc
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    meter_id, date,
                    ov_count_pha, ov_duration_pha,
                    ov_count_phb, ov_duration_phb,
                    ov_count_phc, ov_duration_phc
                ))
            i += 2
        else:
            i += 1

# Finalize
conn.commit()
cur.close()
conn.close()

print("Overvoltage events inserted with cumulative counts and event durations.")
