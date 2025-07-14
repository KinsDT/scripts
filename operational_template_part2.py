import pandas as pd
import psycopg2

# PostgreSQL connection info
PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "goalpara",
    "user": "postgres",
    "password": "Kinshuk2214"
}

# Load sorted Excel file
excel_path = r"C:\Users\KinshukGoswami\OneDrive - Sinhal Udyog pvt ltd\Downloads\billing data.xlsx"
df_excel = pd.read_excel(excel_path)

# Connect to PostgreSQL and fetch unique meter IDs from operational_template
conn = psycopg2.connect(**PG_CONN_PARAMS)
cur = conn.cursor()
cur.execute("SELECT DISTINCT meter_id FROM public.operational_template;")
meter_ids = [row[0] for row in cur.fetchall()]

# Prepare list to collect output rows for export
output_rows = []

for meter_id in meter_ids:
    # Filter rows for this meter_id in Excel
    df_meter = df_excel[df_excel["MeterNo"] == meter_id]

    if df_meter.empty:
        print(f"No data found for MeterNo: {meter_id}")
        continue

    # Find row with max MaximumDemandKva
    idx_max = df_meter["MaximumDemandKva"].idxmax()
    row_max = df_meter.loc[idx_max]

    # Calculate power factors safely (avoid divide by zero)
    try:
        avg_import_pf = (
            row_max["CumulativeEnergyKwhImport"] / row_max["CumulativeEnergyKvahImport"]
            if row_max["CumulativeEnergyKvahImport"] != 0 else None
        )
    except KeyError:
        avg_import_pf = None

    try:
        export_pf = (
            row_max["CumulativeEnergyKwhExport"] / row_max["CumulativeEnergyKvahExport"]
            if row_max["CumulativeEnergyKvahExport"] != 0 else None
        )
    except KeyError:
        export_pf = None

    if avg_import_pf is not None:
        avg_import_pf = float(avg_import_pf)
    if export_pf is not None:
        export_pf = float(export_pf)

    # Update the operational_template table with calculated values
    update_sql = """
        UPDATE public.operational_template
        SET avg_import_pf = %s,
            avg_export_pf = %s
        WHERE meter_id = %s;
    """
    cur.execute(update_sql, (avg_import_pf, export_pf, meter_id))
    print(f"Updated meter_id {meter_id}: avg_import_pf={avg_import_pf}, avg_export_pf={export_pf}")

    # Append to output list for export
    output_rows.append({
        "meter_id": meter_id,
        "MaximumDemandKva": row_max["MaximumDemandKva"],
        "avg_import_pf": avg_import_pf,
        "avg_export_pf": export_pf,
        # Add any other relevant columns from row_max here if needed
    })

# Commit all changes
conn.commit()

cur.close()
conn.close()



