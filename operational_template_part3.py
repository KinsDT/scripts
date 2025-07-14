import psycopg2

# Connection to goalpara database
conn_goalpara = psycopg2.connect(
    host="localhost", port=5432, dbname="goalpara", user="postgres", password="Kinshuk2214"
)
cur_goalpara = conn_goalpara.cursor()

# Connection to meter_info database
conn_meterinfo = psycopg2.connect(
    host="localhost", port=5432, dbname="meter_info", user="postgres", password="Kinshuk2214"
)
cur_meterinfo = conn_meterinfo.cursor()

# Fetch all meter_ids with active_md_import and active_md_export
cur_goalpara.execute("SELECT meter_id, active_md_import, active_md_export FROM public.operational_template;")
meters = cur_goalpara.fetchall()

for meter_id, active_md_import, active_md_export in meters:
    table_name = f"meter_{meter_id.lower()}"

    try:
        # Build dynamic SQL to fetch sum and count
# Equivalent to: df = df[(df['Time'].dt.year == 2025) & (df['Time'].dt.month == 5)]
        query = f"""
            SELECT 
                SUM("CumEimportKwh") AS sum_i,
                SUM("CumEexportKwh") AS sum_e,
                COUNT(*) AS n
            FROM public."{table_name}"
            WHERE EXTRACT(YEAR FROM "Time") = 2025
            AND EXTRACT(MONTH FROM "Time") = 5;
        """

        cur_meterinfo.execute(query)
        result = cur_meterinfo.fetchone()

        sum_i, sum_e, n = result

        # Initialize both values as None
        import_lf = None
        export_lf = None

        if sum_i and active_md_import and n > 0:
            import_lf = (sum_i / (n * 0.5)) / active_md_import

        if sum_e and active_md_export and n > 0:
            export_lf = (sum_e / (n * 0.5)) / active_md_export

        # Update both values
        update_sql = """
            UPDATE public.operational_template
            SET import_lf = %s,
                export_lf = %s
            WHERE meter_id = %s;
        """
        cur_goalpara.execute(update_sql, (import_lf, export_lf, meter_id))
        print(f"Updated {meter_id} → import_lf = {import_lf}, export_lf = {export_lf}")

    except Exception as e:
        print(f"❌ Error with meter_id {meter_id}: {e}")
        continue

# Commit and close
conn_goalpara.commit()
cur_goalpara.close()
cur_meterinfo.close()
conn_goalpara.close()
conn_meterinfo.close()

