import pandas as pd
from sqlalchemy import create_engine, inspect, text
 
# --- Source DB with reference data ---
source_engine = create_engine("postgresql+psycopg2://postgres:Kinshuk2214@localhost:5432/goalpara")
 
# --- Target DB with meter_<meter_id> tables ---
target_engine = create_engine("postgresql+psycopg2://postgres:Kinshuk2214@localhost:5432/meter_info")
 
# --- Load meter info ---
with source_engine.connect() as conn:
    meter_mapping = pd.read_sql("SELECT meter_id, dt_capacity, mf FROM meter_mapping", conn)
 
# --- Inspect target tables ---
inspector = inspect(target_engine)
existing_tables = inspector.get_table_names()
 
tables_updated = 0
 
with target_engine.begin() as conn:
    for _, row in meter_mapping.iterrows():
        meter_id = row['meter_id']
        dt_capacity = row['dt_capacity']
        mf = row['mf']
 
        # Convert meter_id to lowercase to match target DB table names
        table_name = f"meter_{meter_id.lower()}"
 
        if table_name not in existing_tables:
            print(f"❌ Table {table_name} not found — skipping.")
            continue
 
        print(f"🔄 Processing table: {table_name}")
        columns = [col['name'] for col in inspector.get_columns(table_name)]
 
        if 'dt_capacity' not in columns:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN dt_capacity FLOAT;"))
        if 'mf' not in columns:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN mf FLOAT;"))
 
        # Update rows for this meter_id
        result = conn.execute(
    text(f"""
        UPDATE {table_name}
        SET dt_capacity = :dtc, mf = :mf
        WHERE "Meter_id" = :mid
    """),
    {"dtc": dt_capacity, "mf": mf, "mid": meter_id}
)
 
        if result.rowcount > 0:
            print(f" ✅ Updated {result.rowcount} rows in {table_name}")
            tables_updated += 1
        else:
            print(f" ⚠️ No matching meter_id in {table_name}")
 
# --- Final summary ---
print(f"\n✅ Total meter tables updated: {tables_updated} out of {len(meter_mapping)}")
 