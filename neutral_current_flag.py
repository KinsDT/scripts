from sqlalchemy import create_engine, inspect, text
import math

# --- Connect to the target database ---
engine = create_engine("postgresql+psycopg2://postgres:ItsMe%4003@localhost:5432/ct_ls")

# --- Set constant for denominator ---
sqrt3 = math.sqrt(3)
threshold_expr = f"0.5 * dt_capacity / (mf * 0.44 * {sqrt3})"

# --- Get all meter_sc* tables ---
inspector = inspect(engine)
tables = [t for t in inspector.get_table_names() if t.startswith("meter_sc")]

updated_tables = 0

with engine.begin() as conn:
    for table in tables:
        print(f"🔍 Checking table: {table}")
        cols = [col['name'] for col in inspector.get_columns(table)]

        # Ensure required columns exist
        required = {'neutral_current_upd', 'dt_capacity', 'mf', 'neutral_current_flag'}
        if not required.issubset(set(cols)):
            print(f" ❌ Skipping {table}: missing required columns.")
            continue

        # Update rows based on threshold formula
        conn.execute(
            text(f"""
                UPDATE {table}
                SET neutral_current_flag = CASE
                    WHEN neutral_current_upd > {threshold_expr} THEN 1
                    ELSE 0
                END
                WHERE neutral_current_upd IS NOT NULL
                  AND dt_capacity IS NOT NULL
                  AND mf IS NOT NULL;
            """)
        )

        print(f" ✅ Updated flags in table: {table}")
        updated_tables += 1

print(f"\n🎯 Total tables updated: {updated_tables}")
