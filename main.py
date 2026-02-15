import pymysql
import pandas as pd
import duckdb as db
import os
from dotenv import load_dotenv

load_dotenv()
required_vars = ['MOTHERDUCK_TOKEN', 'ENDPOINT', 'PASSWORD']
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    raise RuntimeError(f"Missing required env vars: {missing}")

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
if not MOTHERDUCK_TOKEN:
    raise RuntimeError("❌ MOTHERDUCK_TOKEN is not set")

# --- MySQL credentials ---
ENDPOINT = os.getenv("ENDPOINT")
PORT = 3306
USER = "analytics_ro"#os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DBNAME = "btcdb"

PRIMARY_KEYS = {
    "application": "id",
    "bitcoin_events": "id",
    "opportunity": "id",
    "opportunitycategory": "opportunity_id",
    "organization": "id",
    "organizationmember": "org_id",
    "organizationprompts": "id",
    "orginvite": "id",
    "outputtype": "id",
    "profile": "user_id",
    "profilelink": "id",
    "tools": "id",
    "user": "id"
}

tables_to_pull = list(PRIMARY_KEYS.keys())

conn = None
duck_con = None

try:
    
    print("🔌 Connecting to MotherDuck...")
    duck_con = db.connect("md:btc_analytics")
    print("✅ Connected to MotherDuck")
    conn = pymysql.connect(
        host=ENDPOINT,
        user=USER,
        password=PASSWORD,
        database=DBNAME,
        port=PORT
    )
    print("✅ Connected to MySQL")

    # -----------------------------
    # Loop Through Tables
    # -----------------------------
    for table in tables_to_pull:
        print(f"\n⬇️  Pulling '{table}' from MySQL...")

        try:
            df = pd.read_sql(f"SELECT * FROM `{table}`", conn)
        except Exception as e:
            print(f"❌ Failed to pull '{table}': {e}")
            continue

        if df.empty:
            print(f"⚠️ '{table}' is empty, skipping...")
            continue

        duck_con.register("tmp_df", df)

        # -----------------------------
        # Check if table exists in MotherDuck
        # -----------------------------
        try:
            duck_con.execute(f'SELECT 1 FROM "{table}" LIMIT 1')
            table_exists = True
        except Exception:
            table_exists = False

        # -----------------------------
        # Create Table if Missing
        # -----------------------------
        if not table_exists:
            duck_con.execute(f'CREATE TABLE "{table}" AS SELECT * FROM tmp_df')
            print(f"🎉 Table '{table}' created with {len(df)} rows")
            continue

        # -----------------------------
        # Incremental Load
        # -----------------------------
        pk_column = PRIMARY_KEYS.get(table)

        if not pk_column or pk_column not in df.columns:
            print(f"⚠️ No valid primary key found for '{table}', skipping incremental load")
            continue

        try:
            existing_keys = duck_con.execute(
                f'SELECT "{pk_column}" FROM "{table}"'
            ).df()
        except Exception as e:
            print(f"❌ Failed fetching existing keys for '{table}': {e}")
            continue

        new_rows = df[~df[pk_column].isin(existing_keys[pk_column])]

        if new_rows.empty:
            print(f"✅ No new rows to insert into '{table}'")
            continue

        duck_con.register("new_rows", new_rows)
        duck_con.execute(f'INSERT INTO "{table}" SELECT * FROM new_rows')

        print(f"⬆️ Inserted {len(new_rows)} new rows into '{table}'")

except Exception as e:
    print(f"❌ Database connection or extraction failed: {e}")

finally:
    if conn:
        conn.close()
    if duck_con:
        duck_con.close()
