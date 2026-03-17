import pymysql
import pandas as pd
import duckdb as db
import os
import traceback
import requests
import logging
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()
required_vars = ['MOTHERDUCK_TOKEN', 'ENDPOINT', 'PASSWORD', 'DISCORD_WEBHOOK']
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    raise RuntimeError(f"Missing required env vars: {missing}")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

if DISCORD_WEBHOOK:
    payload = {"content": "✅ Test message from the data pipeline!"}
    try:
        response = requests.post(DISCORD_WEBHOOK, json=payload)
        if response.status_code == 204:
            print("✅ Discord webhook test succeeded!")
        else:
            print(f"❌ Discord webhook test failed: {response.status_code} {response.text}")
    except Exception as e:
        print(f"❌ Discord webhook test error: {e}")
else:
    print("❌ DISCORD_WEBHOOK not set")

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
ENDPOINT = os.getenv("ENDPOINT")
PORT = 3306
USER = "analytics_ro"
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

# -----------------------------
# Logging Setup
# -----------------------------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"etl_{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# -----------------------------
# Discord Helper
# -----------------------------
def send_discord_alert(message):
    payload = {"content": message}
    try:
        r = requests.post(DISCORD_WEBHOOK, json=payload)
        if r.status_code != 204:
            logging.warning(f"Failed to send Discord alert: {r.status_code} {r.text}")
    except Exception as e:
        logging.error(f"Exception sending Discord alert: {e}")

# -----------------------------
# ETL Pipeline
# -----------------------------
conn = None
duck_con = None
summary_messages = []

try:
    logging.info("🔌 Connecting to MotherDuck...")
    duck_con = db.connect("md:btc_analytics")
    logging.info("✅ Connected to MotherDuck")

    conn = pymysql.connect(
        host=ENDPOINT,
        user=USER,
        password=PASSWORD,
        database=DBNAME,
        port=PORT
    )
    logging.info("✅ Connected to MySQL")

    for table in tables_to_pull:
        logging.info(f"⬇️ Pulling '{table}' from MySQL...")
        try:
            df = pd.read_sql(f"SELECT * FROM `{table}`", conn)
        except Exception as e:
            msg = f"❌ Failed to pull '{table}': {e}"
            logging.error(msg)
            summary_messages.append(msg)
            continue

        if df.empty:
            msg = f"⚠️ '{table}' is empty, skipping..."
            logging.warning(msg)
            summary_messages.append(msg)
            continue

        duck_con.register("tmp_df", df)

        # Check if table exists in MotherDuck
        try:
            duck_con.execute(f'SELECT 1 FROM "{table}" LIMIT 1')
            table_exists = True
        except Exception:
            table_exists = False

        # Create table if missing
        if not table_exists:
            duck_con.execute(f'CREATE TABLE "{table}" AS SELECT * FROM tmp_df')
            msg = f"🎉 Table '{table}' created with {len(df)} rows"
            logging.info(msg)
            summary_messages.append(msg)
            continue

        # Check for schema mismatch
        existing_columns = duck_con.execute(f'SELECT * FROM "{table}" LIMIT 0').df().columns.tolist()
        source_columns = df.columns.tolist()

        if set(existing_columns) != set(source_columns):
            msg = f"⚠️ Schema mismatch on '{table}', recreating table..."
            logging.warning(msg)
            summary_messages.append(msg)
            duck_con.execute(f'DROP TABLE "{table}"')
            duck_con.execute(f'CREATE TABLE "{table}" AS SELECT * FROM tmp_df')
            msg = f"🔄 Table '{table}' recreated with {len(df)} rows and updated schema"
            logging.info(msg)
            summary_messages.append(msg)
            continue

        # Incremental load
        pk_column = PRIMARY_KEYS.get(table)
        if not pk_column or pk_column not in df.columns:
            msg = f"⚠️ No valid primary key found for '{table}', skipping incremental load"
            logging.warning(msg)
            summary_messages.append(msg)
            continue

        try:
            existing_keys = duck_con.execute(f'SELECT "{pk_column}" FROM "{table}"').df()
        except Exception as e:
            msg = f"❌ Failed fetching existing keys for '{table}': {e}"
            logging.error(msg)
            summary_messages.append(msg)
            continue

        new_rows = df[~df[pk_column].isin(existing_keys[pk_column])]

        if new_rows.empty:
            msg = f"✅ No new rows to insert into '{table}'"
            logging.info(msg)
            summary_messages.append(msg)
            continue

        duck_con.register("new_rows", new_rows)
        duck_con.execute(f'INSERT INTO "{table}" SELECT * FROM new_rows')
        msg = f"⬆️ Inserted {len(new_rows)} new rows into '{table}'"
        logging.info(msg)
        summary_messages.append(msg)

    send_discord_alert(f"✅ ETL Job Completed Successfully!\n\n" + "\n".join(summary_messages))

except Exception as e:
    error_details = traceback.format_exc()
    logging.error(f"❌ Database connection or extraction failed: {e}")
    send_discord_alert(f"❌ ETL Job Failed!\n```{error_details}```")

finally:
    if conn:
        conn.close()
    if duck_con:
        duck_con.close()