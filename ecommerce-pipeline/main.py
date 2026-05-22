import pandas as pd
import duckdb as db
import os
import traceback
import requests
import logging
import json
import subprocess
import time
import hashlib
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager
from dataclasses import dataclass

load_dotenv()

# =============================
# Configuration
# =============================
class ETLConfig:
    BATCH_SIZE = 5000
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5
    CHECKPOINT_FILE = "etl_checkpoint.json"

@dataclass
class ETLMetrics:
    table_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    rows_inserted: int = 0
    status: str = "running"
    error_message: Optional[str] = None
    
    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0

# =============================
# Logging Setup
# =============================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"etl_{datetime.now().strftime('%Y-%m-%d')}.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================
# COMPLETE TABLES & PRIMARY KEYS
# =============================
PRIMARY_KEYS: Dict[str, str] = {
    "account_holder": "id",
    "auth_identity": "id",
    "customer": "id",
    "customer_account_holder": "id",
    "customer_address": "id",
    "customer_group": "id",
    "customer_group_customer": "id",
    "user": "id",
    "user_preference": "id",
    "user_rbac_role": "id",
    "api_key": "id",
    "invite": "id",
    "publishable_api_key_sales_channel": "id",
    "marketplace_offer": "id",
    "marketplace_order": "id",
    "card": "id",
    "vendor": "id",
    "order": "id",
    "order_address": "id",
    "order_cart": "id",
    "order_change": "id",
    "order_change_action": "id",
    "order_claim": "id",
    "order_claim_item": "id",
    "order_claim_item_image": "id",
    "order_credit_line": "id",
    "order_exchange": "id",
    "order_exchange_item": "id",
    "order_fulfillment": "id",
    "order_item": "id",
    "order_line_item": "id",
    "order_line_item_adjustment": "id",
    "order_line_item_tax_line": "id",
    "order_payment_collection": "id",
    "order_promotion": "id",
    "order_shipping": "id",
    "order_shipping_method": "id",
    "order_shipping_method_adjustment": "id",
    "order_shipping_method_tax_line": "id",
    "order_summary": "id",
    "order_transaction": "id",
    "payment": "id",
    "payment_collection": "id",
    "payment_collection_payment_providers": "id",
    "payment_provider": "id",
    "payment_session": "id",
    "capture": "id",
    "refund": "id",
    "refund_reason": "id",
    "credit_line": "id",
    "cart": "id",
    "cart_address": "id",
    "cart_line_item": "id",
    "cart_line_item_adjustment": "id",
    "cart_line_item_tax_line": "id",
    "cart_payment_collection": "id",
    "cart_promotion": "id",
    "cart_shipping_method": "id",
    "cart_shipping_method_adjustment": "id",
    "cart_shipping_method_tax_line": "id",
    "product": "id",
    "product_category": "id",
    "product_category_product": "id",
    "product_collection": "id",
    "product_option": "id",
    "product_option_value": "id",
    "product_sales_channel": "id",
    "product_shipping_profile": "id",
    "product_tag": "id",
    "product_tags": "id",
    "product_type": "id",
    "product_variant": "id",
    "product_variant_inventory_item": "id",
    "product_variant_option": "id",
    "product_variant_price_set": "id",
    "product_variant_product_image": "id",
    "image": "id",
    "price": "id",
    "price_list": "id",
    "price_list_rule": "id",
    "price_preference": "id",
    "price_rule": "id",
    "price_set": "id",
    "inventory_item": "id",
    "inventory_level": "id",
    "reservation_item": "id",
    "stock_location": "id",
    "stock_location_address": "id",
    "location_fulfillment_provider": "id",
    "location_fulfillment_set": "id",
    "fulfillment": "id",
    "fulfillment_address": "id",
    "fulfillment_item": "id",
    "fulfillment_label": "id",
    "fulfillment_provider": "id",
    "fulfillment_set": "id",
    "return": "id",
    "return_fulfillment": "id",
    "return_item": "id",
    "return_reason": "id",
    "promotion": "id",
    "promotion_application_method": "id",
    "promotion_application_method_buy_rules": "id",
    "promotion_application_method_target_rules": "id",
    "promotion_campaign": "id",
    "promotion_campaign_budget": "id",
    "promotion_campaign_budget_usage": "id",
    "promotion_promotion_rule": "id",
    "promotion_rule": "id",
    "promotion_rule_value": "id",
    "notification": "id",
    "notification_provider": "id",
    "marketplace_email_verification": "id",
    "region": "id",
    "region_country": "id",
    "region_payment_provider": "id",
    "geo_zone": "id",
    "service_zone": "id",
    "sales_channel": "id",
    "sales_channel_stock_location": "id",
    "store": "id",
    "store_currency": "id",
    "store_locale": "id",
    "currency": "code",
    "tax_provider": "id",
    "tax_rate": "id",
    "tax_rate_rule": "id",
    "tax_region": "id",
    "shipping_option": "id",
    "shipping_option_price_set": "id",
    "shipping_option_rule": "id",
    "shipping_option_type": "id",
    "shipping_profile": "id",
    "provider_identity": "id",
    "view_configuration": "id",
    "workflow_execution": "id",
}

# =============================
# Discord Alert
# =============================
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

def send_discord_alert(message: str, is_error: bool = False):
    if not DISCORD_WEBHOOK:
        return
    try:
        payload = {"content": message[:1900]}
        requests.post(DISCORD_WEBHOOK, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")

# =============================
# PostgreSQL Password
# =============================
def get_postgres_password() -> str:
    try:
        secret_arn = os.getenv("RDS_SECRET_ARN")
        if not secret_arn:
            return ""
        
        result = subprocess.run(
            ['aws', 'secretsmanager', 'get-secret-value', 
             '--secret-id', secret_arn,
             '--query', 'SecretString', '--output', 'text'],
            capture_output=True, text=True, timeout=10
        )
        
        if result.returncode != 0:
            return ""
        
        secret = json.loads(result.stdout.strip())
        return secret.get('password', '')
    except Exception as e:
        logger.error(f"Failed to get secret: {e}")
        return ""

# =============================
# Database Connections
# =============================
@contextmanager
def get_postgres_connection():
    conn = None
    try:
        for attempt in range(ETLConfig.MAX_RETRIES):
            try:
                import psycopg2
                password = get_postgres_password()
                
                conn = psycopg2.connect(
                    host=os.getenv("RDS_HOST", "btc-ecommerce.cfqsac0ma55f.us-east-2.rds.amazonaws.com"),
                    port=int(os.getenv("RDS_PORT", 5432)),
                    database=os.getenv("RDS_DATABASE", "postgres"),
                    user=os.getenv("RDS_USER", "postgres"),
                    password=password,
                    sslmode='require',
                    connect_timeout=10
                )
                
                logger.info(f"✅ Connected to PostgreSQL")
                yield conn
                break
                
            except Exception as e:
                logger.error(f"PostgreSQL connection attempt {attempt + 1} failed: {e}")
                if attempt == ETLConfig.MAX_RETRIES - 1:
                    raise
                time.sleep(ETLConfig.RETRY_DELAY_SECONDS)
    finally:
        if conn:
            conn.close()
            logger.info("🔌 PostgreSQL connection closed")

@contextmanager
def get_motherduck_connection():
    conn = None
    try:
        for attempt in range(ETLConfig.MAX_RETRIES):
            try:
                token = os.getenv("MOTHERDUCK_TOKEN")
                db_name = os.getenv("MOTHERDUCK_DB", "ecommerce")
                
                conn = db.connect(f"md:{db_name}?motherduck_token={token}")
                logger.info(f"✅ Connected to MotherDuck")
                yield conn
                break
                
            except Exception as e:
                logger.error(f"MotherDuck connection attempt {attempt + 1} failed: {e}")
                if attempt == ETLConfig.MAX_RETRIES - 1:
                    raise
                time.sleep(ETLConfig.RETRY_DELAY_SECONDS)
    finally:
        if conn:
            conn.close()
            logger.info("🦆 MotherDuck connection closed")

# =============================
# Data Type Converter
# =============================
def convert_to_duckdb_compatible(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pandas dtypes to duckdb-compatible types"""
    for col in df.columns:
        if df[col].dtype == 'float64':
            if df[col].dropna().apply(lambda x: x == int(x) if not pd.isna(x) else True).all():
                df[col] = df[col].astype('Int64')
            else:
                df[col] = df[col].astype(float)
        
        elif df[col].dtype == 'object':
            sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
            if sample and isinstance(sample, (dict, list)):
                df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)
    
    return df

# =============================
# Drop Any Existing Object (View or Table)
# =============================
def drop_existing_object(duck_con, table_name: str):
    """Drop any existing object (view or table) with the same name"""
    try:
        # Check if object exists
        result = duck_con.execute(f"""
            SELECT table_type FROM information_schema.tables 
            WHERE table_name = '{table_name}' AND table_schema = 'main'
        """).fetchone()
        
        if result:
            object_type = result[0]
            if object_type == 'VIEW':
                logger.info(f"   Dropping existing view '{table_name}'")
                duck_con.execute(f'DROP VIEW IF EXISTS "{table_name}"')
            elif object_type == 'BASE TABLE':
                logger.info(f"   Dropping existing table '{table_name}'")
                duck_con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            return True
        return False
    except Exception as e:
        logger.warning(f"   Could not check/cleanup {table_name}: {e}")
        return False

# =============================
# Table Loader (Fixed)
# =============================
def load_table(pg_conn, duck_con, table_name: str, pk_col: str) -> ETLMetrics:
    metrics = ETLMetrics(table_name=table_name, start_time=datetime.now())
    
    try:
        # Check if table has data
        count_query = f'SELECT COUNT(*) as cnt FROM "{table_name}"'
        try:
            total_rows = pd.read_sql(count_query, pg_conn).iloc[0]['cnt']
            logger.info(f"📊 '{table_name}' has {total_rows:,} rows")
        except:
            total_rows = 0
        
        if total_rows == 0:
            metrics.status = "completed"
            metrics.rows_inserted = 0
            logger.info(f"✅ No rows found in '{table_name}'")
            return metrics
        
        # Load in batches
        rows_loaded = 0
        offset = 0
        table_created = False
        
        while True:
            query = f'SELECT * FROM "{table_name}" LIMIT {ETLConfig.BATCH_SIZE} OFFSET {offset}'
            df = pd.read_sql(query, pg_conn)
            
            if df.empty:
                break
            
            # Convert data types
            df = convert_to_duckdb_compatible(df)
            
            # Use unique temp table name
            temp_table = f"temp_{table_name}_{int(time.time() * 1000)}_{offset}"
            duck_con.register(temp_table, df)
            
            try:
                if not table_created:
                    # Drop any existing views/tables before creating
                    drop_existing_object(duck_con, table_name)
                    
                    # Create table from first batch
                    duck_con.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM {temp_table}')
                    table_created = True
                else:
                    # Insert batch
                    duck_con.execute(f'INSERT INTO "{table_name}" SELECT * FROM {temp_table}')
                
                rows_loaded += len(df)
                logger.info(f"   Loaded {rows_loaded:,} / {total_rows:,} rows")
                
            except Exception as e:
                logger.error(f"   Error loading batch: {e}")
                # Try with all columns as VARCHAR as fallback
                try:
                    df_str = df.astype(str)
                    duck_con.register(f"{temp_table}_str", df_str)
                    if not table_created:
                        drop_existing_object(duck_con, table_name)
                        duck_con.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM {temp_table}_str')
                        table_created = True
                    else:
                        duck_con.execute(f'INSERT INTO "{table_name}" SELECT * FROM {temp_table}_str')
                    rows_loaded += len(df)
                    logger.info(f"   Loaded {rows_loaded:,} rows (as strings)")
                except Exception as e2:
                    logger.error(f"   Fallback also failed: {e2}")
                    raise
            
            finally:
                # Drop both VIEW and TABLE versions of temp table to be safe
                try:
                    duck_con.execute(f'DROP VIEW IF EXISTS {temp_table}')
                except:
                    pass
                try:
                    duck_con.execute(f'DROP TABLE IF EXISTS {temp_table}')
                except:
                    pass
                try:
                    duck_con.execute(f'DROP VIEW IF EXISTS {temp_table}_str')
                except:
                    pass
                try:
                    duck_con.execute(f'DROP TABLE IF EXISTS {temp_table}_str')
                except:
                    pass
            
            offset += ETLConfig.BATCH_SIZE
            time.sleep(0.1)
        
        metrics.rows_inserted = rows_loaded
        metrics.status = "completed"
        
        if rows_loaded > 0:
            logger.info(f"✅ Loaded {rows_loaded:,} rows into '{table_name}'")
        
    except Exception as e:
        metrics.status = "failed"
        metrics.error_message = str(e)
        logger.error(f"❌ Failed to load '{table_name}': {e}")
    
    finally:
        metrics.end_time = datetime.now()
    
    return metrics

# =============================
# Main ETL Pipeline
# =============================
def run_etl_pipeline():
    start_time = datetime.now()
    all_metrics = []
    failed_tables = []
    
    send_discord_alert("🚀 **BTC Card Marketplace ETL Started**")
    
    try:
        with get_postgres_connection() as pg_conn, \
             get_motherduck_connection() as md_conn:
            
            # Get existing tables from source
            existing_tables = pd.read_sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
                pg_conn
            )["table_name"].tolist()
            
            # Filter to tables we want that exist
            tables_to_load = [t for t in PRIMARY_KEYS if t in existing_tables]
            skipped = [t for t in PRIMARY_KEYS if t not in existing_tables]
            
            if skipped:
                logger.info(f"⚠️ Skipping {len(skipped)} tables not in source")
            
            logger.info(f"📋 Loading {len(tables_to_load)} tables")
            
            # Load tables one by one
            for i, table in enumerate(tables_to_load, 1):
                logger.info(f"[{i}/{len(tables_to_load)}] Processing '{table}'...")
                
                metrics = load_table(pg_conn, md_conn, table, PRIMARY_KEYS[table])
                all_metrics.append(metrics)
                
                if metrics.status == "failed":
                    failed_tables.append(table)
                
                time.sleep(0.5)
            
            # Send completion report
            duration = (datetime.now() - start_time).total_seconds()
            total_rows = sum(m.rows_inserted for m in all_metrics)
            success_count = len([m for m in all_metrics if m.status == "completed"])
            
            report = f"""✅ **ETL Completed!**
⏱️ Duration: {duration:.1f} seconds
📊 Tables loaded: {success_count}/{len(all_metrics)}
📈 Total rows inserted: {total_rows:,}
❌ Failed: {len(failed_tables)}"""
            
            if failed_tables:
                report += f"\n⚠️ Failed: {', '.join(failed_tables[:10])}"
            
            send_discord_alert(report)
            logger.info(f"✅ ETL completed in {duration:.1f} seconds")
            
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"❌ Pipeline failed: {e}")
        send_discord_alert(f"❌ **ETL Failed!**\n```{error_details[:1500]}```", is_error=True)
        raise

# =============================
# Run
# =============================
if __name__ == "__main__":
    required_vars = ['MOTHERDUCK_TOKEN', 'DISCORD_WEBHOOK']
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {missing}")
    
    run_etl_pipeline()