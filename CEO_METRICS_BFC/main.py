import duckdb as db
import os
import boto3
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
SES_FROM_EMAIL = os.getenv("SES_FROM_EMAIL")
SES_TO_EMAIL_BFC = os.getenv("SES_TO_EMAIL_BFC")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def send_email(subject, html_body):
    client = boto3.client("ses", region_name=AWS_REGION)
    try:
        client.send_email(
            Source=SES_FROM_EMAIL,
            Destination={"ToAddresses": [SES_TO_EMAIL_BFC]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Html": {"Data": html_body}}
            }
        )
        logging.info("✅ Email sent successfully")
    except Exception as e:
        logging.error(f"❌ Failed to send email: {e}")

def metric_row(label, value, note=None):
    note_html = f"<span style='color:#888;font-size:12px;'> {note}</span>" if note else ""
    return f"""
    <tr>
        <td style="padding:8px 12px;color:#555;font-size:14px;border-bottom:1px solid #f3f4f6;">{label}</td>
        <td style="padding:8px 12px;font-weight:bold;font-size:14px;color:#111;border-bottom:1px solid #f3f4f6;">{value}{note_html}</td>
    </tr>
    """

def na_row(label):
    return f"""
    <tr>
        <td style="padding:8px 12px;color:#555;font-size:14px;border-bottom:1px solid #f3f4f6;">{label}</td>
        <td style="padding:8px 12px;font-size:14px;color:#aaa;font-style:italic;border-bottom:1px solid #f3f4f6;">Data not available yet</td>
    </tr>
    """

def section(title, color, rows_html):
    return f"""
    <div style="margin-bottom:32px;">
        <div style="background:{color};padding:12px 16px;border-radius:6px 6px 0 0;">
            <h2 style="margin:0;color:white;font-size:16px;font-family:Arial,sans-serif;">{title}</h2>
        </div>
        <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 6px 6px;">
            {rows_html}
        </table>
    </div>
    """

try:
    logging.info("🔌 Connecting to MotherDuck...")
    bfc_con = db.connect("md:ecommerce")
    logging.info("✅ Connected to MotherDuck")

    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    seven_days_ago = today - timedelta(days=7)

    # ─────────────────────────────
    # BFC METRICS
    # ─────────────────────────────
    total_products = bfc_con.execute('SELECT COUNT(*) FROM "product" WHERE deleted_at IS NULL').fetchone()[0]
    active_listings = bfc_con.execute('SELECT COUNT(*) FROM "product" WHERE status = \'published\' AND deleted_at IS NULL').fetchone()[0]
    new_listings_yesterday = bfc_con.execute(f'SELECT COUNT(*) FROM "product" WHERE DATE(created_at) = \'{yesterday}\' AND deleted_at IS NULL').fetchone()[0]
    new_listings_7d = bfc_con.execute(f'SELECT COUNT(*) FROM "product" WHERE created_at >= \'{seven_days_ago}\' AND deleted_at IS NULL').fetchone()[0]

    total_customers = bfc_con.execute('SELECT COUNT(*) FROM "customer" WHERE deleted_at IS NULL').fetchone()[0]
    new_customers_yesterday = bfc_con.execute(f'SELECT COUNT(*) FROM "customer" WHERE DATE(created_at) = \'{yesterday}\' AND deleted_at IS NULL').fetchone()[0]
    new_customers_7d = bfc_con.execute(f'SELECT COUNT(*) FROM "customer" WHERE created_at >= \'{seven_days_ago}\' AND deleted_at IS NULL').fetchone()[0]

    total_vendors = bfc_con.execute('SELECT COUNT(*) FROM "vendor" WHERE deleted_at IS NULL').fetchone()[0]
    new_vendors_yesterday = bfc_con.execute(f'SELECT COUNT(*) FROM "vendor" WHERE DATE(created_at) = \'{yesterday}\' AND deleted_at IS NULL').fetchone()[0]

    total_orders = bfc_con.execute('SELECT COUNT(*) FROM "marketplace_order" WHERE deleted_at IS NULL').fetchone()[0]
    new_orders_yesterday = bfc_con.execute(f'SELECT COUNT(*) FROM "marketplace_order" WHERE DATE(created_at) = \'{yesterday}\' AND deleted_at IS NULL').fetchone()[0]
    new_orders_7d = bfc_con.execute(f'SELECT COUNT(*) FROM "marketplace_order" WHERE created_at >= \'{seven_days_ago}\' AND deleted_at IS NULL').fetchone()[0]
    completed_orders = bfc_con.execute('SELECT COUNT(*) FROM "marketplace_order" WHERE status = \'completed\' AND deleted_at IS NULL').fetchone()[0]

    total_gmv_raw = bfc_con.execute('SELECT COALESCE(SUM(amount), 0) FROM "marketplace_order" WHERE status = \'completed\' AND deleted_at IS NULL').fetchone()[0]
    total_gmv = f"${total_gmv_raw:,.2f}"

    gmv_yesterday_raw = bfc_con.execute(f'SELECT COALESCE(SUM(amount), 0) FROM "marketplace_order" WHERE DATE(created_at) = \'{yesterday}\' AND status = \'completed\' AND deleted_at IS NULL').fetchone()[0]
    gmv_yesterday = f"${gmv_yesterday_raw:,.2f}"

    total_offers = bfc_con.execute('SELECT COUNT(*) FROM "marketplace_offer" WHERE deleted_at IS NULL').fetchone()[0]
    new_offers_yesterday = bfc_con.execute(f'SELECT COUNT(*) FROM "marketplace_offer" WHERE DATE(created_at) = \'{yesterday}\' AND deleted_at IS NULL').fetchone()[0]

    sell_through_rate = round((completed_orders / total_products * 100), 1) if total_products > 0 else 0

    # ─────────────────────────────
    # BUILD EMAIL
    # ─────────────────────────────
    supply_rows = (
        metric_row("📦 Total Listings", f"{total_products:,}")
        + metric_row("✅ Active Listings", f"{active_listings:,}")
        + metric_row("🆕 New Listings Yesterday", str(new_listings_yesterday))
        + metric_row("📈 New Listings (7d)", str(new_listings_7d))
        + metric_row("🏪 Total Vendors/Sellers", f"{total_vendors:,}")
        + metric_row("🆕 New Vendors Yesterday", str(new_vendors_yesterday))
        + na_row("✔️ Verified Sellers")
    )

    demand_rows = (
        metric_row("👥 Total Customers", f"{total_customers:,}")
        + metric_row("🆕 New Customers Yesterday", str(new_customers_yesterday))
        + metric_row("📈 New Customers (7d)", str(new_customers_7d))
        + metric_row("🛒 Total Orders", f"{total_orders:,}")
        + metric_row("🆕 New Orders Yesterday", str(new_orders_yesterday))
        + metric_row("📈 New Orders (7d)", str(new_orders_7d))
        + metric_row("✅ Completed Orders", str(completed_orders))
        + metric_row("🤝 Total Offers Made", f"{total_offers:,}")
        + metric_row("🆕 New Offers Yesterday", str(new_offers_yesterday))
        + metric_row("💰 Total GMV (Completed)", total_gmv)
        + metric_row("💰 GMV Yesterday", gmv_yesterday)
        + metric_row("📊 Sell-Through Rate", f"{sell_through_rate}%")
        + na_row("👁️ Listing Views")
        + na_row("❤️ Favorites/Watchlist Adds")
        + na_row("🛍️ Checkout Starts")
    )

    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
        <div style="max-width:640px;margin:32px auto;padding:0 16px;">

            <!-- Header -->
            <div style="background:#1a1a2e;padding:28px 24px;border-radius:8px 8px 0 0;text-align:center;">
                <h1 style="margin:0;color:white;font-size:22px;">🃏 Daily CEO Metrics</h1>
                <p style="margin:8px 0 0;color:#a0aec0;font-size:14px;">Bitcoin For Collectors (BFC) &nbsp;·&nbsp; {today.strftime('%B %d, %Y')}</p>
            </div>

            <!-- Body -->
            <div style="background:#f3f4f6;padding:24px 0;">
                {section("4️⃣ BFC Supply", "#0891b2", supply_rows)}
                {section("5️⃣ BFC Demand & Liquidity", "#dc2626", demand_rows)}
            </div>

            <!-- Footer -->
            <div style="text-align:center;padding:16px;color:#9ca3af;font-size:12px;">
                Generated automatically by BFC Data Pipeline &nbsp;·&nbsp; {today.strftime('%B %d, %Y')}
            </div>

        </div>
    </body>
    </html>
    """

    subject = f"Daily CEO Metrics — BFC — {today.strftime('%B %d, %Y')}"
    send_email(subject, html)

except Exception as e:
    logging.error(f"❌ BFC CEO metrics failed: {e}")
    import traceback
    logging.error(traceback.format_exc())
    try:
        send_email(
            f"❌ BFC CEO Metrics Failed — {datetime.utcnow().date()}",
            f"<p>The BFC CEO metrics script failed with the following error:</p><pre>{str(e)}</pre>"
        )
    except Exception:
        pass

finally:
    try:
        bfc_con.close()
    except Exception:
        pass