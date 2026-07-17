import duckdb as db
import os
import boto3
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
SES_FROM_EMAIL = os.getenv("SES_FROM_EMAIL")
SES_TO_EMAIL = os.getenv("SES_TO_EMAIL")
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
            Destination={"ToAddresses": [SES_TO_EMAIL, SES_TO_EMAIL_BFC]},
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
    bch_con = db.connect("md:btc_analytics")
    logging.info("✅ Connected to MotherDuck")

    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    seven_days_ago = today - timedelta(days=7)

    # ─────────────────────────────
    # BCH METRICS
    # ─────────────────────────────
    total_users = bch_con.execute('SELECT COUNT(*) FROM "user"').fetchone()[0]
    new_users_yesterday = bch_con.execute(f'SELECT COUNT(*) FROM "user" WHERE DATE(created_at) = \'{yesterday}\'').fetchone()[0]
    new_users_7d = bch_con.execute(f'SELECT COUNT(*) FROM "user" WHERE created_at >= \'{seven_days_ago}\'').fetchone()[0]
    users_onboarded = bch_con.execute('SELECT COUNT(*) FROM "user" WHERE onboarding_completed_at IS NOT NULL').fetchone()[0]
    onboarding_rate = round((users_onboarded / total_users * 100), 1) if total_users > 0 else 0

    total_profiles = bch_con.execute('SELECT COUNT(*) FROM "profile"').fetchone()[0]
    completed_profiles = bch_con.execute('''
        SELECT COUNT(*) FROM "profile"
        WHERE (
            CASE WHEN username IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN bio IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN headline IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN summary IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN interests IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN profile_picture IS NOT NULL THEN 1 ELSE 0 END
        ) >= 3
    ''').fetchone()[0]
    profile_completion_rate = round((completed_profiles / total_profiles * 100), 1) if total_profiles > 0 else 0

    total_proofs = bch_con.execute('SELECT COUNT(*) FROM "proof"').fetchone()[0]
    new_proofs_yesterday = bch_con.execute(f'SELECT COUNT(*) FROM "proof" WHERE DATE(created_at) = \'{yesterday}\'').fetchone()[0]
    new_proofs_7d = bch_con.execute(f'SELECT COUNT(*) FROM "proof" WHERE created_at >= \'{seven_days_ago}\'').fetchone()[0]

    total_orgs = bch_con.execute('SELECT COUNT(*) FROM "organization"').fetchone()[0]
    new_orgs_yesterday = bch_con.execute(f'SELECT COUNT(*) FROM "organization" WHERE DATE(submitted_at) = \'{yesterday}\'').fetchone()[0]
    total_org_members = bch_con.execute('SELECT COUNT(*) FROM "organizationmember"').fetchone()[0]

    total_opportunities = bch_con.execute('SELECT COUNT(*) FROM "opportunity"').fetchone()[0]
    new_opportunities_yesterday = bch_con.execute(f'SELECT COUNT(*) FROM "opportunity" WHERE DATE(created_at) = \'{yesterday}\'').fetchone()[0]
    new_opportunities_7d = bch_con.execute(f'SELECT COUNT(*) FROM "opportunity" WHERE created_at >= \'{seven_days_ago}\'').fetchone()[0]
    active_opportunities = bch_con.execute('SELECT COUNT(*) FROM "opportunity" WHERE status = \'active\' AND deleted_at IS NULL').fetchone()[0]

    total_applications = bch_con.execute('SELECT COUNT(*) FROM "application"').fetchone()[0]
    new_applications_yesterday = bch_con.execute(f'SELECT COUNT(*) FROM "application" WHERE DATE(applied_at) = \'{yesterday}\'').fetchone()[0]
    new_applications_7d = bch_con.execute(f'SELECT COUNT(*) FROM "application" WHERE applied_at >= \'{seven_days_ago}\'').fetchone()[0]
    total_invites = bch_con.execute('SELECT COUNT(*) FROM "orginvite"').fetchone()[0]

    # ─────────────────────────────
    # BUILD EMAIL
    # ─────────────────────────────
    snapshot_rows = (
        metric_row("👥 Total Users", f"{total_users:,}")
        + metric_row("🆕 New Users Yesterday", str(new_users_yesterday))
        + metric_row("📈 New Users (7d)", str(new_users_7d))
        + metric_row("✅ Onboarding Completion Rate", f"{onboarding_rate}%")
    )

    network_rows = (
        na_row("🟢 7-Day Active Users")
        + metric_row("📋 Profile Completion Rate", f"{profile_completion_rate}%", f"({completed_profiles}/{total_profiles})")
        + metric_row("🏆 Proof Entries Yesterday", str(new_proofs_yesterday))
        + metric_row("📦 Total Proof Entries", f"{total_proofs:,}")
        + metric_row("📈 New Proofs (7d)", str(new_proofs_7d))
        + na_row("⚡ Activation Rate")
        + metric_row("🏢 Total Organizations", f"{total_orgs:,}")
        + metric_row("🆕 New Organizations Yesterday", str(new_orgs_yesterday))
        + metric_row("🤝 Total Org Members", f"{total_org_members:,}")
    )

    opportunity_rows = (
        metric_row("📌 Total Opportunities", f"{total_opportunities:,}")
        + metric_row("🆕 New Opportunities Yesterday", str(new_opportunities_yesterday))
        + metric_row("📈 New Opportunities (7d)", str(new_opportunities_7d))
        + metric_row("✅ Active Opportunities", str(active_opportunities))
        + metric_row("📝 Total Applications", f"{total_applications:,}")
        + metric_row("🆕 New Applications Yesterday", str(new_applications_yesterday))
        + metric_row("📈 New Applications (7d)", str(new_applications_7d))
        + metric_row("🔗 Total Org Invites", f"{total_invites:,}")
        + na_row("🤖 Matches Generated")
        + na_row("👆 Match/Opportunity Clicks")
        + na_row("🌟 Qualified Connections")
    )

    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
        <div style="max-width:640px;margin:32px auto;padding:0 16px;">

            <!-- Header -->
            <div style="background:#1a1a2e;padding:28px 24px;border-radius:8px 8px 0 0;text-align:center;">
                <h1 style="margin:0;color:white;font-size:22px;">⚡ Daily CEO Metrics</h1>
                <p style="margin:8px 0 0;color:#a0aec0;font-size:14px;">Bitcoin Culture Hub (BCH) &nbsp;·&nbsp; {today.strftime('%B %d, %Y')}</p>
            </div>

            <!-- Body -->
            <div style="background:#f3f4f6;padding:24px 0;">
                {section("1️⃣ BCH Snapshot", "#2563eb", snapshot_rows)}
                {section("2️⃣ BCH Network Health", "#16a34a", network_rows)}
                {section("3️⃣ BCH Opportunity Engine", "#9333ea", opportunity_rows)}
            </div>

            <!-- Footer -->
            <div style="text-align:center;padding:16px;color:#9ca3af;font-size:12px;">
                Generated automatically by BCH Data Pipeline &nbsp;·&nbsp; {today.strftime('%B %d, %Y')}
            </div>

        </div>
    </body>
    </html>
    """

    subject = f"Daily CEO Metrics — BCH — {today.strftime('%B %d, %Y')}"
    send_email(subject, html)

except Exception as e:
    logging.error(f"❌ BCH CEO metrics failed: {e}")
    import traceback
    logging.error(traceback.format_exc())
    try:
        send_email(
            f"❌ BCH CEO Metrics Failed — {datetime.utcnow().date()}",
            f"<p>The BCH CEO metrics script failed with the following error:</p><pre>{str(e)}</pre>"
        )
    except Exception:
        pass

finally:
    try:
        bch_con.close()
    except Exception:
        pass