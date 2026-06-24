import duckdb as db
import os
import requests
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def send_discord_report(message):
    chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
    for chunk in chunks:
        payload = {"content": chunk}
        try:
            r = requests.post(DISCORD_WEBHOOK, json=payload)
            if r.status_code != 204:
                logging.warning(f"Failed to send Discord report: {r.status_code} {r.text}")
        except Exception as e:
            logging.error(f"Exception sending Discord report: {e}")

try:
    logging.info("🔌 Connecting to MotherDuck...")
    duck_con = db.connect("md:btc_analytics")
    logging.info("✅ Connected to MotherDuck")

    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    seven_days_ago = today - timedelta(days=7)

    # -----------------------------
    # CEO Snapshot
    # -----------------------------
    total_users = duck_con.execute('SELECT COUNT(*) FROM "user"').fetchone()[0]

    new_users_yesterday = duck_con.execute(f'''
        SELECT COUNT(*) FROM "user"
        WHERE DATE(created_at) = '{yesterday}'
    ''').fetchone()[0]

    new_users_7d = duck_con.execute(f'''
        SELECT COUNT(*) FROM "user"
        WHERE created_at >= '{seven_days_ago}'
    ''').fetchone()[0]

    users_onboarded = duck_con.execute(f'''
        SELECT COUNT(*) FROM "user"
        WHERE onboarding_completed_at IS NOT NULL
    ''').fetchone()[0]

    onboarding_rate = round((users_onboarded / total_users * 100), 1) if total_users > 0 else 0

    # -----------------------------
    # BCH Network Health
    # -----------------------------
    total_profiles = duck_con.execute('SELECT COUNT(*) FROM "profile"').fetchone()[0]

    completed_profiles = duck_con.execute('''
        SELECT COUNT(*) FROM "profile"
        WHERE username IS NOT NULL
        AND bio IS NOT NULL
        AND headline IS NOT NULL
        AND summary IS NOT NULL
        AND interests IS NOT NULL
        AND profile_picture IS NOT NULL
    ''').fetchone()[0]

    profile_completion_rate = round((completed_profiles / total_profiles * 100), 1) if total_profiles > 0 else 0

    total_proofs = duck_con.execute('SELECT COUNT(*) FROM "proof"').fetchone()[0]

    new_proofs_yesterday = duck_con.execute(f'''
        SELECT COUNT(*) FROM "proof"
        WHERE DATE(created_at) = '{yesterday}'
    ''').fetchone()[0]

    new_proofs_7d = duck_con.execute(f'''
        SELECT COUNT(*) FROM "proof"
        WHERE created_at >= '{seven_days_ago}'
    ''').fetchone()[0]

    total_orgs = duck_con.execute('SELECT COUNT(*) FROM "organization"').fetchone()[0]

    new_orgs_yesterday = duck_con.execute(f'''
        SELECT COUNT(*) FROM "organization"
        WHERE DATE(submitted_at) = '{yesterday}'
    ''').fetchone()[0]

    total_org_members = duck_con.execute('SELECT COUNT(*) FROM "organizationmember"').fetchone()[0]

    # -----------------------------
    # BCH Opportunity Engine
    # -----------------------------
    total_opportunities = duck_con.execute('SELECT COUNT(*) FROM "opportunity"').fetchone()[0]

    new_opportunities_yesterday = duck_con.execute(f'''
        SELECT COUNT(*) FROM "opportunity"
        WHERE DATE(created_at) = '{yesterday}'
    ''').fetchone()[0]

    new_opportunities_7d = duck_con.execute(f'''
        SELECT COUNT(*) FROM "opportunity"
        WHERE created_at >= '{seven_days_ago}'
    ''').fetchone()[0]

    active_opportunities = duck_con.execute('''
        SELECT COUNT(*) FROM "opportunity"
        WHERE status = 'active'
        AND deleted_at IS NULL
    ''').fetchone()[0]

    total_applications = duck_con.execute('SELECT COUNT(*) FROM "application"').fetchone()[0]

    new_applications_yesterday = duck_con.execute(f'''
        SELECT COUNT(*) FROM "application"
        WHERE DATE(applied_at) = '{yesterday}'
    ''').fetchone()[0]

    new_applications_7d = duck_con.execute(f'''
        SELECT COUNT(*) FROM "application"
        WHERE applied_at >= '{seven_days_ago}'
    ''').fetchone()[0]

    total_invites = duck_con.execute('SELECT COUNT(*) FROM "orginvite"').fetchone()[0]

    # -----------------------------
    # Build Report
    # -----------------------------
    report = f"""
📊 **Daily CEO Metrics — BCH — {today.strftime('%B %d, %Y')}**

━━━━━━━━━━━━━━━━━━━━━━
**1️⃣ CEO SNAPSHOT**
━━━━━━━━━━━━━━━━━━━━━━
👥 Total Users: **{total_users}**
🆕 New Users Yesterday: **{new_users_yesterday}**
📈 New Users (7d): **{new_users_7d}**
✅ Onboarding Completion Rate: **{onboarding_rate}%**
💰 GMV: *Data not available yet*
💵 Net Revenue: *Data not available yet*

━━━━━━━━━━━━━━━━━━━━━━
**2️⃣ BCH NETWORK HEALTH**
━━━━━━━━━━━━━━━━━━━━━━
👥 Total Users: **{total_users}**
🟢 7-Day Active Users: *Data not available yet*
📋 Profile Completion Rate: **{profile_completion_rate}%** ({completed_profiles}/{total_profiles})
🏆 Proof-of-Work Entries Yesterday: **{new_proofs_yesterday}**
📦 Total Proof Entries: **{total_proofs}**
📈 New Proofs (7d): **{new_proofs_7d}**
⚡ Activation Rate: *Data not available yet*
🏢 Total Organizations: **{total_orgs}**
🆕 New Organizations Yesterday: **{new_orgs_yesterday}**
🤝 Total Org Members: **{total_org_members}**

━━━━━━━━━━━━━━━━━━━━━━
**3️⃣ BCH OPPORTUNITY ENGINE**
━━━━━━━━━━━━━━━━━━━━━━
📌 Total Opportunities: **{total_opportunities}**
🆕 New Opportunities Yesterday: **{new_opportunities_yesterday}**
📈 New Opportunities (7d): **{new_opportunities_7d}**
✅ Active Opportunities: **{active_opportunities}**
📝 Total Applications: **{total_applications}**
🆕 New Applications Yesterday: **{new_applications_yesterday}**
📈 New Applications (7d): **{new_applications_7d}**
🔗 Total Org Invites: **{total_invites}**
🤖 Matches Generated: *Data not available yet*
👆 Match/Opportunity Clicks: *Data not available yet*
🌟 Qualified Connections: *Data not available yet*
"""

    send_discord_report(report)
    logging.info("✅ CEO metrics report sent to Discord")

except Exception as e:
    logging.error(f"❌ CEO metrics failed: {e}")
    if DISCORD_WEBHOOK:
        requests.post(DISCORD_WEBHOOK, json={"content": f"❌ CEO Metrics Failed!\n```{str(e)}```"})

finally:
    try:
        duck_con.close()
    except Exception:
        pass