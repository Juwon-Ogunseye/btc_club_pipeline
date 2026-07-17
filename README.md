# btc-data-hub

> Automated Bitcoin data infrastructure — ETL pipelines, analytics warehouse, and daily reporting for **Bitcoin Culture Hub (BCH)** and **Bitcoin For Collectors (BFC)**.

---

## Architecture

```
PostgreSQL (AWS RDS)          MotherDuck (DuckDB Cloud)        Reporting
─────────────────────         ──────────────────────────       ──────────
  BCH Production DB    ──►    btc_analytics database    ──►    Daily Email (AWS SES)
  BFC Production DB    ──►    ecommerce database         ──►    Metabase Dashboards (AWS EC2)
                              
GitHub Actions orchestrates all pipelines on a daily schedule
```

---

## Stack

| Layer | Technology |
|---|---|
| Source | PostgreSQL on AWS RDS |
| Orchestration | GitHub Actions |
| Transformation | Python, pandas, SQLAlchemy |
| Warehouse | DuckDB + MotherDuck |
| Visualization | Metabase on AWS EC2 |
| Reporting | AWS SES (HTML email) |
| Alerts | Discord Webhooks |

---

## Pipelines

```
.github/workflows/
├── data-pipeline.yml        →  BCH ETL          (runs 12:35 AM UTC)
├── ecommerce-etl.yml        →  BFC ETL          (runs  1:00 AM UTC)
├── ceo-metrics.yml          →  BCH CEO Report   (runs  1:05 AM UTC)
└── ceo-metrics-bfc.yml      →  BFC CEO Report   (runs  1:15 AM UTC)
```

Each pipeline:
1. Spins up a GitHub Actions runner
2. Whitelists the runner IP on the RDS security group
3. Extracts data from PostgreSQL into pandas DataFrames
4. Loads into MotherDuck incrementally
5. Revokes the runner IP from the security group
6. Sends a Discord alert with the run summary

---

## Repository Structure

```
btc-data-hub/
├── .github/workflows/           # GitHub Actions pipeline definitions
├── CEO_METRICS/                 # BCH daily CEO metrics email
├── CEO_METRICS_BFC/             # BFC daily CEO metrics email
├── ecommerce-pipeline/          # BFC ETL script
├── main.py                      # BCH ETL script
├── requirements.txt
└── README.md
```

---

## Data Flow

```
                    ┌─────────────────────────────────────┐
                    │         GitHub Actions               │
                    │   (Scheduled daily cron jobs)        │
                    └────────────┬────────────────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                                     │
              ▼                                     ▼
   ┌─────────────────────┐             ┌─────────────────────┐
   │   BCH PostgreSQL    │             │   BFC PostgreSQL    │
   │   (AWS RDS)         │             │   (AWS RDS)         │
   └────────┬────────────┘             └────────┬────────────┘
            │                                   │
            ▼                                   ▼
   ┌─────────────────────┐             ┌─────────────────────┐
   │  btc_analytics DB   │             │   ecommerce DB      │
   │  (MotherDuck)       │             │   (MotherDuck)      │
   └────────┬────────────┘             └────────┬────────────┘
            │                                   │
            ▼                                   ▼
   ┌─────────────────────────────────────────────────────────┐
   │              Metabase (AWS EC2)                         │
   │         Live dashboards for both products               │
   └─────────────────────────────────────────────────────────┘
            │                                   │
            ▼                                   ▼
   ┌─────────────────┐                 ┌─────────────────┐
   │  BCH CEO Email  │                 │  BFC CEO Email  │
   │  (AWS SES)      │                 │  (AWS SES)      │
   └─────────────────┘                 └─────────────────┘
```

---

## Pipeline Flow

```mermaid
flowchart TD
    subgraph Sources["📦 Data Sources"]
        A[(BCH PostgreSQL\nbtc-opten-ps\nAWS RDS)]
        B[(BFC PostgreSQL\nbtc-ecommerce\nAWS RDS)]
    end

    subgraph Orchestration["⚙️ Orchestration"]
        C[GitHub Actions\nDaily Cron Jobs]
    end

    subgraph ETL["🔄 ETL Layer"]
        D[BCH ETL Pipeline\nPython + SQLAlchemy\n12:35 AM UTC]
        E[BFC ETL Pipeline\nPython + psycopg2\n1:00 AM UTC]
    end

    subgraph Warehouse["🏠 Analytics Warehouse"]
        F[(MotherDuck\nbtc_analytics DB\nDuckDB Cloud)]
        G[(MotherDuck\necommerce DB\nDuckDB Cloud)]
    end

    subgraph Reporting["📊 Reporting & Visualization"]
        H[Metabase\nAWS EC2\nLive Dashboards]
        I[BCH CEO Email\nAWS SES\n1:05 AM UTC]
        J[BFC CEO Email\nAWS SES\n1:15 AM UTC]
    end

    subgraph Alerts["🔔 Alerts"]
        K[Discord Webhook\nPipeline Status]
    end

    C -->|Triggers| D
    C -->|Triggers| E
    A -->|SSL/TLS| D
    B -->|SSL/TLS| E
    D -->|Incremental Load| F
    E -->|Incremental Load| G
    F --> H
    G --> H
    F --> I
    G --> J
    D -->|Run Summary| K
    E -->|Run Summary| K
```

---

## BCH Pipeline Detail

```mermaid
flowchart LR
    subgraph Source["BCH Source"]
        A[(PostgreSQL\nbtc-opten-ps)]
    end

    subgraph Tables["Tables Synced"]
        direction TB
        T1[user]
        T2[profile]
        T3[organization]
        T4[opportunity]
        T5[application]
        T6[proof]
        T7[organizationmember]
        T8[orginvite]
        T9[+ more]
    end

    subgraph Process["ETL Process"]
        E1[Extract\nSQLAlchemy]
        E2[Transform\npandas]
        E3[Load\nDuckDB]
    end

    subgraph Output["Output"]
        W[(MotherDuck\nbtc_analytics)]
        M[Metabase\nDashboards]
        R[CEO Email\nAWS SES]
        D[Discord\nAlert]
    end

    A --> Tables
    Tables --> E1
    E1 --> E2
    E2 --> E3
    E3 --> W
    W --> M
    W --> R
    E3 --> D
```

---

## BFC Pipeline Detail

```mermaid
flowchart LR
    subgraph Source["BFC Source"]
        A[(PostgreSQL\nbtc-ecommerce\nmedusa-bitcoin-card-backend)]
    end

    subgraph Tables["Tables Synced"]
        direction TB
        T1[product]
        T2[customer]
        T3[vendor]
        T4[marketplace_order]
        T5[marketplace_offer]
        T6[payment]
        T7[cart]
        T8[image]
        T9[+ 126 more]
    end

    subgraph Process["ETL Process"]
        E1[Extract\npsycopg2]
        E2[Transform\npandas + batch]
        E3[Load\nDuckDB]
    end

    subgraph Output["Output"]
        W[(MotherDuck\necommerce)]
        M[Metabase\nDashboards]
        R[CEO Email\nAWS SES]
        D[Discord\nAlert]
    end

    A --> Tables
    Tables --> E1
    E1 --> E2
    E2 --> E3
    E3 --> W
    W --> M
    W --> R
    E3 --> D
```

---

## Key Features

- **Incremental loading** — only new rows are inserted on each run, no full reloads
- **Schema drift detection** — tables are automatically recreated if the source schema changes
- **Dynamic IP whitelisting** — runner IP is added and removed from RDS security group on every run
- **Retry logic** — failed connections retry up to 3 times before alerting
- **Discord alerts** — every pipeline run posts a success or failure summary
- **Daily CEO email** — HTML metrics report delivered every morning via AWS SES
- **Metabase dashboards** — live analytics on top of MotherDuck via self-hosted Metabase on EC2

---

## Local Development

```bash
# Clone
git clone https://github.com/Bitcoin-Culture-Hub/btc-data-hub.git
cd btc-data-hub

# Install dependencies
pip install -r requirements.txt

# Copy and fill in environment variables
cp .env.example .env

# Run BCH pipeline
python main.py

# Run BFC pipeline
cd ecommerce-pipeline && python main.py
```

---

## Requirements

- Python 3.10+
- MotherDuck account
- AWS account with RDS and SES access
- PostgreSQL access to BCH and BFC production databases

---

## License

MIT