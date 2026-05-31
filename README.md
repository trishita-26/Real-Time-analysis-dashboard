<div align="center">

<br/>

```
██████╗ ███████╗ █████╗ ██╗     ████████╗██╗███╗   ███╗███████╗
██╔══██╗██╔════╝██╔══██╗██║        ██║   ██║████╗ ████║██╔════╝
██████╔╝█████╗  ███████║██║        ██║   ██║██╔████╔██║█████╗  
██╔══██╗██╔══╝  ██╔══██║██║        ██║   ██║██║╚██╔╝██║██╔══╝  
██║  ██║███████╗██║  ██║███████╗   ██║   ██║██║ ╚═╝ ██║███████╗
╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═╝╚═╝     ╚═╝╚══════╝

     ANALYSIS DASHBOARD  ·  Streaming Data at the Speed of Now
```

<br/>

[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.8%2B-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-IsolationForest-F7931E?style=flat-square&logo=scikit-learn&logoColor=white)](https://scikit-learn.org)
[![Power BI](https://img.shields.io/badge/Power%20BI-DirectQuery-F2C811?style=flat-square&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com)
[![License](https://img.shields.io/badge/License-MIT-22C55E?style=flat-square)](LICENSE)

<br/>

> **151,000+ transactions streamed · 14,966 anomalies detected · 16 second pipeline · Live Power BI dashboard**

*A production-grade real-time data engineering pipeline that streams e-commerce events through Apache Kafka, detects anomalies using unsupervised machine learning, and surfaces live business intelligence through Power BI DirectQuery — all automated with a single command.*

<br/>

</div>

---

## What Makes This Different

Most anomaly detection projects download a clean CSV and run a model. We built something different:

| Standard Approach | This Project |
|---|---|
| Static CSV dataset | Live Kafka event stream |
| Pre-labeled anomalies | 8 types of injected real-world corruption |
| Run ML on clean data | Run ML on raw dirty data intentionally |
| Manual steps between stages | Single command pipeline automation |
| Static dashboard snapshot | Power BI DirectQuery — live data on refresh |
| One data flow | Two parallel flows — ML on raw, insights on clean |
| No alerting | Threshold-based anomaly rate monitoring |

The producer injects **8 real-world anomaly types** that actually occur in production e-commerce systems — not random noise:

```
1. Negative prices          → refunds logged incorrectly
2. Currency mismatch        → paise vs rupees (price × 1000)
3. Bot traffic              → user_id clusters, quantity = 0
4. Schema drift             → prod_id vs product_id rename
5. Missing payment fields   → mobile app version bug
6. Corrupt city encoding    → DELHI / delhi / DEL / Dilli
7. Invalid event types      → VIEW / AddToCart / BUY
8. Timestamp corruption     → NaT / epoch zero / wrong format
```

This closed-loop design means we can actually **validate** the ML model — we know exactly what we injected and can verify what was found.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DOCKER COMPOSE                                   │
│  ┌─────────────┐    ┌──────────────────────┐    ┌─────────────────────┐ │
│  │  Zookeeper  │◄──►│    Apache Kafka       │    │    PostgreSQL        │ │
│  │  port 2181  │    │    port 9092          │    │    port 5433         │ │
│  └─────────────┘    └──────────┬───────────┘    └──────────┬──────────┘ │
└─────────────────────────────────┼───────────────────────────┼────────────┘
                                  │                           │
         ┌────────────────────────┼───────────────────────────┼──────────┐
         │                        │                           │          │
  ┌──────▼──────┐          ┌──────▼──────┐                   │          │
  │  producer   │          │  consumer   │──── INSERT ───────►│ raw_data │
  │  Injects 8  │─ stream ►│  Batch      │                   │          │
  │  anomaly    │          │  commits    │◄─── AUTO ──────────┤          │
  │  types      │          │  every 50   │   cleaning.sql     │          │
  └─────────────┘          └─────────────┘                   │          │
                                                              │ cleaned  │
                           ┌─────────────────────────────┐   │ _data    │
                           │       pipeline.py            │   │          │
                           │  ┌─────────────────────────┐│   │ anomaly  │
                           │  │   IsolationForest ML    ││──►│ _data    │
                           │  │   raw_data → anomaly    ││   │          │
                           │  └─────────────────────────┘│   │ business │
                           │  ┌─────────────────────────┐│   │ _insights│
                           │  │   Business Analysis     ││──►│          │
                           │  │   cleaned_data → KPIs   ││   │ anomaly  │
                           │  └─────────────────────────┘│   │ _alerts  │
                           │  ┌─────────────────────────┐│   │          │
                           │  │   Alert Monitor         ││──►│          │
                           │  │   rate > 15% → alert    ││   └──────────┘
                           │  └─────────────────────────┘│        │
                           └─────────────────────────────┘        │
                                                                   │
                           ┌───────────────────────────────────────▼──────┐
                           │           Power BI  DirectQuery               │
                           │   Sales Overview │ Anomaly Detection │ KPIs   │
                           └──────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Streaming** | Apache Kafka + Zookeeper | Real-time event ingestion, fault-tolerant message queue |
| **Storage** | PostgreSQL 15 | Data warehouse — 5 purpose-built tables |
| **ML** | IsolationForest (scikit-learn) | Unsupervised anomaly detection — no labels needed |
| **Pipeline** | Python 3.9+ | Data engineering, feature encoding, orchestration |
| **Cleaning** | PostgreSQL SQL | 8-rule standardization + outlier removal |
| **BI** | Power BI DirectQuery | Live dashboard — queries PostgreSQL on every refresh |
| **Infrastructure** | Docker Compose | Zero-friction local deployment |

---

## Project Structure

```
Real-Time-analysis-dashboard/
│
├── Producer/
│   └── producer.py              # Kafka producer with 8 anomaly injection functions
│
├── consumer/
│   └── consumer.py              # Kafka consumer → PostgreSQL with auto-cleaning
│
├── Cleaning/
│   └── cleaning.sql             # SQL cleaning — city standardization, outlier removal
│
├── ML_models/
│   ├── pipeline.py              # One-command ML pipeline (anomaly + analysis)
│   ├── anomaly_detection.ipynb  # IsolationForest notebook with 7 charts
│   └── analysis.ipynb           # Business insights notebook
│
├── docker-compose.yml           # Kafka + Zookeeper + PostgreSQL
└── README.md
```

---

## Database Schema

```
PostgreSQL (port 5433)
│
├── raw_data           ← all 151,000+ events including corrupt ones
├── cleaned_data       ← 97,983 clean records (65% retention)
├── anomaly_data       ← ML results with anomaly_label + anomaly_reason
├── business_insights  ← 6 KPI metrics computed by pipeline
└── anomaly_alerts     ← triggered when anomaly rate exceeds 15%
```

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.9+
- Power BI Desktop (for dashboard)

### Install Dependencies

```bash
pip install kafka-python pandas psycopg2-binary sqlalchemy \
            scikit-learn matplotlib seaborn numpy
```

### 1. Clean Start

```bash
docker compose down -v    # wipe old Kafka state
docker compose up         # start Kafka + Zookeeper + PostgreSQL
```

Wait until you see:
```
kafka | [KafkaServer id=1] started
```

### 2. Generate Data

```bash
# Terminal 2
python Producer/producer.py
```

### 3. Consume and Store

```bash
# Terminal 3
python consumer/consumer.py
```

Let both run for **2–3 minutes**. Press `Ctrl+C` on the consumer — cleaning runs automatically:

```
[CONSUMER] Closed.
[CLEANING] Starting automatic data cleaning...
[CLEANING] Done — 97,983 rows in cleaned_data ✓
```

### 4. Run ML Pipeline

```bash
python ML_models/pipeline.py
```

Expected output:
```
STEP 1 — ANOMALY DETECTION
[3/5] Training IsolationForest model...
      Normal:    136,506 (90.1%)
      Anomalies: 14,966  (9.9%)
      Anomaly rate 9.9% is within normal range ✓

STEP 2 — BUSINESS ANALYSIS
      Top Category:    Fashion
      Peak Hour:       12:00
      Conversion Rate: 95.99%
      Top City:        Mumbai

PIPELINE COMPLETE — Time: 16.2 seconds
```

### 5. Connect Power BI

```
Get Data → PostgreSQL Database
Server:   127.0.0.1:5433
Database: postgres
Mode:     DirectQuery

Select tables:
  ✓ public.raw_data
  ✓ public.cleaned_data
  ✓ public.anomaly_data
  ✓ public.business_insights
  ✓ public.anomaly_alerts
```

Click **Refresh** — dashboard shows live results.

---

## ML Model — Why IsolationForest

We had zero labeled data — no column marking which rows are anomalies. This rules out supervised approaches. We chose IsolationForest for specific reasons:

```
Requirement                    IsolationForest    DBSCAN    LOF
─────────────────────────────────────────────────────────────
No labeled data needed              ✓               ✓        ✓
Works on 7 features together        ✓               ✗        ✗
Fast on 150,000 rows                ✓               ✗        ✗
Scores new incoming data            ✓               ✗        ✗
Handles mixed anomaly types         ✓               ✗        ✗
```

The model detects anomalies by measuring how few random splits are needed to isolate a data point. Anomalies — being rare and different — get isolated in fewer splits than normal transactions surrounded by similar data.

**Results:**

```
Total rows analyzed  : 151,472
Normal transactions  : 136,506  (90.1%)
Anomalies detected   :  14,966  ( 9.9%)

Top anomaly reasons:
  Missing Payment Method        →  4,821
  Quantity Outlier (Bot)        →  3,204
  Unknown City (Corrupt)        →  2,891
  Price Outlier (Currency)      →  1,847
  Invalid Event Type            →    987
  ML Pattern Anomaly            →  1,216
```

---

## Alert System

The pipeline monitors its own anomaly rate on every execution:

```python
ALERT_THRESHOLD = 15.0  # %

if actual_rate > ALERT_THRESHOLD:
    # writes to anomaly_alerts table
    # Power BI alert card turns red
```

This shifts the system from passive detection to **active monitoring** — the difference between finding bad rows and knowing when the entire pipeline is under stress.

---

## Power BI Dashboard

Three pages connected via DirectQuery to PostgreSQL:

```
Page 1 — Sales Overview
  Slicers: City | Category | Payment Method
  Charts:  Revenue by Category, Hour, City
           Payment Distribution, Customer Funnel

Page 2 — Anomaly Detection
  Cards:   Total Anomalies, Normal Transactions, Alert Rate
  Charts:  Anomaly Reasons, By City, By Category, By Event Type
  Table:   Alert History with timestamps

Page 3 — Business Insights
  Cards:   Top Category, Top City, Conversion Rate,
           Peak Hour, Total Orders, Total Revenue
  Table:   KPI Summary (auto-computed by pipeline.py)
```

Every Power BI refresh queries live PostgreSQL — not a static snapshot.

---

## Future Extensions

- **Cloud deployment** — AWS MSK for Kafka, RDS for PostgreSQL
- **REST API** — FastAPI endpoint for real-time single-transaction scoring
- **Model retraining** — Apache Airflow scheduled daily retraining
- **Explainable AI** — SHAP values showing why each transaction was flagged
- **Multi-node Kafka** — eliminate single point of failure for production scale
- **Stream processing** — Apache Flink for parallel consumer processing

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit: `git commit -m 'Add your feature'`
4. Push: `git push origin feature/your-feature`
5. Open a Pull Request

---

## Authors

**Trishita** — [@trishita-26](https://github.com/trishita-26)
**Diya** — [@diya94](https://github.com/diya94)

---

<div align="center">

*Built with Kafka, curiosity, and too much coffee.*

**⭐ If this project helped you, give it a star!**

</div>
