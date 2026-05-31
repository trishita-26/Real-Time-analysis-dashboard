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
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![Confluent](https://img.shields.io/badge/Confluent-7.4.0-CC0000?style=flat-square)](https://confluent.io)
[![License](https://img.shields.io/badge/License-MIT-22C55E?style=flat-square)](LICENSE)

<br/>

*A real-time data streaming pipeline that ingests, cleans, and surfaces live analytics — built on Apache Kafka and Docker.*

</div>

---

## Overview

This project implements an end-to-end **real-time analytics pipeline** using Apache Kafka as the streaming backbone. Raw data is produced to a Kafka topic, cleaned and transformed in-flight, and consumed into a downstream analytics layer — all orchestrated with Docker Compose for zero-friction local deployment.

The architecture follows the classic **Producer → Kafka → Consumer** pattern, with a dedicated data-cleaning stage to ensure only well-formed records make it downstream.

---

## Architecture

```
┌─────────────┐        ┌──────────────────────┐        ┌─────────────────┐
│             │        │                      │        │                 │
│  Producer   │──────▶ │   Apache Kafka       │──────▶ │    Consumer     │
│  (Python)   │  topic │   (Confluent 7.4.0)  │  topic │    (Python)     │
│             │        │                      │        │                 │
└─────────────┘        └──────────────────────┘        └─────────────────┘
       │                        │                              │
       │                 ┌──────┴──────┐                      │
       │                 │  Zookeeper  │                 ┌─────▼──────┐
       ▼                 │  (Coord.)   │                 │  Cleaning  │
  Raw Data In            └─────────────┘                 │  (Python)  │
                                                         └────────────┘
                                                         Processed Data Out
```

| Component | Role |
|-----------|------|
| **Producer** | Reads raw data and streams it to a Kafka topic at `localhost:9092` |
| **Kafka + Zookeeper** | Distributed message broker managing topic partitions and offsets |
| **Cleaning** | Validates, normalises, and filters records before downstream use |
| **Consumer** | Reads processed messages and feeds the analytics dashboard |

---

## Tech Stack

- **Python 3.9+** — Producer, Consumer, and Cleaning scripts
- **Apache Kafka 7.4.0** (Confluent) — Distributed event streaming
- **Apache Zookeeper 7.4.0** (Confluent) — Kafka broker coordination
- **Docker & Docker Compose** — Full local infrastructure in one command

---

## Project Structure

```
Real-Time-analysis-dashboard/
│
├── Producer/               # Kafka producer — ingests and publishes raw data
│   └── producer.py
│
├── consumer/               # Kafka consumer — reads and displays live data
│   └── consumer.py
│
├── Cleaning/               # Data cleaning — validates and normalises records
│   └── cleaning.py
│
└── docker-compose.yml      # Spins up Kafka + Zookeeper with one command
```

---

## Getting Started

### Prerequisites

Make sure the following are installed on your machine:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- Python 3.9 or higher
- `pip` for installing Python dependencies

### 1. Clone the Repository

```bash
git clone https://github.com/trishita-26/Real-Time-analysis-dashboard.git
cd Real-Time-analysis-dashboard
```

### 2. Start Kafka & Zookeeper

Spin up the message broker infrastructure using Docker Compose:

```bash
docker-compose up -d
```

This launches:
- **Zookeeper** on port `2181`
- **Kafka broker** on port `9092`

Verify containers are running:

```bash
docker ps
```

### 3. Install Python Dependencies

```bash
pip install kafka-python pandas
```

> Adjust based on your actual `requirements.txt` or imported libraries.

### 4. Run the Pipeline

Open **three separate terminals** and run each component:

**Terminal 1 — Start the Producer:**
```bash
python Producer/producer.py
```

**Terminal 2 — Run the Cleaning script:**
```bash
python Cleaning/cleaning.py
```

**Terminal 3 — Start the Consumer:**
```bash
python consumer/consumer.py
```

Data will begin flowing through the pipeline in real time.

### 5. Tear Down

```bash
docker-compose down
```

---

## Configuration

The Kafka broker is configured in `docker-compose.yml` with the following defaults:

| Parameter | Value |
|-----------|-------|
| Kafka Bootstrap Server | `localhost:9092` |
| Zookeeper Port | `2181` |
| Broker ID | `1` |
| Confluent Version | `7.4.0` |
| Replication Factor | `1` |

To change the topic name or bootstrap server, update the corresponding variables in `producer.py` and `consumer.py`.

---

## How It Works

1. **Producer** continuously reads from a data source (CSV, API, or simulated stream) and publishes records as JSON messages to a Kafka topic.
2. **Kafka** durably stores and routes messages, decoupling producers from consumers.
3. **Cleaning** subscribes to the raw topic, applies validation and transformation logic, and re-publishes clean records to a processed topic.
4. **Consumer** subscribes to the processed topic and feeds analytics or a dashboard downstream.

This architecture ensures the pipeline is **fault-tolerant**, **scalable**, and **modular** — each component can be updated or scaled independently.

---

## Potential Extensions

- Add **Apache Spark Structured Streaming** for aggregations and windowed analytics
- Integrate **PostgreSQL** or **InfluxDB** as a persistence layer
- Build a live dashboard with **Grafana**, **Streamlit**, or **Superset**
- Deploy to the cloud using **Confluent Cloud** or **AWS MSK**
- Add **schema validation** with Confluent Schema Registry (Avro/JSON Schema)

---

## Contributing

Contributions, issues, and feature requests are welcome!

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m 'Add your feature'`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request

---

## Author

**Trisita** — [@trishita-26](https://github.com/trishita-26)
**Diya** — [@diya94](https://github.com/diya94)

---

<div align="center">

*Built with curiosity and caffeine.*

⭐ If this project helped you, consider giving it a star!

</div>
