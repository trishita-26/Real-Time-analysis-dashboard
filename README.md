# Real-Time Analysis Dashboard

A real-time data streaming and analysis pipeline built with **Apache Kafka**, **Python**, and **Docker**. The system ingests, cleans, and processes data streams using a producer-consumer architecture, enabling live analytics through a continuously updated dashboard.

---

## Architecture Overview

```
Data Source  ──►  Producer  ──►  Kafka Broker  ──►  Consumer  ──►  Dashboard
                                      │
                               (Zookeeper manages
                                broker coordination)
                                      │
                               Cleaning Module
                             (data preprocessing)
```

The pipeline consists of three main stages:

- **Producer** — fetches or generates data and publishes messages to a Kafka topic
- **Kafka Broker** — manages message queuing and distribution (backed by Zookeeper)
- **Consumer** — subscribes to Kafka topics, processes/cleans incoming data, and feeds the dashboard

---

## Project Structure

```
Real-Time-analysis-dashboard/
├── Producer/               # Kafka producer scripts
├── consumer/               # Kafka consumer & dashboard logic
├── Cleaning/               # Data cleaning and preprocessing utilities
└── docker-compose.yml      # Docker setup for Kafka + Zookeeper
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Messaging | Apache Kafka (Confluent Platform 7.4.0) |
| Coordination | Apache Zookeeper |
| Language | Python |
| Containerization | Docker & Docker Compose |

---

## Prerequisites

- [Docker](https://www.docker.com/get-started) and Docker Compose installed
- Python 3.8+
- `pip` package manager

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/trishita-26/Real-Time-analysis-dashboard.git
cd Real-Time-analysis-dashboard
```

### 2. Start Kafka and Zookeeper

Spin up the Kafka broker and Zookeeper using Docker Compose:

```bash
docker-compose up -d
```

This starts:
- **Zookeeper** on port `2181`
- **Kafka broker** on port `9092`

Verify containers are running:

```bash
docker ps
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

> If a `requirements.txt` is not present, install the core dependencies manually:
> ```bash
> pip install confluent-kafka pandas
> ```

### 4. Run the Producer

```bash
python Producer/producer.py
```

The producer connects to the Kafka broker at `localhost:9092` and begins publishing data to the configured topic.

### 5. Run the Consumer / Dashboard

```bash
python consumer/consumer.py
```

The consumer reads messages from the Kafka topic, applies cleaning logic, and updates the dashboard in real time.

---

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| Kafka Bootstrap Server | `127.0.0.1:9092` | Kafka broker address |
| Zookeeper Port | `2181` | Zookeeper client port |
| Kafka Broker ID | `1` | Unique broker identifier |
| Replication Factor | `1` | Topic replication (single-node setup) |

To change the Kafka topic name or broker settings, update the relevant configuration in `Producer/producer.py` and `consumer/consumer.py`.

---

## Stopping the Services

```bash
docker-compose down
```

To also remove volumes (clears all Kafka data):

```bash
docker-compose down -v
```

---

## Troubleshooting

**Kafka connection refused** — Make sure Docker containers are fully started before running the producer/consumer. Wait a few seconds after `docker-compose up -d` before connecting.

**Messages not appearing** — Verify the producer and consumer are using the same topic name.

**Port conflict** — If port `9092` is already in use, update the port mapping in `docker-compose.yml` and the bootstrap server address in the Python scripts.

---

## Contributing

Pull requests are welcome. For significant changes, please open an issue first to discuss what you'd like to change.

---

## License

This project is open source. See [LICENSE](LICENSE) for details.
