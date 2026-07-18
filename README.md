# Fraud Detection ML Platform

An end-to-end, scalable machine learning platform for real-time fraud detection on transaction data. Built as a follow-up to the [network security monitor](https://github.com/sevvalcrt/network-security-monitor) project — same core idea (catching anomalous behavior in real time), but this time focused on building a production-grade, scalable ML system rather than a single detection script.

**Status:** Work in progress — Phase 1 of 6 complete.

## Features

- Synthetic real-time transaction data generator (user, amount, location, device, timestamp)
- Two embedded fraud patterns for realistic testing: abnormally high amounts, and "impossible velocity" (transactions from different locations too quickly)
- Kafka-based event streaming pipeline
- Kafka UI for live visual inspection of the data stream
- Redis instance provisioned for the upcoming feature store

*(Planned: stream-based feature engineering, model training/versioning with MLflow, FastAPI serving on Kubernetes, Prometheus/Grafana monitoring, CI/CD — see [Roadmap](#roadmap))*

## How It Works

A Python producer generates synthetic transaction events using the Faker library and streams them continuously to a Kafka topic (`transactions`) at a configurable rate (default ~5 events/sec). About 3% of events are intentionally generated as fraud, using one of two patterns:

- **High amount** — a transaction far above the user's normal range
- **Impossible velocity** — a transaction placed too soon after one from a different location to be physically possible

This is the first stage of a larger pipeline: later stages will consume this stream to compute features in real time, train and version a fraud detection model, and serve predictions through a scalable API — all while remaining observable end-to-end.

## Tech Stack

- Python 3 (data generation, `kafka-python`, `Faker`)
- Apache Kafka + Zookeeper (event streaming)
- Redis (feature store — provisioned, not yet used)
- Docker Compose (local infrastructure)
- Kafka UI (stream inspection)

*(Planned additions: MLflow, FastAPI, Kubernetes, Prometheus, Grafana, GitHub Actions — see [Roadmap](#roadmap))*

## Installation & Usage

```bash
git clone https://github.com/sevvalcrt/fraud-detection-ml-platform.git
cd fraud-detection-ml-platform

# 1. Start Kafka, Zookeeper, Redis, and Kafka UI
cd infra
docker compose up -d

# 2. Install Python dependencies
cd ../producer
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 3. Start streaming synthetic transactions
python transaction_producer.py
```

Watch the live stream in Kafka UI: **http://localhost:8080**

## Testing

Verify events are landing in Kafka:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --from-beginning
```

## Roadmap

- [x] Kafka infrastructure + synthetic transaction producer
- [ ] Stream processing + feature store (Redis)
- [ ] Model training + versioning (MLflow)
- [ ] FastAPI serving + Kubernetes horizontal scaling + load testing
- [ ] Observability (Prometheus/Grafana) + model drift detection
- [ ] CI/CD pipeline + documentation polish

Full weekly breakdown: [`docs/roadmap.md`](docs/roadmap.md)

## License

MIT