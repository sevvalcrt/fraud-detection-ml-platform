"""
transaction_producer.py

Continuously generates realistic synthetic transaction data with
intentionally embedded fraud patterns and streams it to Kafka.

Phase 1 goal: the first link of the end-to-end pipeline.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "transactions"

# Pool of simulated users making continuous transactions (some behave maliciously)
USER_POOL_SIZE = 200
USERS = [str(uuid.uuid4()) for _ in range(USER_POOL_SIZE)]

# Mark a subset of users as "fraud-prone" (for scenario generation)
FRAUD_PRONE_USERS = set(random.sample(USERS, k=int(USER_POOL_SIZE * 0.05)))


def random_location():
    return {
        "country": fake.country_code(),
        "lat": float(fake.latitude()),
        "lon": float(fake.longitude()),
    }


def generate_normal_transaction():
    user_id = random.choice(USERS)
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(random.uniform(5, 500), 2),
        "currency": "USD",
        "merchant": fake.company(),
        "location": random_location(),
        "device_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_fraud_label": False,  # ground truth - for evaluation only, the model must not see this
    }


def generate_fraud_transaction():
    """
    Simulates one of two known fraud patterns:
    - abnormally high amount
    - "impossible velocity" (transaction from a different country too soon)
    """
    user_id = random.choice(list(FRAUD_PRONE_USERS))
    pattern = random.choice(["high_amount", "impossible_velocity"])

    txn = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": round(random.uniform(2000, 9000), 2) if pattern == "high_amount" else round(random.uniform(5, 500), 2),
        "currency": "USD",
        "merchant": fake.company(),
        "location": random_location(),
        "device_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_fraud_label": True,
        "fraud_pattern": pattern,
    }
    return txn


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def run(events_per_second: float = 5.0, fraud_ratio: float = 0.03):
    producer = create_producer()
    print(f"[producer] starting to write to topic '{TOPIC_NAME}' "
          f"(~{events_per_second} events/sec, fraud ratio ~{fraud_ratio*100:.0f}%)")

    delay = 1.0 / events_per_second

    try:
        while True:
            if random.random() < fraud_ratio:
                txn = generate_fraud_transaction()
            else:
                txn = generate_normal_transaction()

            producer.send(TOPIC_NAME, value=txn)
            print(f"[producer] sent -> user={txn['user_id'][:8]} "
                  f"amount={txn['amount']} fraud={txn['is_fraud_label']}")

            time.sleep(delay)
    except KeyboardInterrupt:
        print("\n[producer] stopping...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()