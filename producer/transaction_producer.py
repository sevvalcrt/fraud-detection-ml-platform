"""
transaction_producer.py

Kafka'ya sürekli akan, gerçekçi ve içine bilinçli olarak
sahtekarlık (fraud) pattern'leri gömülmüş sentetik işlem verisi üretir.

Faz 1 hedefi: Uçtan uca boru hattının ilk halkası.
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

# Sürekli işlem yapan sahte kullanıcı havuzu (bazıları "kötü niyetli" davranacak)
USER_POOL_SIZE = 200
USERS = [str(uuid.uuid4()) for _ in range(USER_POOL_SIZE)]

# Bazı kullanıcıları "fraud'a yatkın" olarak işaretle (senaryo üretimi için)
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
        "is_fraud_label": False,  # ground truth - sadece değerlendirme için, model bunu görmemeli
    }


def generate_fraud_transaction():
    """
    Bilinen fraud pattern'lerinden birini simüle eder:
    - anormal yüksek tutar
    - "imkansız hız" (kısa sürede farklı ülkeden işlem)
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
    print(f"[producer] {TOPIC_NAME} topic'ine yazmaya başlıyor "
          f"(~{events_per_second} olay/sn, fraud oranı ~%{fraud_ratio*100:.0f})")

    delay = 1.0 / events_per_second

    try:
        while True:
            if random.random() < fraud_ratio:
                txn = generate_fraud_transaction()
            else:
                txn = generate_normal_transaction()

            producer.send(TOPIC_NAME, value=txn)
            print(f"[producer] gönderildi -> user={txn['user_id'][:8]} "
                  f"amount={txn['amount']} fraud={txn['is_fraud_label']}")

            time.sleep(delay)
    except KeyboardInterrupt:
        print("\n[producer] durduruluyor...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()
