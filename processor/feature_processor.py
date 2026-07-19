import json
import time
from math import radians, sin, cos, sqrt, atan2

import redis
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "transactions"

REDIS_HOST = "localhost"
REDIS_PORT = 6379

ONE_HOUR_SECONDS = 3600
MAX_PLAUSIBLE_SPEED_KMH = 900


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )


def create_redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def haversine_distance(lat1, lon1, lat2, lon2) -> float:
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def update_transaction_window(r: redis.Redis, user_id: str, timestamp: float) -> int:
    key = f"user:{user_id}:txn_window"
    r.zadd(key, {str(timestamp): timestamp})
    r.zremrangebyscore(key, 0, timestamp - ONE_HOUR_SECONDS)
    return r.zcard(key)


def update_average_amount(r: redis.Redis, user_id: str, amount: float) -> float:
    key = f"user:{user_id}:stats"
    new_total = r.hincrbyfloat(key, "total_amount", amount)
    new_count = r.hincrby(key, "txn_count", 1)
    return float(new_total) / new_count


def check_velocity(r: redis.Redis, user_id: str, lat: float, lon: float, timestamp: float) -> bool:
    key = f"user:{user_id}:last_seen"
    previous = r.hgetall(key)

    flagged = False

    if previous:
        prev_lat = float(previous["lat"])
        prev_lon = float(previous["lon"])
        prev_timestamp = float(previous["timestamp"])

        time_diff_hours = (timestamp - prev_timestamp) / 3600
        distance_km = haversine_distance(lat, lon, prev_lat, prev_lon)

        if time_diff_hours > 0:
            implied_speed_kmh = distance_km / time_diff_hours
            if implied_speed_kmh > MAX_PLAUSIBLE_SPEED_KMH:
                flagged = True

    r.hset(key, mapping={"lat": lat, "lon": lon, "timestamp": timestamp})
    return flagged


def store_features(r: redis.Redis, user_id: str, features: dict):
    key = f"user:{user_id}:features"
    r.hset(key, mapping=features)


def process_transaction(r: redis.Redis, txn: dict):
    user_id = txn["user_id"]
    amount = txn["amount"]
    lat = txn["location"]["lat"]
    lon = txn["location"]["lon"]
    timestamp = time.time()

    txn_count_1h = update_transaction_window(r, user_id, timestamp)
    avg_amount = update_average_amount(r, user_id, amount)
    velocity_flag = check_velocity(r, user_id, lat, lon, timestamp)

    features = {
        "txn_count_1h": txn_count_1h,
        "avg_amount": round(avg_amount, 2),
        "velocity_flag": int(velocity_flag),
        "last_amount": amount,
    }
    store_features(r, user_id, features)

    print(f"[processor] user={user_id[:8]} txn_count_1h={txn_count_1h} "
          f"avg_amount={avg_amount:.2f} velocity_flag={velocity_flag}")


def run():
    consumer = create_consumer()
    r = create_redis_client()
    print(f"[processor] listening to topic '{TOPIC_NAME}'...")

    try:
        for message in consumer:
            txn = message.value
            process_transaction(r, txn)
    except KeyboardInterrupt:
        print("\n[processor] stopping...")


if __name__ == "__main__":
    run()