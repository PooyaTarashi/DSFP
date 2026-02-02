import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os


import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("Bismillah")
print("Bismillah")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "weather_raw")


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version_auto_timeout_ms=5000
            )
            logger.info("✅ Connected to Kafka")
            print("✅ Connected to Kafka")
            return producer
        except NoBrokersAvailable:
            logger.info("⏳ Kafka not ready, retrying in 5 seconds...")
            time.sleep(5)


def fetch_weather():
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=35.6892"
        "&longitude=51.3890"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        "&timezone=UTC"
    )

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    idx = -1

    return {
        "timestamp": data["hourly"]["time"][idx],
        "location": "Tehran",
        "temperature": data["hourly"]["temperature_2m"][idx],
        "humidity": data["hourly"]["relative_humidity_2m"][idx],
        "wind_speed": data["hourly"]["wind_speed_10m"][idx],
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }


if __name__ == "__main__":
    producer = create_producer()
    print("🌦 Weather Fetcher started")

    while True:
        try:
            weather = fetch_weather()
            producer.send(TOPIC, weather)
            producer.flush()
            print(f"📤 Sent: {weather}")

        except Exception as e:
            print(f"❌ Error: {e}")

        time.sleep(3600)