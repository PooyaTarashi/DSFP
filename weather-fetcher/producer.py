import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
from kafka.admin import NewTopic
import os
import logging
import socket

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def wait_for_kafka(host="kafka", port=9092, timeout=60):
    logger.info("⏳ Waiting for Kafka socket...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                logger.info("✅ Kafka socket is open")
                return
        except OSError:
            time.sleep(2)
    raise TimeoutError("Kafka not available after timeout")


logger.info("Bismillah")
logger.info("Bismillah2")
print("Bismillah")
print("Bismillah2")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "weather_raw")

def create_topic_if_not_exists():
    """Create the topic if it doesn't exist"""
    logger.info("Bism2222222222222\n\n\n")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            client_id='weather-producer-admin'
        )
        
        topic_list = admin_client.list_topics()
        if TOPIC not in topic_list:
            logger.info(f"📝 Creating topic: {TOPIC}")
            topic = NewTopic(
                name=TOPIC,
                num_partitions=1,
                replication_factor=1
            )
            admin_client.create_topics([topic])
            logger.info(f"✅ Topic '{TOPIC}' created successfully")
        else:
            logger.info(f"✅ Topic '{TOPIC}' already exists")
        
        admin_client.close()
    except Exception as e:
        logger.info(f"⚠️ Could not check/create topic: {e}")

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version_auto_timeout_ms=5000,
                # Add retry configuration
                retries=5,
                retry_backoff_ms=1000
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
    wait_for_kafka()
    # First create topic if it doesn't exist
    create_topic_if_not_exists()
    
    # Then create producer
    producer = create_producer()
    logger.info("🌦 Weather Fetcher started")

    while True:
        try:
            weather = fetch_weather()
            producer.send(TOPIC, weather)
            producer.flush()
            logger.info(f"📤 Sent: {weather}")

        except Exception as e:
            logger.info(f"❌ Error: {e}")

        time.sleep(3600)