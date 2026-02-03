import json
import time
import requests
import csv
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
from kafka.admin import NewTopic
import os
import logging
import socket
import io

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

# City attributes from CSV
CITY_ATTRIBUTES_CSV = """City,Country,Latitude,Longitude
Vancouver,Canada,49.24966,-123.119339
Portland,United States,45.523449,-122.676208
San Francisco,United States,37.774929,-122.419418
Seattle,United States,47.606209,-122.332069
Los Angeles,United States,34.052231,-118.243683
San Diego,United States,32.715328,-117.157257
Las Vegas,United States,36.174969,-115.137222
Phoenix,United States,33.44838,-112.074043
Albuquerque,United States,35.084492,-106.651138
Denver,United States,39.739151,-104.984703
San Antonio,United States,29.42412,-98.493629
Dallas,United States,32.783058,-96.806671
Houston,United States,29.763281,-95.363274
Kansas City,United States,39.099731,-94.578568
Minneapolis,United States,44.979969,-93.26384
Saint Louis,United States,38.62727,-90.197891
Chicago,United States,41.850029,-87.650047
Nashville,United States,36.16589,-86.784439
Indianapolis,United States,39.768379,-86.158043
Atlanta,United States,33.749001,-84.387978
Detroit,United States,42.331429,-83.045753
Jacksonville,United States,30.33218,-81.655647
Charlotte,United States,35.227089,-80.843132
Miami,United States,25.774269,-80.193657
Pittsburgh,United States,40.44062,-79.995888
Toronto,Canada,43.700111,-79.416298
Philadelphia,United States,39.952339,-75.163788
New York,United States,40.714272,-74.005966
Montreal,Canada,45.508839,-73.587807
Boston,United States,42.358429,-71.059769
Beersheba,Israel,31.25181,34.791302
Tel Aviv District,Israel,32.083328,34.799999
Eilat,Israel,29.55805,34.948212
Haifa,Israel,32.815559,34.98917
Nahariyya,Israel,33.005859,35.09409
Jerusalem,Israel,31.769039,35.216331"""

def load_cities():
    """Load cities from CSV string"""
    cities = []
    csv_reader = csv.DictReader(io.StringIO(CITY_ATTRIBUTES_CSV))
    for row in csv_reader:
        cities.append({
            "city": row["City"],
            "country": row["Country"],
            "latitude": float(row["Latitude"]),
            "longitude": float(row["Longitude"])
        })
    logger.info(f"✅ Loaded {len(cities)} cities from CSV")
    return cities

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

def fetch_weather(city_data):
    """Fetch weather for a specific city"""
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={city_data['latitude']}"
        f"&longitude={city_data['longitude']}"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        "&timezone=UTC"
    )

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    idx = -1  # Get the latest forecast (most recent hour)

    return {
        "timestamp": data["hourly"]["time"][idx],
        "city": city_data["city"],
        "country": city_data["country"],
        "latitude": city_data["latitude"],
        "longitude": city_data["longitude"],
        "temperature": data["hourly"]["temperature_2m"][idx],
        "humidity": data["hourly"]["relative_humidity_2m"][idx],
        "wind_speed": data["hourly"]["wind_speed_10m"][idx],
        "ingested_at": datetime.now(timezone.utc).isoformat()
    }

if __name__ == "__main__":
    wait_for_kafka()
    
    # Load cities from CSV
    cities = load_cities()
    
    # First create topic if it doesn't exist
    create_topic_if_not_exists()
    
    # Then create producer
    producer = create_producer()
    logger.info("🌦 Weather Fetcher started")
    
    # Initialize counter for city rotation
    fetch_counter = 0
    current_city_index = 0

    while True:
        try:
            # Every 100 fetches, switch to next city
            if fetch_counter % 100 == 0:
                current_city_index = (fetch_counter // 100) % len(cities)
                city_data = cities[current_city_index]
                logger.info(f"🔄 Switching to city: {city_data['city']}, {city_data['country']} " 
                          f"(lat: {city_data['latitude']}, lon: {city_data['longitude']})")
            
            # Fetch weather for current city
            weather = fetch_weather(city_data)
            producer.send(TOPIC, weather)
            producer.flush()
            
            fetch_counter += 1
            
            logger.info(f"📤 Sent: City={weather['city']}, "
                       f"Temp={weather['temperature']}°C, "
                       f"Humidity={weather['humidity']}%, "
                       f"Wind={weather['wind_speed']}km/h "
                       f"(Fetch #{fetch_counter})")

        except Exception as e:
            logger.info(f"❌ Error: {e}")
            # If there's an error with current city, move to next one
            current_city_index = (current_city_index + 1) % len(cities)
            city_data = cities[current_city_index]

        time.sleep(30)