import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer")

# -----------------------------
# CONFIG
# -----------------------------

CONFIG_PATH = "/opt/airflow/config/config.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)

API_KEY = config["YOUTUBE_API_KEY"]

BASE_URL = "https://www.googleapis.com/youtube/v3/videos"

# IMPORTANT:
# inside Docker containers use kafka:29092
KAFKA_BROKER = "kafka:29092"

TOPIC_NAME = "youtube_trending"

# -----------------------------
# KAFKA PRODUCER
# -----------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    acks="all",
    retries=10,
    linger_ms=50,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------
# FETCH YOUTUBE DATA
# -----------------------------

def fetch_trending_videos():

    params = {
        "part": "snippet,statistics",
        "chart": "mostPopular",
        "regionCode": "IN",
        "maxResults": 10,
        "key": API_KEY
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)

        response.raise_for_status()

        data = response.json()

        return data.get("items", [])

    except requests.RequestException as e:
        logger.error(f"YouTube API Error: {e}")
        return []

# -----------------------------
# SEND TO KAFKA
# -----------------------------

def send_to_kafka():

    items = fetch_trending_videos()

    if not items:
        logger.warning("No videos fetched")
        return

    for item in items:

        try:

            future = producer.send(TOPIC_NAME, item)

            metadata = future.get(timeout=10)

            title = item.get("snippet", {}).get("title", "Unknown")

            logger.info(
                f"Sent | Partition: {metadata.partition} | Offset: {metadata.offset} | Title: {title}"
            )

        except KafkaError as ke:
            logger.error(f"Kafka Error: {ke}")

        except Exception as e:
            logger.error(f"Unexpected Error: {e}")

    producer.flush()

# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":

    logger.info("Starting YouTube Trending Producer...")

    while True:

        send_to_kafka()

        # wait before next API call
        time.sleep(60)