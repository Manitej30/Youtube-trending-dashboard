import os
import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

CONFIG_PATH = r"C:\youtube_pipeline\config\config.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)

API_KEY = config["YOUTUBE_API_KEY"]
BASE_URL = "https://www.googleapis.com/youtube/v3/videos"

# External listener for Windows host
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "youtube_trending"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    acks="all",
    retries=10,
    linger_ms=50,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

def fetch_trending_videos():
    params = {
        "part": "snippet,statistics",
        "chart": "mostPopular",
        "regionCode": "IN",
        "maxResults": 10,
        "key": API_KEY
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        return r.json().get("items", [])
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def send_to_kafka():
    items = fetch_trending_videos()
    for item in items:
        try:
            future = producer.send(TOPIC_NAME, item)
            record_md = future.get(timeout=10)
            title = item.get("snippet", {}).get("title", "")
            print(f"Sent to Kafka partition {record_md.partition} offset {record_md.offset} | {title}")
        except KafkaError as ke:
            print(f"Kafka error: {str(ke)}")
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
    producer.flush()

if __name__ == "__main__":
    print("Starting YouTube Trending Producer")
    while True:
        send_to_kafka()
        time.sleep(60)
