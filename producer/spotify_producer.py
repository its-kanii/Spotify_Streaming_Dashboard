from kafka import KafkaProducer
import json
import time
import random

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # connect to Docker Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

songs = ["Song A", "Song B", "Song C", "Song D"]
user_ids = list(range(1, 101))  # 100 users

print("Producing messages to Kafka...")

while True:
    data = {
        "user_id": random.choice(user_ids),
        "song": random.choice(songs),
        "timestamp": time.time()
    }

    producer.send("spotify-stream", value=data)
    print("Sent:", data)
    time.sleep(2)  # send every 2 seconds



