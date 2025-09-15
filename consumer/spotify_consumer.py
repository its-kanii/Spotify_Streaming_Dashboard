from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "spotify-stream",
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening for messages...")

for message in consumer:
    print("Received:", message.value)
