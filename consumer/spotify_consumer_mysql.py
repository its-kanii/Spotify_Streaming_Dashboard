from kafka import KafkaConsumer
import json
import mysql.connector
import time

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    port=3307,          # mapped host port
    user="root",
    password="rootpassword",
    database="spotify"
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS user_streams (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    song VARCHAR(100),
    timestamp DOUBLE
)
""")

# Kafka consumer
consumer = KafkaConsumer(
    "spotify-stream",
    bootstrap_servers=['localhost:9092'],  # connect to Docker Kafka
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening for messages and saving to MySQL...")

# Consume messages
for message in consumer:
    data = message.value
    print("Received:", data)

    # Insert into MySQL
    cursor.execute(
        "INSERT INTO user_streams (user_id, song, timestamp) VALUES (%s, %s, %s)",
        (data["user_id"], data["song"], data["timestamp"])
    )
    conn.commit()

