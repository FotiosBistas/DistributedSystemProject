from kafka import KafkaProducer
import json
import time
import random

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define 10 specific car IDs
car_ids = [f"car_{i}" for i in range(10)]

# Simulate data in a round-robin fashion
for i in range(100):  # Total messages to send
    # Pick a car ID in round-robin fashion
    car_id = car_ids[i % len(car_ids)]

    vehicle = {
        "vehicle_id": car_id,
        "timestamp": time.time(),
        "position": {"x": random.randint(0, 100), "y": random.randint(0, 100)},
        "type": "car" if i % 2 == 0 else "truck"
    }

    # Send the vehicle data to the Kafka topic
    producer.send("car", vehicle)
    print(f"Sent: {vehicle}")

    time.sleep(1)  # Simulate a 1-second delay between messages
