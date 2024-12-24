import json
import time
import random
import logging
import numpy as np
from kafka import KafkaProducer
from datetime import datetime, timedelta
from dotenv import dotenv_values

logging.getLogger("kafka").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("producer_logs.log"),
        logging.StreamHandler()
    ]
)

env_values = dotenv_values("./.env", verbose=True)

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define 10 specific car IDs
car_ids = [f"car_{i}" for i in range(10)]

# Start timestamp for simulation
start_time = datetime.now()

# Simulate data in a round-robin fashion
for i in range(10000):  # Total messages to send
    # Pick a car ID in round-robin fashion
    car_id = car_ids[i % len(car_ids)]

    # Simulate realistic timestamps
    message_time = start_time + timedelta(seconds=i * random.randint(1, 3))  # Random interval of 1-3 seconds
    timestamp = message_time.timestamp()  # Convert to UNIX timestamp

    vehicle = {
        "vehicle_id": car_id,
        "timestamp": timestamp,
        "position": {
            "x": np.random.randint(low=0, high=100),  
            "y": np.random.randint(low=0, high=100)
        },
        "type": np.random.choice(["car", "truck"]),
        "lane": np.random.choice(["inbound", "outbound"])
    }

    # Send the vehicle data to the Kafka topic
    producer.send("cars", vehicle)

    logging.info(f"Sent: {vehicle}")

    time.sleep(1)  # Simulate a 1-second delay between messages
