#!/usr/bin/env python
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Copyright 2016 Confluent Inc.
# Licensed under the MIT License.
# Licensed under the Apache License, Version 2.0
#
# Original Confluent sample modified for use with Azure Event Hubs for Apache Kafka Ecosystems

import sys
import json
import numpy as np

from datetime import datetime, timedelta
from confluent_kafka import Producer
from dotenv import dotenv_values

env_values = dotenv_values("./.env", verbose=True)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.stderr.write('Usage: %s <topic>\n' % sys.argv[0])
        sys.exit(1)
    topic = sys.argv[1]

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    # See https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka#prerequisites for SSL issues
    conf = {
        'bootstrap.servers': 'cartopics.servicebus.windows.net:9093', #replace
        'security.protocol': 'SASL_SSL',
        'ssl.ca.location': '/usr/lib/ssl/certs/ca-certificates.crt',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': env_values["EVENT_HUB_CARS_PRODUCE"],
        'client.id': 'python-example-producer'
    }

    # Create Producer instance
    p = Producer(**conf)


    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' % (msg.topic(), msg.partition(), msg.offset()))


    # Define 10 specific car IDs
    car_ids = [f"car_{i}" for i in range(10)]

    # Start timestamp for simulation
    start_time = datetime.now()

    # Simulate data in a round-robin fashion
    for i in range(10000):  # Total messages to send
        # Pick a car ID in round-robin fashion
        car_id = car_ids[i % len(car_ids)]

        # Simulate realistic timestamps
        message_time = start_time + timedelta(seconds=i * np.random.randint(low=1, high=3))  # Random interval of 1-3 seconds
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
        vehicle = json.dumps(obj=vehicle, indent=2).encode("utf-8")
        try:
            p.produce(topic, vehicle, callback=delivery_callback)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
        p.poll(0)


    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()