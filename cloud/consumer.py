#!/usr/bin/env python
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Copyright 2016 Confluent Inc.
# Licensed under the MIT License.
# Licensed under the Apache License, Version 2.0
#
# Original Confluent sample modified for use with Azure Event Hubs for Apache Kafka Ecosystems

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import sys
import getopt
import json
import logging
from pprint import pformat
from dotenv import dotenv_values

env_values = dotenv_values("./.env", verbose=True)

GREEN_LINE_Y1 = 10
GREEN_LINE_Y2 = 30
DISTANCE_BETWEEN_LINES = GREEN_LINE_Y2 - GREEN_LINE_Y1

vehicle_data = {}

def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s [options..] <consumer-group> <topic1> <topic2> ..\n' % program_name)
    options = '''
 Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
'''
    sys.stderr.write(options)
    sys.exit(1)


if __name__ == '__main__':
    optlist, argv = getopt.getopt(sys.argv[1:], 'T:')
    if len(argv) < 2:
        print_usage_and_exit(sys.argv[0])

    group = argv[0]
    topics = argv[1:]
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        'bootstrap.servers': 'cartopics.servicebus.windows.net:9093', #update
        'security.protocol': 'SASL_SSL',
        'ssl.ca.location': '/usr/lib/ssl/certs/ca-certificates.crt',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': env_values["EVENT_HUB_CARS_CONSUME"],   
        'group.id': group,
        'client.id': 'car-consumer',
        'request.timeout.ms': 60000,
        'session.timeout.ms': 60000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }

    # Check to see if -T option exists
    for opt in optlist:
        if opt[0] != '-T':
            continue
        try:
            intval = int(opt[1])
        except ValueError:
            sys.stderr.write("Invalid option value for -T: %s\n" % opt[1])
            sys.exit(1)

        if intval <= 0:
            sys.stderr.write("-T option value needs to be larger than zero: %s\n" % opt[1])
            sys.exit(1)

        conf['stats_cb'] = stats_cb
        conf['statistics.interval.ms'] = int(opt[1])

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(conf, logger=logger)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    message = None
    try:
        while True:
            msg = c.poll(timeout=100.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                else:
                    # Error
                    raise KafkaException(msg.error())
            else:
                # Proper message
                vehicle = json.loads(msg.value().decode('utf-8'))
                print(vehicle)

                vehicle_id = vehicle["vehicle_id"]
                timestamp = vehicle["timestamp"]
                y_position = vehicle["position"]["y"]

                if vehicle_id not in vehicle_data:
                    #TODO this should be all the available instances of the green line (there are multiple green lines) 
                    # There are multiple green lines
                    vehicle_data[vehicle_id] = {"y1_timestamp": None, "y2_timestamp": None}

                # Check if it hasn't crossed the Y1 green line yet
                #TODO what happens if its detected after Y2?
                if y_position >= GREEN_LINE_Y1 and y_position < GREEN_LINE_Y2: 
                    vehicle_data[vehicle_id]["y1_timestamp"] = timestamp
                    logging.debug(f"{vehicle_id} has passed the first line")

                # Check if it hasn't crossed the Y2 green line yet
                if y_position >= GREEN_LINE_Y2 and vehicle_data[vehicle_id]["y1_timestamp"] is not None:
                    vehicle_data[vehicle_id]["y2_timestamp"] = timestamp
                    logging.debug(f"{vehicle_id} has passed the second line")

                    time_taken = vehicle_data[vehicle_id]["y2_timestamp"] - vehicle_data[vehicle_id]["y1_timestamp"]
                    speed = DISTANCE_BETWEEN_LINES / time_taken

                    logging.info(f"{vehicle_id} speed: {speed:.2f} m/s")

                    if speed > 130:
                        logging.info(f"ALERT: {vehicle['vehicle_id']} moving at {speed} km/h")

                    vehicle["speed"] = speed

                # Producer configuration
                # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                # See https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka#prerequisites for SSL issues
                conf = {
                    'bootstrap.servers': 'cartopics.servicebus.windows.net:9093', #replace
                    'security.protocol': 'SASL_SSL',
                    'ssl.ca.location': '/usr/lib/ssl/certs/ca-certificates.crt',
                    'sasl.mechanism': 'PLAIN',
                    'sasl.username': '$ConnectionString',
                    'sasl.password': env_values["EVENT_HUB_PROCESSED_CARS_PRODUCE"],
                    'client.id': 'processed-car-producer'
                }

                # Create Producer instance
                p = Producer(**conf)

                def delivery_callback(err, msg):
                    if err:
                        sys.stderr.write('%% Message failed delivery: %s\n' % err)
                    else:
                        sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' % (msg.topic(), msg.partition(), msg.offset()))

                # Send the vehicle data to the Kafka topic
                vehicle = json.dumps(obj=vehicle, indent=2).encode("utf-8")
                try:
                    p.produce("processed_cars", vehicle, callback=delivery_callback)
                except BufferError as e:
                    sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
                p.poll(0)
                p.flush()


    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()