import json
import logging

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

consumer = KafkaConsumer(
    "car",
    bootstrap_servers="localhost:9092",
)

#TODO this should be filled with the actuals values of the green line
#TODO this should be all the available distances of the green line
#TODO note that due to smaller pixel in the image some green lines will appear smaller
#TODO what happens if its detected after each line pair? rip?
# There are multiple green lines
GREEN_LINE_Y1 = 10
GREEN_LINE_Y2 = 30
DISTANCE_BETWEEN_LINES = GREEN_LINE_Y2 - GREEN_LINE_Y1

vehicle_data = {}

for message in consumer:
    vehicle = message.value

    # Vehicle.value is a bytestream
    vehicle = json.loads(vehicle.decode('utf-8'))
    logging.debug(f"Received vehicle data:\n{vehicle}")

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

        # Reset data for the vehicle to calculate again in the future
        vehicle_data[vehicle_id] = {"y1_crossed_time": None, "y2_crossed_time": None}


