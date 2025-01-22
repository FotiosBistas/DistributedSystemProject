from datetime import datetime, timedelta

import numpy as np
import logging


def calculate_speed(positions: list[np.ndarray], direction, time_to_frame_ratio = 1/25.0) -> float:
    """
    Calculate the speed of the object based on its positions over time.
    :param direction: inbound or outbound, cause of perspective we need different pixel to meter ratio
    :param time_to_frame_ratio:
    :param positions: List of coordinates representing the tracked object's path.
    :return: Calculated speed in km/h.
    """

    if direction == 'outbound':
        pixel_ratio = 0.18
    else:
        pixel_ratio = 0.09


    # Euclidean distance traveled scaled by the pixel to meter ratio
    numerator = np.linalg.norm(positions[0] - positions[-1], ord = 2) * pixel_ratio

    # Time given the fps and the number of frames that the object was detected
    denominator = (len(positions)) * time_to_frame_ratio

    # Convert them into km/h
    speed = (numerator / denominator) * 3.6

    return float(round(speed, 1))

def calculate_angle(positions: list[np.ndarray]) -> float:
    vector = positions[0] - positions[-1]

    angle = np.arctan2(vector[1], vector[0])
    return angle


def lower_line(x: float):
    return 1/73 * x + 38255 /71

def upper_line(x: float):
    return 3/145 * x + 12033/29

def find_positions_inside_green_line_indices(positions: list[np.ndarray]) -> list[np.ndarray]:
    """
    :param positions: the list with the tracked positions
    :param lower_bound: the y-coordinate's lower bound
    :param upper_bound: the y-coordinate's upper bound'
    :return: list[np.ndarray] with the positions only inside the bounding box
    """
    bounding_box_positions = []


    for position in positions:
        x, y = position
        # lim_low = lower_line(x)
        # lim_up = upper_line(x)

        if 420 <= y <= 550:
            bounding_box_positions.append(position)
        else:
            continue
    return bounding_box_positions



def determine_direction(positions: list[np.ndarray]) -> str:
    """
    Determine the direction of the object based on its movement.
    :param positions: List of coordinates representing the tracked object's path.
    :return: "inbound" or "outbound".
    """
    return "inbound" if positions[0][1] < positions[-1][1] else "outbound"


def log_speed_alert(object_id: int, vehicle_type: str, speed: float):
    """
    Log a speed alert if the object's speed exceeds the threshold.
    :param object_id: The ID of the tracked object.
    :param vehicle_type: The type of the vehicle.
    :param speed: The calculated speed of the object.
    """
    if speed >= 130:
        logging.warning(
            f"Speed Alert: Vehicle {object_id} ({vehicle_type}) detected with speed {speed:.2f} km/h."
        )

def calculate_time(timestamp, frame_count, num_frames):
    """
    :param timestamp: The timestamp of the frame as it was parsed from the chunk's name
    :param frame_count: The number of the frame the object was last detected
    :param num_frames:  For how many frames the object was detected
    :return: The timestamp of the first detected frame
    """

    # We calculate the time of tracking by subtracting numbers of detections from the last frame's number
    # Thus giving us the elapsed time. Then we add it to the chunk's timestamp
    parsed_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    elapsed_time = (frame_count - num_frames) / 25.0
    new_time = parsed_time + timedelta(seconds=elapsed_time)

    return new_time.strftime("%Y-%m-%d %H:%M:%S")



def prepare_tracking_data(object_id, positions, timestamp, frame_count, vehicle_types):
    """
    Prepare tracking data for database entry.
    :param object_id: The ID of the tracked object.
    :param positions: List of coordinates representing the tracked object's path.
    :param vehicle_types: dict with the truck or car of the vehicles.
    :return: A dictionary with tracking data.
    """
    timestamp = calculate_time(timestamp, frame_count, len(positions))
    direction = determine_direction(positions)
    positions = find_positions_inside_green_line_indices(positions)


    if len(positions) < 2:
        return None


    speed = calculate_speed(positions, direction)
    log_speed_alert(object_id, vehicle_types.get(object_id, "unknown"), speed)

    return {
        "vehicle_id": object_id,
        "vehicle_type": vehicle_types.get(object_id, "unknown"),
        "direction": direction,
        "speed": speed,
        "timestamp": timestamp
    }
