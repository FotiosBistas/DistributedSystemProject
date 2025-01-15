import numpy as np
import logging


def calculate_speed(positions: list[np.ndarray], pixel_to_meter_ratio = 1.0, time_to_frame_ratio = 1/25) -> float:
    """
    Calculate the speed of the object based on its positions over time.
    :param positions: List of coordinates representing the tracked object's path.
    :param pixel_to_meter_ratio:
    :param time_per_frame: relative time
    :return: Calculated speed in km/h.
    """
    return float(round(np.linalg.norm(positions[0] - positions[-1] * pixel_to_meter_ratio) / (len(positions) * time_to_frame_ratio), 4))


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


def prepare_tracking_data(object_id, positions, vehicle_types):
    """
    Prepare tracking data for database entry.
    :param object_id: The ID of the tracked object.
    :param positions: List of coordinates representing the tracked object's path.
    :param vehicle_types: dict with the truck or car of the vehicles.
    :return: A dictionary with tracking data.
    """
    direction = determine_direction(positions)
    speed = calculate_speed(positions)
    log_speed_alert(object_id, vehicle_types.get(object_id, "unknown"), speed)

    return {
        "vehicle_type": vehicle_types.get(object_id, "unknown"),
        "direction": direction,
        "speed": speed,
    }
