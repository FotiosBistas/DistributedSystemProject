import cv2
import logging
import numpy as np
from vehicle_utils import prepare_tracking_data
import time
from db import DBHandler
from object_detection import ObjectDetection

DEBUG_MODE = True

if DEBUG_MODE:
    logging.basicConfig(
        level=logging.DEBUG,  # Set the logging level to INFO or DEBUG
        format="%(asctime)s - %(levelname)s - %(message)s",  # Customize the format
        datefmt="%Y-%m-%d %H:%M:%S"  # Add a timestamp for better readability
    )
else:
    logging.basicConfig(
        level=logging.INFO,  # Set the logging level to INFO or DEBUG
        format="%(asctime)s - %(levelname)s - %(message)s",  # Customize the format
        datefmt="%Y-%m-%d %H:%M:%S"  # Add a timestamp for better readability
    )


class ObjectTracker:
    def __init__(self, green_line_indices, min_positions_detected: int = 3, should_visualize: bool = False):
        """
        :param green_line_indices: In what area of the frame should the tracking be implemented
        :param should_visualize: Visualize the object tracking defaults
        :param min_positions_detected: How many times should and object be detected in order to be considered valid
        """
        # self.green_line_indices = green_line_indices
        self.od = ObjectDetection()
        self.db_handler = DBHandler()
        self.should_visualize = should_visualize
        self.tracking_objects = {}
        self.vehicle_types = {}
        self.classes = {2: "car", 7: "truck"}
        self.track_id = 0
        self.min_positions_detected = min_positions_detected
        self.objects_to_write = {}
        self.checkpoint_counter = 0
        self.BATCH_SIZE_DB = 16
        self.SCORE_THRESHOLD = 0.5


    def visualize(self, frame):
        """
        Visualizes the object detection and tracking.
        :param frame: draws bounding boxes to the current frame. Used for debugging reasons.
        """
        for object_id, positions in self.tracking_objects.items():
            last_position = positions[-1]
            x, y = last_position
            vehicle_type = self.vehicle_types[object_id]

            w, h = 50, 50
            cv2.rectangle(frame, (x - w // 2, y - h // 2), (x + w // 2, y + h // 2), (0, 255, 0), 2)

            label = f"ID: {object_id} {vehicle_type}"
            cv2.putText(frame, label, (x - w // 2, y - h // 2 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

        cv2.imshow("Object Tracking", frame)

    def match_objects(
            self,
            center_points_x: np.ndarray,
            center_points_y: np.ndarray,
            class_ids: np.ndarray,
    ):
        """
        Tracks objects inbetween frames. The core idea is that identical objects in consecutive frames,
        would have center coordinates that are less than a certain threshold. We track existing object with that reasoning, and
        detect new vehicles. After a certain amount of tracked vehicles, the tracked objects are sent to the postgresDB.

        :param center_points_x: The position of the objects detect in the current frame on the x-axis
        :param center_points_y: The position of the objects detect in the current frame on the y-axis
        :param class_ids: The ids of the yolo classes, currently only car and truck available
        :return:
        """

        if len(self.tracking_objects) == 0:
            # Assign new IDs to unmatched detections
            for i, (position_x, position_y) in enumerate(zip(center_points_x, center_points_y)):
                self.tracking_objects[self.track_id] = [np.array((position_x, position_y))]
                detected_class_id = class_ids[i]
                if detected_class_id in self.classes:
                    self.vehicle_types[self.track_id] = self.classes[detected_class_id]
                else:
                    self.vehicle_types[self.track_id] = "unknown"  # Default for unidentified classes

                self.track_id += 1

            return

        updated_tracking_objects = {}
        matched_objects = [False] * len(center_points_x)

        current_centers = np.column_stack((center_points_x, center_points_y))

        for object_id, positions in self.tracking_objects.items():

            if current_centers.size == 0:
                break

            # Get the last known position of the object
            # Try to find a best match of the object in the current frame
            last_position = positions[-1]
            best_match = None

            distances = np.linalg.norm(current_centers - last_position, axis=1)

            # Find the closest point within the threshold
            min_distance = np.min(distances)
            best_match = np.argmin(distances) if min_distance < 50 else None

            if best_match is not None:
                # Append the position of the tracked object as a new position
                updated_tracking_objects[object_id] = positions + [
                    np.array((center_points_x[best_match], center_points_y[best_match]))]
                matched_objects[best_match] = True

        # Assign new IDs to unmatched detections
        for i, matched in enumerate(matched_objects):
            if not matched:
                updated_tracking_objects[self.track_id] = [np.array((center_points_x[i], center_points_y[i]))]
                # Assign vehicle type based on class_ids
                detected_class_id = class_ids[i]
                if detected_class_id in self.classes:
                    self.vehicle_types[self.track_id] = self.classes[detected_class_id]
                else:
                    self.vehicle_types[self.track_id] = "unknown"  # Default for unidentified classes

                self.track_id = (self.track_id + 1) % 200

        # Find the inactive objects and fill up a dictionary of them.
        # After a certain amount of time send them to be written into the db
        for object_id in self.tracking_objects.keys():
            # If the object is not the updated dictionary, that means that has become inactive.
            # No more new positions are going to be detected, thus it can be written to the DB and be processed for rta
            if object_id not in updated_tracking_objects:
                self.db_handler.prepare_and_add_to_queue(object_id, self.tracking_objects[object_id], self.vehicle_types)

        self.tracking_objects = updated_tracking_objects

    def detect_position(
            self,
            scores: np.ndarray,
            boxes: np.ndarray
    ) -> tuple:
        """
        Detects the center positions of objects in the current frame.
        :param scores: Array of confidence scores
        :param boxes: Array of bounding box coordinates
        :return: A list of center points and their corresponding class IDs
        """

        center_points_x = np.zeros(len(scores), dtype=np.int16)
        center_points_y = np.zeros(len(scores), dtype=np.int16)

        for i, box in enumerate(boxes):

            if scores[i] < self.SCORE_THRESHOLD:
                continue

            x, y, w, h = box
            center_x = x + w / 2
            center_y = y + h / 2

            center_points_x[i] = center_x
            center_points_y[i] = center_y

            # logging.debug(f"Detected class {class_ids[i]} with score {scores[i]} at center ({center_x}, {center_y})")

        return center_points_x, center_points_y

    def process_video(self, video_path: str):

        timer = cv2.TickMeter()
        self.cap = cv2.VideoCapture(video_path)

        while True:
            ret, frame = self.cap.read()
            if not ret:
                break

            # Convert the frame to grayscale
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            # Replicate the grayscale channel into 3 channels
            gray_frame_3channel = cv2.merge([gray_frame, gray_frame, gray_frame])

            # Apply Gaussian blur
            frame = cv2.GaussianBlur(gray_frame_3channel, (15, 15), 0)

            class_ids, scores, boxes = self.od.detect(frame)

            center_points_x, center_points_y = self.detect_position(scores, boxes)

            start = time.time()
            self.match_objects(
                center_points_x=center_points_x,
                center_points_y=center_points_y,
                class_ids=class_ids,
            )
            end = time.time()
            logging.debug(f"Time: {end - start}")

            if self.should_visualize:
                self.visualize(frame)
                key = cv2.waitKey(1)
                if key == 27:
                    break

        self.cap.release()
        cv2.destroyAllWindows()

    def __call__(self, video_path: str, json_output_path: str):
        self.process_video(video_path)



if __name__ == "__main__":
    video_path = '../video.mp4'
    json_output_path = '../tracking_data.json'

    tracker = ObjectTracker(2, should_visualize=True)
    tracker(video_path, json_output_path)