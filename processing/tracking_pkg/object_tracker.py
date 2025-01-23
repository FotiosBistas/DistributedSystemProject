from datetime import date, datetime

import cv2
import logging
import numpy as np
import re

from .db import DBHandler
from .object_detection import ObjectDetection
from .vehicle_utils import determine_direction


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
    def __init__(
        self, 
        min_positions_detected: int = 3, 
        should_visualize: bool = False,
        log_to_database: bool = True,
    ):
        """
        :param green_line_indices: In what area of the frame should the tracking be implemented
        :param should_visualize: Visualize the object tracking defaults
        :param min_positions_detected: How many times should and object be detected in order to be considered valid
        """
        # self.green_line_indices = green_line_indices
        self.od = ObjectDetection()
        # start a deamon thread in the background
        if log_to_database:
            self.db_handler = DBHandler()
        self.log_to_database = log_to_database
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
        self.timestamp = None
        self.frame_count = 0


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

            label = f"ID: {object_id} {vehicle_type} {determine_direction(positions=positions)}"
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

                self.track_id = (self.track_id + 1) % 32767

        # Don't log to database and just return
        if not self.log_to_database:
            self.tracking_objects = updated_tracking_objects
            return

        # Find the inactive objects and fill up a dictionary of them.
        # After a certain amount of time send them to be written into the db
        for object_id in self.tracking_objects.keys():
            # If the object is not the updated dictionary, that means that has become inactive.
            # No more new positions are going to be detected, thus it can be written to the DB and be processed for rta
            if object_id not in updated_tracking_objects:
                self.db_handler.prepare_and_add_to_queue(object_id, self.tracking_objects[object_id], self.vehicle_types, self.timestamp, self.frame_count)

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

    def _timestamp(self, video_path: str):
        """
        Parses the chunk's name and creates a time signature. Every chunk has
        traffic-1-of-5-1-of-5.mp4 format.
        First number means the # of 2 minutes, in the 8.20 minutes chunks
        Third number means the # of segments of the whole audio
        Azure Blob did not let us to upload the whole video, thus we needed to improvise.

        :param video_path: the path to the video
        :return: the timestamp for the given chunk of the video
        """

        # Parse the numbers from the string
        numbers = re.search(r'(\d+)-of-(\d+)-(\d+)-of-(\d+)', video_path)
        if numbers:
            part_1, total_1, part_2, total_2 = map(int, numbers.groups())
            if part_1 != 1:
                # Calculate the start minute and start second
                start_minute = (part_1 - 1) * 8 + (part_2 - 1) * 2
                start_second = 20 * (part_1 - 1)
                if start_second >= 60:
                    start_second -= 60
                    start_minute += 1


            else:
                start_minute = (part_2 - 1) * 2
                start_second = 0

            timestamp = datetime.combine(date.today(), datetime.min.time()).replace(
                minute=int(start_minute), second=int(start_second)
            )

            formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            self.timestamp = formatted_timestamp

    def process_video(self, video_path: str):
        """
        Processes the video, applies preprocessing, and performs object detection.
        """
        try:
            self.cap = cv2.VideoCapture(video_path)
            if not self.cap.isOpened():
                logging.error(f"Failed to open video file: {video_path}")
                return

            while True:
                ret, frame = self.cap.read()
                if not ret:
                    logging.info("End of video or failed to read frame.")
                    break

                if frame.size == 0:
                    logging.warning("Empty or invalid frame detected. Skipping.")
                    continue

                self.frame_count += 1
                try:
                    # Convert the frame to grayscale
                    gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

                    # Replicate the grayscale channel into 3 channels (if required)
                    gray_frame_3channel = cv2.merge([gray_frame, gray_frame, gray_frame])

                    # Apply Gaussian blur
                    processed_frame = cv2.GaussianBlur(gray_frame_3channel, (15, 15), 0)

                    # Perform object detection
                    class_ids, scores, boxes = self.od.detect(processed_frame)

                    # Detect object positions
                    center_points_x, center_points_y = self.detect_position(scores, boxes)

                    # Match objects
                    self.match_objects(
                        center_points_x=center_points_x,
                        center_points_y=center_points_y,
                        class_ids=class_ids,
                    )

                    # Visualize if required
                    if self.should_visualize:
                        self.visualize(frame)
                        key = cv2.waitKey(1)
                        if key == 27:  # ESC key to exit visualization
                            break

                except Exception as e:
                    logging.error(f"Error processing frame: {e}", exc_info=True)
                    continue

        except Exception as e:
            logging.error(f"Critical error processing video: {e}", exc_info=True)

        finally:
            # Cleanup resources
            if self.cap:
                self.cap.release()
            if self.should_visualize:
                cv2.destroyAllWindows()


    def __call__(self, video_path: str):
        self._timestamp(video_path)
        self.process_video(video_path)




if __name__ == "__main__":
    video_path = '../../downloaded_blobs/traffic-2-of-5-2-of-5.mp4'

    tracker = ObjectTracker(should_visualize=False, log_to_database=False)
    tracker(video_path)