import json
import cv2
import logging
import numpy as np

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
    def __init__(self, green_line_indices, min_positions_detected: int = 3 , should_visualize: bool = False):
        """
        :param green_line_indices: In what area of the frame should the tracking be implemented
        :param should_visualize: Visualize the object tracking defaults
        :param min_positions_detected: How many times should and object be detected in order to be considered valid
        """
        # self.green_line_indices = green_line_indices
        self.min_positions_detected = min_positions_detected
        self.od = ObjectDetection()
        self.should_visualize = should_visualize
        self.center_points_prev_frame = []
        self.tracking_objects = {}
        self.track_id = 0
        self.vehicle_types = {}
        self.classes = {2: "car", 7: "truck"}
        self.frame_count = 0

        self.SCORE_THRESHOLD = 0.6

    def write_json_file(self, json_path: str):
        tracking_data = {}

        for object_id, positions in self.tracking_objects.items():
            if len(positions) > self.min_positions_detected:
                tracking_data[object_id] = {
                    'positions': positions,
                    'vehicle_type': self.vehicle_types[object_id]
                }
            else:
                continue

        with open(json_path, "w") as outfile:
            json.dump(tracking_data, outfile, indent=4)

    def visualize(self, frame):
        for object_id, positions in self.tracking_objects.items():
            last_position = positions[-1]
            x, y = last_position
            #vehicle_type = self.vehicle_types[object_id]

            w, h = 50, 50
            cv2.rectangle(frame, (x - w // 2, y - h // 2), (x + w // 2, y + h // 2), (0, 255, 0), 2)

            label = f"ID: {object_id}"
            cv2.putText(frame, label, (x - w // 2, y - h // 2 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

        cv2.imshow("Object Tracking", frame)

    def match_objects(
        self, 
        center_points_x: np.ndarray, 
        center_points_y: np.ndarray,
    ): 


        if len(self.tracking_objects) == 0:  
            # Assign new IDs to unmatched detections
            for position_x, position_y in zip(center_points_x, center_points_y):
                self.tracking_objects[self.track_id] = [np.array((position_x, position_y))]
                self.track_id += 1
            return

        updated_tracking_objects = {}
        matched_objects = [False] * len(center_points_x)

        for object_id, positions in self.tracking_objects.items(): 

            # Get the last known position of the object
            # Try to find a best match of the object in the current frame
            last_position = positions[-1]
            best_match = None

            current_centers = np.column_stack((center_points_x, center_points_y))
            distances = np.linalg.norm(current_centers - last_position, axis=1)

            # Find the closest point within the threshold
            min_distance = np.min(distances)
            best_match = np.argmin(distances) if min_distance < 50 else None

            if best_match is not None:
                # Append the position of the tracked object as a new position
                updated_tracking_objects[object_id] = positions + [np.array((center_points_x[best_match], center_points_y[best_match]))]
                matched_objects[best_match] = True

        non_matched_centers = [np.array((center_x, center_y)) for (center_x, center_y), matched in zip(zip(center_points_x, center_points_y), matched_objects) if not matched] 
        # Assign new IDs to unmatched detections
        for arr in non_matched_centers:
            updated_tracking_objects[self.track_id] = [arr]
            self.track_id += 1

        self.tracking_objects = updated_tracking_objects

    def detect_position(
        self, 
        class_ids: np.ndarray, 
        scores: np.ndarray, 
        boxes: np.ndarray
    ) -> tuple:
        """
        Detects the center positions of objects in the current frame.
        :param class_ids: Array of detected class IDs
        :param scores: Array of confidence scores
        :param boxes: Array of bounding box coordinates
        :return: A list of center points and their corresponding class IDs
        """

        center_points_x = np.zeros(class_ids.shape[0], dtype=np.int16)
        center_points_y = np.zeros(class_ids.shape[0], dtype=np.int16)

        for i, box in enumerate(boxes):

            if scores[i] < self.SCORE_THRESHOLD: 
                continue

            x, y, w, h = box  
            center_x = int(x + w / 2)
            center_y = int(y + h / 2)

            center_points_x[i] = center_x
            center_points_y[i] = center_y

            #logging.debug(f"Detected class {class_ids[i]} with score {scores[i]} at center ({center_x}, {center_y})")

        return center_points_x, center_points_y

    def process_video(self, video_path: str):

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
            frame = cv2.GaussianBlur(gray_frame_3channel, (19, 19), 0)

            class_ids, scores, boxes = self.od.detect(frame)

            center_points_x, center_points_y = self.detect_position(class_ids, scores, boxes)

            self.match_objects(
                center_points_x=center_points_x,
                center_points_y=center_points_y,
            )


            if self.should_visualize:
                self.visualize(frame)
                key = cv2.waitKey(1)
                if key == 27:
                    break

        self.cap.release()
        cv2.destroyAllWindows()

    def __call__(self, video_path: str, json_output_path: str):
        self.process_video(video_path)
        self.write_json_file(json_output_path)


if __name__ == "__main__":
    video_path = 'video.mp4'
    json_output_path = 'tracking_data.json'

    tracker = ObjectTracker(2, should_visualize=True)
    tracker(video_path, json_output_path)