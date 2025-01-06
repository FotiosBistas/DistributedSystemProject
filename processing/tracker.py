import json
import cv2
import numpy as np
from object_detection import ObjectDetection


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
        self.count = 0

    def detect_position(self, class_ids: list[int], scores: list[float], boxes: list[tuple[int, int, int, int]]) -> tuple[list[tuple[int, int]], list[str]]:
        center_points_current_frame = []
        classes_current_frame = []

        for class_id, score, box in zip(class_ids, scores, boxes):
            if score >= 0.6:
                x, y, w, h = box
                cx = int((2 * x + w) / 2)
                cy = int((2 * y + h) / 2)
                center_points_current_frame.append((cx, cy))

                if class_id not in self.classes: 
                    continue

                classes_current_frame.append(self.classes[class_id])
        return center_points_current_frame, classes_current_frame

    def calculate_distance(self, point_prev: tuple, point_new: tuple) -> float:
        return np.sqrt((point_new[0] - point_prev[0]) ** 2 + (point_new[1] - point_prev[1]) ** 2)

    def track_objects(self, track_objects_copy: dict, center_points_current_frame: list, classes_current_frame: list) -> tuple[dict, list]:
        center_points_current_frame_copy = center_points_current_frame.copy()
        classes_current_frame_copy = classes_current_frame.copy()

        removed_indices = []

        for object_id, existing_point in track_objects_copy.items():
            for i, (new_point, class_id) in enumerate(zip(center_points_current_frame_copy, classes_current_frame_copy)):
                if self.calculate_distance(existing_point[-1], new_point) < 20:
                    track_objects_copy[object_id].append(new_point)
                    removed_indices.append(i)

        classes_current_frame = []
        center_points_current_frame= []
        for i, vehicle_type  in enumerate(classes_current_frame_copy):
            if i not in removed_indices:
                classes_current_frame.append(vehicle_type)
                center_points_current_frame.append(center_points_current_frame_copy[i])

        for i, new_object in enumerate(center_points_current_frame):

            track_objects_copy[self.track_id] = [new_object]
            self.vehicle_types[self.track_id] = classes_current_frame[i]
            self.track_id += 1

        return track_objects_copy, classes_current_frame

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
            vehicle_type = self.vehicle_types[object_id]

            w, h = 50, 50
            cv2.rectangle(frame, (x - w // 2, y - h // 2), (x + w // 2, y + h // 2), (0, 255, 0), 2)

            label = f"ID: {object_id} {vehicle_type}"
            cv2.putText(frame, label, (x - w // 2, y - h // 2 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

        cv2.imshow("Object Tracking", frame)

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

            self.count += 1
            class_ids, scores, boxes = self.od.detect(frame)
            center_points_current_frame, classes_current_frame = self.detect_position(class_ids, scores, boxes)

            if self.count <= 2:
                for i, (point, class_id) in enumerate(zip(center_points_current_frame, class_ids)):
                    for previous_point in self.center_points_prev_frame:
                        if self.calculate_distance(point, previous_point) < 20:
                            self.tracking_objects[self.track_id] = [previous_point, point]
                            self.vehicle_types[self.track_id] = self.classes[class_id]
                            self.track_id += 1
            else:
                tracking_objects_copy = self.tracking_objects.copy()
                self.tracking_objects, center_points_current_frame = self.track_objects(
                    tracking_objects_copy, center_points_current_frame, classes_current_frame)

            self.center_points_prev_frame = center_points_current_frame.copy()

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