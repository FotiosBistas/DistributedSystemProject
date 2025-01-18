import cv2
import logging
import numpy as np

class ObjectDetection:
    def __init__(self, weights_path="./yolo_files/yolov4.weights", cfg_path="./yolo_files/yolov4.cfg"):
        logging.info("Loading Object Detection")
        logging.info("Running opencv dnn with YOLOv4")
        self.nmsThreshold = 0.4
        self.confThreshold = 0.5
        self.image_size = 608

        net = cv2.dnn.readNetFromDarknet(cfg_path, weights_path)

        #try:
        #    # Attempt to use CUDA
        #    net.setPreferableBackend(cv2.dnn.DNN_BACKEND_CUDA)
        #    net.setPreferableTarget(cv2.dnn.DNN_TARGET_CUDA)
        #    logging.info("CUDA backend enabled.")
        #except cv2.error as e:
            # Fallback to CPU
        logging.warning("CUDA initialization failed. Falling back to CPU.")
        net.setPreferableBackend(cv2.dnn.DNN_BACKEND_DEFAULT)
        net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)

        self.model = cv2.dnn_DetectionModel(net)

        self.classes = []
        self.load_class_names()
        self.colors = np.random.uniform(0, 255, size=(80, 3))

        self.model.setInputParams(size=(self.image_size, self.image_size), scale=1/255)

    def load_class_names(self, classes_path="./yolo_files/classes.txt"):

        with open(classes_path, "r") as file_object:
            for class_name in file_object.readlines():
                class_name = class_name.strip()
                self.classes.append(class_name)

        self.colors = np.random.uniform(0, 255, size=(80, 3))
        return self.classes

    def detect(self, frame):
        return self.model.detect(frame, nmsThreshold=self.nmsThreshold, confThreshold=self.confThreshold)

