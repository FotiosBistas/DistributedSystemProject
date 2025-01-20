from tracking_pkg import Tracker

tracker = Tracker(should_visualize=True)

temp_video_path = "./tracking_pkg/traffic-1-of-5-1-of-5.mp4"

tracker(video_path=temp_video_path)