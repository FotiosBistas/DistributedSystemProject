from tracking_pkg import Tracker

tracker = Tracker(should_visualize=False)

temp_video_path = "./tracking_pkg/chunk_120-240.mp4"

tracker(video_path=temp_video_path)