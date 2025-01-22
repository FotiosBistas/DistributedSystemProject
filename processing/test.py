import os
from tracking_pkg import Tracker

def process_videos_in_directory(directory_path, should_visualize=False):
    """
    Process all video files in a given directory serially.

    :param directory_path: Path to the directory containing video files.
    :param should_visualize: Flag to indicate if visualization should be enabled.
    """
    tracker = Tracker(should_visualize=should_visualize)

    for file_name in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file_name)


        if os.path.isfile(file_path) and file_name.lower().endswith(('.mp4', '.avi', '.mov', '.mkv')):
            print(f"Starting tracking for: {file_name}")
            tracker(video_path=file_path)
        else:
            print(f"Skipping non-video file: {file_name}")

if __name__ == "__main__":
    temp_video_path = "../downloaded_blobs"
    process_videos_in_directory(temp_video_path, should_visualize=False)
