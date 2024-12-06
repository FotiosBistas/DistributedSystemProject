from moviepy.video.io.VideoFileClip import VideoFileClip

def segment_video(video_path, output_dir, chunk_length=120):
    """
    Segments a video into smaller chunks.

    Args:
        video_path (str): Path to the input video file.
        output_dir (str): Directory to save the video chunks.
        chunk_length (int): Length of each chunk in seconds (default is 120 seconds).

    Returns:
        list: List of file paths for the created video chunks.
    """
    from os import makedirs
    from os.path import join

    makedirs(output_dir, exist_ok=True)
    video = VideoFileClip(video_path)
    duration = video.duration  # Total duration in seconds
    chunk_paths = []

    for start_time in range(0, int(duration), chunk_length):
        end_time = min(start_time + chunk_length, duration)
        output_file = join(output_dir, f"chunk_{start_time}-{end_time}.mp4")
        video.subclipped(start_time, end_time).write_videofile(output_file, codec="libx264", audio_codec="aac")
        chunk_paths.append(output_file)

    return chunk_paths


#segment_video("./test.mp4", "./output")