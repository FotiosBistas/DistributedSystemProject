import requests
import shlex
import subprocess
import tempfile
import logging
import os
import math

code = os.getenv("FUNCTION_CODE")
url = f"https://video-preprocessing.azurewebsites.net/api/video-preprocessing?code={code}"

# Path to the file you want to upload
video_path = "./traffic.mp4"

# code taken from https://github.com/c0decracker/video-splitter/blob/master/ffmpeg-split.py
def segment_video(
    video_path: str, 
    output_dir: str, 
    chunk_length=120,
    vcodec="copy",
    acodec="copy",
    extra="",
) -> list[str]:
    """Segments a video into smaller chunks.

    Args:
        video_path (str): Path to the input video file.
        output_dir (str): Directory to save the video chunks.
        chunk_length (int): Length of each chunk in seconds (default is 120 seconds).

    Returns:
        list: List of file paths for the created video chunks.
    """
    from os.path import join

    def _get_video_length():
        output = subprocess.check_output(
            ("ffprobe", "-v", "error", "-show_entries", "format=duration", "-of",
            "default=noprint_wrappers=1:nokey=1", video_path)
        ).strip()
        video_length = int(float(output))
        logging.debug(f"Video length in seconds: {video_length}")

        return video_length

    video_length = _get_video_length()

    chunk_count = int(math.ceil(video_length / float(chunk_length)))
    chunk_paths = []
    chunk_length = int(chunk_length)  # Ensure chunk_length is an integer

    if chunk_count == 1:
        logging.warning(f"Video duration ({video_length}s) is smaller than chunk length ({chunk_length}s). "
                        f"Creating a single chunk for the entire video.")
        chunk_length = int(video_length)  # Adjust the chunk length to the video's duration

    logging.info("Starting to segment video")
    print("Starting to segment video")
    split_cmd = ["ffmpeg", "-y", "-i", video_path, "-vcodec", vcodec, "-acodec", acodec] + shlex.split(extra)

    try:
        filebase = os.path.basename(video_path).rsplit(".", 1)[0]
        fileext = video_path.rsplit(".", 1)[-1]
    except IndexError as e:
        raise IndexError("No . in filename. Error: " + str(e))

    for n in range(chunk_count):
        split_args = []
        split_start = n * chunk_length

        output_path = join(output_dir, f"{filebase}-{n+1}-of-{chunk_count}.{fileext}") if output_dir else f"{filebase}-{n+1}-of-{chunk_count}.{fileext}"

        split_args += ["-ss", str(split_start), "-t", str(chunk_length), output_path]
        logging.info("About to run: " + " ".join(split_cmd + split_args))
        print("About to run: " + " ".join(split_cmd + split_args))
        subprocess.check_output(split_cmd + split_args)

        chunk_paths.append(output_path)  # Add the output path to the list

    return chunk_paths

temp_file_path = tempfile.gettempdir()

chunk_paths = segment_video(video_path=video_path, output_dir=temp_file_path, chunk_length=500)

for idx, chunk_path in enumerate(chunk_paths, start=1):
    logging.info(f"Uploading chunk: {chunk_path}")
    print(f"Uploading chunk: {chunk_path}")
    try:
        # Open the chunk file in binary mode
        with open(chunk_path, "rb") as chunk_file:
            # Prepare the files and data payload
            files = {
                "file": (os.path.basename(chunk_path), chunk_file, "video/mp4"),
            }
            data = {
                "chunk_number": idx,
                "total_chunks": len(chunk_paths),
                "original_filename": os.path.basename(chunk_path).rsplit("-", 1)[0],  # Extract original filename
            }

            # Make the POST request
            response = requests.post(url, files=files, data=data)

            # Log the response
            logging.info(f"Uploaded chunk {idx}/{len(chunk_paths)}: {response.status_code} - {response.text}")
            print(f"Uploaded chunk {idx}/{len(chunk_paths)}: {response.status_code} - {response.text}")

    except Exception as e:
        logging.error(f"Failed to upload chunk {idx}: {e}")
        print(f"Failed to upload chunk {idx}: {e}")