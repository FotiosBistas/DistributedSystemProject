import os
import subprocess
import shlex
import math
import logging
import tempfile
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError




SEGMENTS_CONTAINER = "input-segments-container"  
BLOB_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="main-video/{name}", connection="AzureWebjobsStorage")
def video_preprocessing(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processing blob "
                 f"Name: {myblob.name}, Blob Size: {myblob.length} bytes")

    tempFilePath = tempfile.gettempdir()

    logging.info(f"Found {tempFilePath} directory")
    # Extract just the file name from the blob name
    blob_filename = os.path.basename(myblob.name)
    temp_video_path = f"{tempFilePath}/{blob_filename}"
    logging.info(f"Temp video path: {temp_video_path}")

    with open(temp_video_path, "wb") as f:
        f.write(myblob.read())

    # Create a temporary directory for video segments
    try: 
        chunk_paths = segment_video(temp_video_path, tempFilePath)
    except Exception as e: 
        logging.error(f"Unexpected error during segmentation: {e}", exc_info=True)
        raise e

    logging.info(f"Segment video created paths: {chunk_paths}")

    # Upload each segment to the "input-segments-container"
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(SEGMENTS_CONTAINER)
    try:
        container_client.create_container()
        logging.info(f"Container '{SEGMENTS_CONTAINER}' created.")
    except ResourceExistsError:
        logging.info(f"Container '{SEGMENTS_CONTAINER}' already exists.")

    for chunk_path in chunk_paths:
        chunk_name = os.path.basename(chunk_path)
        with open(chunk_path, "rb") as data:
            blob_client = container_client.get_blob_client(chunk_name)
            blob_client.upload_blob(data, overwrite=True)
            logging.info(f"Uploaded {chunk_name} to {SEGMENTS_CONTAINER}")

    # Cleanup temporary files and directory
    try: 
        os.remove(temp_video_path)
        for chunk_path in chunk_paths:
            os.remove(chunk_path) 
            logging.info(f"Removed video {chunk_path}")
    except Exception as e:
        logging.error(f"Unexpected error during cleanup: {e}", exc_info=True)
    


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
        subprocess.check_output(split_cmd + split_args)

        chunk_paths.append(output_path)  # Add the output path to the list

    return chunk_paths