import os
import shutil
import logging
import azure.functions as func
from moviepy.video.io.VideoFileClip import VideoFileClip
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from dotenv import dotenv_values

env_values = dotenv_values("../.env", verbose=True)



SEGMENTS_CONTAINER = "input-segments-container"  
BLOB_CONNECTION_STRING = env_values["STORAGE_CONNECTION"]

app = func.FunctionApp()
#bp = func.Blueprint()

#@app.blob_trigger(arg_name="myblob", path="main-video", connection="AzureWebjobsStorage") 
#@bp.blob_trigger(arg_name="myblob", path="main-video", connection="AzureWebjobsStorage")
@app.blob_trigger(arg_name="myblob", path="main-video", connection="AzureWebjobsStorage")
def video_preprocessing(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob "
                 f"Name: {myblob.name}, Blob Size: {myblob.length} bytes")

    os.makedirs("./temp", exist_ok=True)

    logging.info(f"Created temp directory")
    # Extract just the file name from the blob name
    blob_filename = os.path.basename(myblob.name)
    temp_video_path = f"./temp/{blob_filename}"
    logging.info(f"Temp video path: {temp_video_path}")

    with open(temp_video_path, "wb") as f:
        f.write(myblob.read())

    # Create a temporary directory for video segments
    output_dir = "./temp/"
    chunk_paths = segment_video(temp_video_path, output_dir)

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
    os.remove(temp_video_path)
    for chunk_path in chunk_paths:
        os.remove(chunk_path)
    
    shutil.rmtree("./temp", ignore_errors=True)



def segment_video(video_path: str, output_dir: str, chunk_length=120):
    """
    Segments a video into smaller chunks.

    Args:
        video_path (str): Path to the input video file.
        output_dir (str): Directory to save the video chunks.
        chunk_length (int): Length of each chunk in seconds (default is 120 seconds).

    Returns:
        list: List of file paths for the created video chunks.
    """
    import logging
    from os import makedirs
    from os.path import join

    makedirs(output_dir, exist_ok=True)
    video = VideoFileClip(video_path)
    duration = video.duration  # Total duration in seconds

    chunk_paths = []

    # Ensure chunk_length is an integer
    chunk_length = int(chunk_length)

    if duration < chunk_length:
        # Log a warning and create a single chunk
        logging.warning(f"Video duration ({duration}s) is smaller than chunk length ({chunk_length}s). "
                        f"Creating a single chunk for the entire video.")
        chunk_length = int(duration)  # Adjust the chunk length to the video's duration

    for start_time in range(0, int(duration), chunk_length):
        end_time = min(start_time + chunk_length, duration)
        output_file = join(output_dir, f"chunk_{start_time}-{end_time}.mp4")
        video.subclipped(start_time, end_time).write_videofile(output_file, codec="libx264", audio_codec="aac")
        chunk_paths.append(output_file)

    return chunk_paths


