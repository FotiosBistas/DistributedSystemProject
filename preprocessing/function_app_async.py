import os
import logging
import asyncio
import tempfile
import azure.functions as func
from moviepy.video.io.VideoFileClip import VideoFileClip
from os.path import join
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
    with open(temp_video_path, "wb") as temp_file:
        # Stream the blob content into a file
        logging.info(f"Reading file as chunks")
        for chunk_index, chunk in enumerate(iter(lambda: myblob.read(1024 * 1024), b"")):  # Read in 1MB chunks
            logging.info(f"Successfully read 1MB with index {chunk_index}! Writing to disk.")
            temp_file.write(chunk)

    # Create a temporary directory for video segments
    chunk_paths = asyncio.run(segment_video(temp_video_path, tempFilePath))

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
    

async def process_chunk(video_path, start_time, end_time, output_file, chunk_index):
    """
    Asynchronously processes a single chunk of the video.
    """
    logging.info(f"Processing chunk {chunk_index} asynchronously.")

    def _process_chunk_blocking():
        """Blocking version of the chunk processor (runs in a thread via asyncio.to_thread)."""
        video = VideoFileClip(video_path)
        video.subclipped(start_time, end_time).write_videofile(
            output_file,
            codec="libx264",
            audio=False,
            audio_codec="aac"
        )

    await asyncio.to_thread(_process_chunk_blocking)
    logging.info(f"Processed chunk: {chunk_index}")
    return output_file

async def segment_video(video_path: str, output_dir: str, chunk_length=120):
    """
    Asynchronously segments a video into smaller chunks using asyncio.

    Args:
        video_path (str): Path to the input video file.
        output_dir (str): Directory to save the video chunks.
        chunk_length (int): Length of each chunk in seconds (default is 120 seconds).

    Returns:
        list: List of file paths for the created video chunks.
    """
    video = VideoFileClip(video_path)
    duration = video.duration
    chunk_paths = []
    
    # Ensure chunk_length is an integer
    chunk_length = int(chunk_length)

    if duration < chunk_length:
        logging.warning(f"Video duration ({duration}s) is smaller than chunk length ({chunk_length}s). "
                        f"Creating a single chunk for the entire video.")
        chunk_length = int(duration)

    tasks = []
    for chunk_index, start_time in enumerate(range(0, int(duration), chunk_length)):
        logging.info(f"Scheduling chunk {chunk_index} for processing.")
        end_time = min(start_time + chunk_length, duration)
        output_file = join(output_dir, f"chunk_{start_time}-{end_time}.mp4")
        tasks.append(process_chunk(video_path, start_time, end_time, output_file, chunk_index))
    
    # Run all tasks concurrently and gather results
    chunk_paths = await asyncio.gather(*tasks)

    return chunk_paths

