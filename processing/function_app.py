import logging
import tempfile
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient


from tracking_pkg import Tracker

# Use a global tracker
tracker = Tracker(should_visualize=False)

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="input-segments-container",
                  connection="AzureWebJobsStorage")
def video_processing(myblob: func.InputStream):
    logging.info(f"Object detection triggered for blob: {myblob.name}")

    data = myblob.read()
    blob_size = len(data)

    logging.info(f"Read blob of size {blob_size}")

    temp_file_path = tempfile.gettempdir()
    logging.info(f"Found {temp_file_path} directory")
    # Extract just the file name from the blob name
    blob_filename = os.path.basename(myblob.name)
    temp_video_path = f"{temp_file_path}/{blob_filename}"
    logging.info(f"Temp video path: {temp_video_path}")

    blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AzureWebJobsStorage"))
     # Get the container client
    container_client = blob_service_client.get_container_client("input-segments-container")

    # Get the blob client for the specified blob
    blob_client = container_client.get_blob_client(os.path.basename(myblob.name))

    # Download the blob to a local file
    with open(temp_video_path, "wb") as file:
        file.write(blob_client.download_blob().readall())
    
    tracker(video_path=temp_video_path)

    try: 
        os.remove(temp_video_path)
    except Exception as e:
        logging.error(f"Unexpected error during cleanup: {e}", exc_info=True)

