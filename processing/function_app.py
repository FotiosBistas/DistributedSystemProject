import logging
import tempfile
import os
import azure.functions as func

from tracker_experimental import ObjectTracker

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="input-segments-container",
                  connection="AzureWebJobsStorage")
def video_processing(myblob: func.InputStream):
    logging.info(f"Object detection triggered for blob: {myblob.name}")

    temp_file_path = tempfile.gettempdir()
    logging.info(f"Found {temp_file_path} directory")
    # Extract just the file name from the blob name
    blob_filename = os.path.basename(myblob.name)
    temp_video_path = f"{temp_file_path}/{blob_filename}"
    logging.info(f"Temp video path: {temp_video_path}")

    with open(temp_video_path, "wb") as f:
        f.write(myblob.read())
    
    tracker = ObjectTracker(green_line_indices=2, should_visualize=False)

    tracker(video_path=temp_video_path, json_output_path=f"{temp_video_path}/tracking_data.json")

    try: 
        os.remove(temp_video_path)
    except Exception as e:
        logging.error(f"Unexpected error during cleanup: {e}", exc_info=True)


