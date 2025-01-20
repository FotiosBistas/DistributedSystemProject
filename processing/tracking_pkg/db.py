import os
import psycopg2
import threading
import queue
import logging
import numpy as np

from .vehicle_utils import prepare_tracking_data

db_params = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

class DBHandler:
    def __init__(self, batch_size: int = 16):
        """
        Initialize the database handler.
        :param batch_size: Number of entries to process in each batch.
        """
        self.db_params = db_params
        self.batch_size = batch_size
        self.queue = queue.Queue()
        self.thread_running = True
        self._ensure_table_exists()  # Ensure table exists before starting
        self.db_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.db_thread.start()

    def _connect(self):
        """
        Establish a connection to the database.
        :return: A psycopg2 connection object.
        """
        return psycopg2.connect(**self.db_params)

    def _ensure_table_exists(self):
        """
        Ensure the tracking_data table exists in the database.
        If not, create it.
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tracking_data (
            id SMALLSERIAL,
            vehicle_type VARCHAR(50),
            direction VARCHAR(50),
            speed FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id, timestamp)
        );
        """
        connection = None
        cursor = None
        try:
            connection = self._connect()
            cursor = connection.cursor()
            cursor.execute(create_table_query)
            connection.commit()
            logging.info("Ensured tracking_data table exists.")
        except Exception as error:
            logging.error(f"Error ensuring table exists: {error}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()


    def _write_batch(self, batch: list):
        """
        Write a batch of tracking data to the database.
        :param batch: List of tracking data dictionaries.
        """
        connection = None
        cursor = None
        try:
            connection = self._connect()
            cursor = connection.cursor()

            insert_query = "INSERT INTO tracking_data (vehicle_type, direction, speed) VALUES (%s, %s, %s);"
            for data in batch:
                cursor.execute(insert_query, (data['vehicle_type'], data['direction'], data['speed']))

            connection.commit()
            logging.info("Batch written to DB successfully.")

        except Exception as error:
            logging.error(f"Error writing batch to database: {error}")
            if connection:
                connection.rollback()

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _process_queue(self):
        """
        Background thread for processing the database queue.
        """
        logging.info("DBHandler background thread started.")
        while self.thread_running or not self.queue.empty():
            batch = []
            while not self.queue.empty() and len(batch) < self.batch_size:
                batch.append(self.queue.get())

            if batch:
                self._write_batch(batch)

            threading.Event().wait(0.1)  # Reduce CPU usage if queue is empty

        logging.info("DBHandler background thread stopped.")

    def prepare_and_add_to_queue(self, object_id: dict, positions: list[np.ndarray], vehicle_types: dict, min_positions_detected=3):
        """
        Prepare tracking data for a specific object and add it to the queue.
        :param object_id: ID of the tracked object.
        :param positions: List of tracked positions for the object.
        :param vehicle_types: Dictionary mapping object IDs to vehicle types.
        """
        if len(positions) > min_positions_detected:
            data = prepare_tracking_data(object_id, positions, vehicle_types=vehicle_types)
            if data is not None:
                self.queue.put(data)

    def stop(self):
        """
        Stop the background thread and process remaining items in the queue.
        """
        self.thread_running = False
        self.db_thread.join()
