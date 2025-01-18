import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, col, lit

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Average Speed Per Stream Per 5-Minutes") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

# Database connection parameters
db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'default_db')}"
db_properties = {
    "user": os.getenv("POSTGRES_USER", "default_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "default_password"),
    "driver": "org.postgresql.Driver"
}

# Load data from PostgreSQL
car_data_df = spark.read \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "tracking_data") \
    .options(**db_properties) \
    .load()

# Print schema to verify the structure
car_data_df.printSchema()

# Ensure the timestamp column is in Spark's TimestampType
from pyspark.sql.functions import to_timestamp
car_data_df = car_data_df.withColumn("timestamp", to_timestamp("timestamp"))

# Group by 5-minute window and direction, calculate average speed
avg_speed_df = car_data_df.groupBy(
    window(car_data_df["timestamp"], "5 minutes"),
    car_data_df["direction"]
).agg(
    avg("speed").alias("average_speed")
)

# Add Nth 5-minute interval numbering for continuous indexing
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Create a row number per direction to represent the 5-minute interval
interval_window = Window.partitionBy("direction").orderBy("window.start")
avg_speed_with_index = avg_speed_df.withColumn("interval_index", row_number().over(interval_window))

# Select and format the result
formatted_result_df = avg_speed_with_index.select(
    col("direction"),
    col("interval_index").alias("5min_interval"),
    col("average_speed")
).orderBy("5min_interval", "direction")

# Write results to a log file in the desired format
log_file_path = "average_speed_per_stream.log"
with open(log_file_path, "w") as log_file:
    for row in formatted_result_df.collect():
        # Format Nth interval with ordinal suffix (1st, 2nd, 3rd, etc.)
        interval_suffix = f"{row['5min_interval']}{'th' if 11 <= row['5min_interval'] % 100 <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(row['5min_interval'] % 10, 'th')}"
        log_entry = f"({row['direction']}, {interval_suffix} 5min, {row['average_speed']:.2f} km/h)\n"
        log_file.write(log_entry)


# Stop the Spark session
spark.stop()
