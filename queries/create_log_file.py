import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

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

car_data_df = car_data_df.withColumn("timestamp", F.to_timestamp("timestamp"))

# Add a separate date column
car_data_df = car_data_df.withColumn("date", F.to_date("timestamp"))

# Group by date, direction, 5-minute window
avg_speed_df = car_data_df.groupBy(
    F.col("date"),
    F.window("timestamp", "5 minutes").alias("time_window"),
    F.col("direction")
).agg(F.avg("speed").alias("average_speed"))

avg_speed_df.select("time_window").show(truncate=False)

# Drop duplicates if needed
avg_speed_df = avg_speed_df.dropDuplicates(["date","time_window","direction"])

# Window for row numbering: partition by (date, direction) => per day & direction
interval_window = Window.partitionBy("date", "direction") \
                        .orderBy(F.col("time_window.start"))

avg_speed_with_index = avg_speed_df.withColumn(
    "interval_index",
    F.row_number().over(interval_window)
)

# Prepare final output ordering
final_df = avg_speed_with_index.orderBy("date", "interval_index", "direction")

final_df.select("interval_index").show()

# Now write results out
log_file_path = "average_speed_per_stream.log"

def ordinal_suffix(n: int) -> str:
    """Convert integer n into its ordinal suffix string (1->1st, 2->2nd, 3->3rd, etc.)"""
    return (
        f"{n}th"
        if 11 <= n % 100 <= 13
        else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    )

with open(log_file_path, "w") as log_file:
    for row in final_df.collect():
        idx = row["interval_index"]
        dir_ = row["direction"]
        speed = row["average_speed"]
        log_entry = f"({dir_}, {idx}{ordinal_suffix(idx)} 5min, {speed:.2f} km/h)\n"
        log_file.write(log_entry)

spark.stop()
