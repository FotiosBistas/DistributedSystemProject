import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Average Speed Per Stream - Merged Days") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------------------------
# 1. Load Data
# ------------------------------------------------------------------------------
db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'default_db')}"
db_properties = {
    "user": os.getenv("POSTGRES_USER", "default_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "default_password"),
    "driver": "org.postgresql.Driver"
}

car_data_df = (
    spark.read
    .format("jdbc")
    .option("url", db_url)
    .option("dbtable", "tracking_data")
    .options(**db_properties)
    .load()
)

# Make sure "timestamp" is a proper TimestampType
car_data_df = car_data_df.withColumn("timestamp", F.to_timestamp("timestamp"))

# ------------------------------------------------------------------------------
# 2. Create Time-of-Day Columns (Ignoring Date)
# ------------------------------------------------------------------------------
car_data_df = (
    car_data_df
    .withColumn("hour_of_day", F.hour("timestamp"))
    .withColumn("minute_of_day", F.minute("timestamp"))
    .withColumn("five_min_bucket", F.floor(F.col("minute_of_day") / 5))  # integer 0..11
)

car_data_df.show(n=1000, truncate=False)

# ------------------------------------------------------------------------------
# 3. Aggregate By (direction, hour_of_day, five_min_bucket)
# ------------------------------------------------------------------------------
avg_speed_df = (
    car_data_df
    .groupBy("direction", "hour_of_day", "five_min_bucket")
    .agg(F.avg("speed").alias("average_speed"))
)

# Order for consistent output
avg_speed_df = avg_speed_df.orderBy("direction", "hour_of_day", "five_min_bucket")

avg_speed_df.show(n=200, truncate=False)

# ------------------------------------------------------------------------------
# 4. Number the 5-Minute Intervals (1st, 2nd, 3rd...) Within Each Direction
# ------------------------------------------------------------------------------
w = Window.partitionBy("direction").orderBy("hour_of_day", "five_min_bucket")
avg_speed_df = avg_speed_df.withColumn("interval_index", F.row_number().over(w))

# Create a time_label: "HH:MM:00" (e.g., 00:05:00 for bucket 1 in hour 0)
avg_speed_df = avg_speed_df.withColumn(
    "time_label",
    F.format_string("%02d:%02d:00", F.col("hour_of_day"), (F.col("five_min_bucket") * 5))
)

# ------------------------------------------------------------------------------
# 5. Ordinal Suffix Helper
# ------------------------------------------------------------------------------
def ordinal_suffix(n: int) -> str:
    """Convert integer n into its ordinal suffix string: 1->1st, 2->2nd, 3->3rd, etc."""
    # Handle special case for 11th, 12th, 13th
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    # Otherwise apply standard rules for last digit
    last_digit = n % 10
    if last_digit == 1:
        return f"{n}st"
    elif last_digit == 2:
        return f"{n}nd"
    elif last_digit == 3:
        return f"{n}rd"
    else:
        return f"{n}th"

# ------------------------------------------------------------------------------
# 6. Write Results to Log File
# ------------------------------------------------------------------------------
log_file_path = "average_speed_per_stream.log"
final_rows = avg_speed_df.collect()

with open(log_file_path, "w") as log_file:
    for row in final_rows:
        direction      = row["direction"]
        interval_index = row["interval_index"]  # integer (1, 2, 3, ...)
        speed          = row["average_speed"]
        t_label        = row["time_label"]      # e.g., "00:00:00"
        
        ordinal_idx_str = ordinal_suffix(interval_index)
        line = f"({direction}, {ordinal_idx_str} 5min, {t_label}, {speed:.2f} km/h)\n"
        
        log_file.write(line)

spark.stop()
