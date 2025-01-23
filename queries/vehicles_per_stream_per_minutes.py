import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp

# 1. Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Vehicle Count by Hour:Minute:Second Summed Across Dates") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

# 2. Database connection parameters
db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'default_db')}"
db_properties = {
    "user": os.getenv("POSTGRES_USER", "default_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "default_password"),
    "driver": "org.postgresql.Driver"
}

# 3. Load data from PostgreSQL
car_data_df = (
    spark.read
    .format("jdbc")
    .option("url", db_url)
    .option("dbtable", "tracking_data")
    .options(**db_properties)
    .load()
)

# Convert 'timestamp' column to Spark TimestampType
car_data_df = car_data_df.withColumn("timestamp", to_timestamp("timestamp"))

# Step A: Group by date + 5-minute window + direction, count vehicles
count_df = (
    car_data_df
    .withColumn("date", F.to_date("timestamp")) 
    .groupBy(
        F.col("date"),
        F.window("timestamp", "5 minutes").alias("time_window"),
        F.col("direction")
    )
    .agg(F.count("id").alias("count"))
)

# (B) Extract hour, minute, second from the window start time
count_with_hms = (
    count_df
    .withColumn("hour",   F.hour(F.col("time_window.start")))
    .withColumn("minute", F.minute(F.col("time_window.start")))
    .withColumn("second", F.second(F.col("time_window.start")))
)

# (C) Re-group by (hour, minute, second, direction) to combine across dates
#     and sum the counts.
agg_hms_df = (
    count_with_hms
    .groupBy("hour", "minute", "second", "direction")
    .agg(F.sum("count").alias("total_count"))
)

# (D) Create a single `hh:mm:ss` string column
agg_hms_df = agg_hms_df.withColumn(
    "timestamp",
    F.format_string("%02d:%02d:%02d", F.col("hour"), F.col("minute"), F.col("second"))
)

# (E) Select final columns and sort
result_df = agg_hms_df.select(
    "timestamp",      # The final column with `HH:mm:ss` format
    "direction",
    "total_count"
).orderBy("timestamp", "direction")

# Show final result
result_df.show(200, truncate=False)

spark.stop()