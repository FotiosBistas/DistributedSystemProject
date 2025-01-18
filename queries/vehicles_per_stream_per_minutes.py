import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count
from pyspark.sql.functions import to_timestamp

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Vehicle Count Per 5-Second Interval") \
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
car_data_df = car_data_df.withColumn("timestamp", to_timestamp("timestamp"))

# Group by 5-second window and direction
vehicle_count_df = car_data_df.groupBy(
    window(car_data_df["timestamp"], "5 minutes"),
    car_data_df["direction"]
).agg(
    count("id").alias("vehicle_count")
)

# Select and format the result
result_df = vehicle_count_df.select(
    vehicle_count_df["window.start"].alias("start_time"),
    vehicle_count_df["window.end"].alias("end_time"),
    vehicle_count_df["direction"],
    vehicle_count_df["vehicle_count"]
).orderBy("start_time", "direction")

# Show the result
result_df.show(truncate=False)

# Stop the Spark session
spark.stop()
