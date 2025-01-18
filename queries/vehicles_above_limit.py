import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Vehicle Count Per Stream") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

# Database connection parameters
db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
db_properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
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

# Add a column to check if the vehicle exceeded the speed limit
# Speed limits: 90 km/h for cars, 80 km/h for trucks
exceeded_speed_limit_df = car_data_df.withColumn(
    "exceeded_speed_limit",
    when((col("vehicle_type") == "car") & (col("speed") > 90), 1)
    .when((col("vehicle_type") == "truck") & (col("speed") > 80), 1)
    .otherwise(0)
)

# Count the total number of vehicles that exceeded the speed limit
total_exceeded = exceeded_speed_limit_df.filter(col("exceeded_speed_limit") == 1) \
    .agg(count("id").alias("total_exceeded"))

# Show the result
total_exceeded.show()

# Stop the Spark session
spark.stop()
