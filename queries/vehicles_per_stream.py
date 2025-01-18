import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

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

# Print schema for debugging
car_data_df.printSchema()

# Count vehicles per stream
vehicle_count_df = car_data_df.groupBy("direction").agg(
    count("id").alias("vehicle_count")
)

vehicle_count_df.show()
