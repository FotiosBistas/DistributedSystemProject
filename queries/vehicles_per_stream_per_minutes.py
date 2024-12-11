from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, window
from dotenv import dotenv_values

# Load environment variables
env_values = dotenv_values(dotenv_path="../.env")

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Vehicles Per Stream and 5 Minutes") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

# JDBC connection properties
jdbc_url = f"jdbc:postgresql://distributed.postgres.database.azure.com:5432/postgres?user={env_values['PGUSER']}&password={env_values['PGPASSWORD']}&sslmode=require"
db_properties = {
    "user": env_values["PGUSER"],
    "password": env_values["PGPASSWORD"],
    "driver": "org.postgresql.Driver",
    "sslmode": "require"
}

# Load data from PostgreSQL
car_data_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "car_data") \
    .options(**db_properties) \
    .load()

# Convert timestamps to Spark's TimestampType
# No need to divide by 1000 since the timestamp is in seconds
car_data_df = car_data_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Group data by stream and 5-minute windows, and count vehicles
vehicles_per_stream_df = car_data_df.groupBy(
    window(col("timestamp"), "5 minutes"),  # 5-minute windows
    col("lane")  # Stream direction: inbound, outbound
).agg(
    count("vehicle_id").alias("vehicle_count")  # Count vehicles
).select(
    col("window.start").alias("start_time"),
    col("window.end").alias("end_time"),
    col("lane"),
    col("vehicle_count")
)

vehicles_per_stream_df.show(truncate=False)
