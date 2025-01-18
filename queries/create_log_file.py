import logging
from dotenv import dotenv_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window

# Load environment variables
env_values = dotenv_values(dotenv_path="../.env")

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Create log file") \
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

# Do not divide by 1000 since the timestamps must be in seconds
car_data_df = car_data_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

average_speed_df = car_data_df.groupBy(
    window(col("timestamp"), "1 minutes"),  
    col("lane")  # Stream direction: inbound, outbound
).agg(
    avg("speed").alias("average_speed")
).select(
    col("window.start").alias("start_time"),
    col("window.end").alias("end_time"),
    col("lane"),
    col("average_speed")
)

# Set up logging
logging.basicConfig(
    filename="traffic_speed_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Map 5-minute intervals to ordinal labels
def get_ordinal_interval(index):
    ordinals = ["1st", "2nd", "3rd"] + [f"{i}th" for i in range(4, 21)]
    return ordinals[index % len(ordinals)]  # Cycle through ordinals if needed

# Collect results and log them
average_speed_logs = average_speed_df.collect()

interval_start = min([row["start_time"] for row in average_speed_logs])  # Starting time
for row in average_speed_logs:
    lane = row["lane"]  # inbound or outbound
    start_time = row["start_time"]
    avg_speed = round(row["average_speed"], 2)

    # Calculate the interval number
    interval_number = int((start_time - interval_start).total_seconds() / 300) + 1
    ordinal_interval = get_ordinal_interval(interval_number)

    # Log the message
    logging.info(f"({lane}, {ordinal_interval} 5min, {avg_speed} kmh)")
