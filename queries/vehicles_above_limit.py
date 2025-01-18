from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
from dotenv import dotenv_values

# Load environment variables
env_values = dotenv_values(dotenv_path="../.env")

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Exceed Speed Limit Analysis") \
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

# Add a column to check if the vehicle exceeded the speed limit
# Limit: 90 km/h for cars, 80 km/h for trucks
exceeded_speed_limit_df = car_data_df.withColumn(
    "exceeded_speed_limit",
    when((col("type") == "car") & (col("speed") > 0.5), 1)
    .when((col("type") == "truck") & (col("speed") > 0.5), 1)
    .otherwise(0)
)

# Count the total number of vehicles that exceeded the speed limit
total_exceeded = exceeded_speed_limit_df.filter(col("exceeded_speed_limit") == 1) \
    .agg(count("vehicle_id").alias("total_exceeded"))

# Show the result
total_exceeded.show()
