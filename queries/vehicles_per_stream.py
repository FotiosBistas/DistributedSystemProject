from dotenv import dotenv_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Load environment variables
env_values = dotenv_values(dotenv_path="../.env")

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Vehicle Count Per Stream") \
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

# Count vehicles per stream
vehicle_count_df = car_data_df.groupBy("lane").agg(
    count("vehicle_id").alias("vehicle_count")
)

vehicle_count_df.show()
