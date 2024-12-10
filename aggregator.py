from dotenv import dotenv_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType


env_values = dotenv_values("./.env", verbose=True)

# Schema for Kafka JSON messages
schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", FloatType(), True),
    StructField("position", StructType([
        StructField("x", FloatType(), True),
        StructField("y", FloatType(), True)
    ]), True),
    StructField("type", StringType(), True),
    StructField("speed", FloatType(), True),
])

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()


print(spark.sparkContext._jsc.sc().listJars())

streaming_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "processed_cars") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON messages
json_df = streaming_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*", 
            col("position.x").alias("position_x"), 
            col("position.y").alias("position_y")) \
    .drop("position")

#query = json_df.writeStream.outputMode("append").format("console").start()

jdbc_url = f"jdbc:postgresql://distributed.postgres.database.azure.com:5432/postgres?user={env_values['PGUSER']}&password={env_values['PGPASSWORD']}&sslmode=require"
db_properties = {
    "user": env_values["PGUSER"],
    "password": env_values["PGPASSWORD"],
    "driver": "org.postgresql.Driver",
    "sslmode": "require"
}

def write_to_postgres(batch_df, batch_id):
    try:
        # Filter invalid rows again as a safeguard
        valid_df = batch_df.filter(
            col("vehicle_id").isNotNull() &
            col("timestamp").isNotNull()
        )

        # Write valid data to PostgreSQL
        valid_df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "car_data") \
            .options(**db_properties) \
            .mode("append") \
            .save()

    except Exception as e:
        print(f"Error writing batch {batch_id} to PostgreSQL: {e}")


query = json_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
