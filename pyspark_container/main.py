from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, length
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
import time 

# Define schema for incoming data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("content", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("favourites", LongType(), True),
    StructField("reblogs", LongType(), True),
    StructField("hashtags", ArrayType(StringType()), True)
])

spark = SparkSession.builder \
    .appName("MastodonStreamProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.apache.kafka:kafka-clients:3.3.1,"
            "org.postgresql:postgresql:42.2.18") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/correct/path/to/log4j.properties") \
    .getOrCreate()


# Read from Kafka topic
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "mastodonStream") \
    .load()

# Parse the Kafka stream data
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Filter for content containing "AI"
keyword_filtered_df = parsed_df.filter(col("content").contains("AI"))

# Aggregate by window of 1 hour
windowed_df = keyword_filtered_df \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .groupBy(window(col("timestamp"), "1 hour")) \
    .count() \
    .withColumn("start_time", col("window.start")) \
    .withColumn("end_time", col("window.end")) \
    .drop("window")

# Calculate average toot length per user
avg_toot_length_df = keyword_filtered_df \
    .withColumn("toot_length", length(col("content"))) \
    .groupBy("user_id") \
    .agg({"toot_length": "avg"})

# JDBC connection settings
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Write windowed aggregation to PostgreSQL
window_query = windowed_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epochId: df.write.jdbc(
        url=jdbc_url, 
        table="toot_window_counts", 
        mode="append", 
        properties=connection_properties)) \
    .start()

# Write average toot length to PostgreSQL
avg_length_query = avg_toot_length_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epochId: df.write.jdbc(
        url=jdbc_url, 
        table="avg_toot_length", 
        mode="append", 
        properties=connection_properties)) \
    .start()

window_query.awaitTermination()
avg_length_query.awaitTermination()