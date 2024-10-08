from mastodon_manager import Mastodon_manager
from producer import Producer 

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, length
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
import time 

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
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
            "org.postgresql:postgresql:42.2.18") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
    .config("spark.executorEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-arm64") \
    .config("spark.driverEnv.JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-arm64") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()


kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mastodonStream") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


keyword_filtered_df = parsed_df.filter(col("content").contains("AI"))

windowed_df = keyword_filtered_df \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .groupBy(window(col("timestamp"), "1 hour")) \
    .count() \
    .withColumn("start_time", col("window.start")) \
    .withColumn("end_time", col("window.end")) \
    .drop("window")

avg_toot_length_df = keyword_filtered_df \
    .withColumn("toot_length", length(col("content"))) \
    .groupBy("user_id") \
    .agg({"toot_length": "avg"})

jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

window_query = windowed_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epochId: df.write.jdbc(
        url=jdbc_url, 
        table="toot_window_counts", 
        mode="append", 
        properties=connection_properties)) \
    .start()

avg_length_query = avg_toot_length_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epochId: df.write.jdbc(
        url=jdbc_url, 
        table="avg_toot_length", 
        mode="append", 
        properties=connection_properties)) \
    .start()



def stream_toots():
    api = Mastodon_manager().mastodon
    producer = Producer().producer
    while True:
        toots = api.timeline_hashtag('DataScience')  # Example hashtag
        for toot in toots:
            toot_data = {
                'user_id': toot['account']['id'],
                'content': toot['content'],
                'timestamp': toot['created_at'].isoformat(),
                'favourites': toot['favourites_count'],
                'reblogs': toot['reblogs_count'],
                'hashtags': [tag['name'] for tag in toot['tags']]
            }
            producer.send('mastodonStream', toot_data)
            print(f"Sent toot to Kafka: {toot_data}")
        time.sleep(10)  # Control API rate limits

if __name__ == '__main__':
    print('Done')
    window_query.awaitTermination()
    avg_length_query.awaitTermination()
    print('DOne DONE')
    #stream_toots()
    pass
