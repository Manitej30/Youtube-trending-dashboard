from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Kafka + Postgres connection configs
KAFKA_BROKER = "kafka:29092"  # Internal listener for Spark container
TOPIC_NAME = "youtube_trending"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_TABLE = "youtube_trending_videos"

# Spark session
spark = SparkSession.builder \
    .appName("YouTubeTrendingConsumer") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for YouTube API JSON
snippet_schema = StructType([
    StructField("title", StringType()),
    StructField("channelTitle", StringType()),
    StructField("publishedAt", StringType())
])

statistics_schema = StructType([
    StructField("viewCount", StringType()),
    StructField("likeCount", StringType()),
    StructField("commentCount", StringType())
])

video_schema = StructType([
    StructField("id", StringType()),
    StructField("snippet", snippet_schema),
    StructField("statistics", statistics_schema)
])

# Read from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "youtube_trending") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka JSON messages
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), video_schema).alias("data")) \
    .select(
        col("data.id").alias("video_id"),
        col("data.snippet.title").alias("title"),
        col("data.snippet.channelTitle").alias("channel_title"),
        col("data.snippet.publishedAt").alias("published_at"),
        col("data.statistics.viewCount").cast(LongType()).alias("views"),
        col("data.statistics.likeCount").cast(LongType()).alias("likes"),
        col("data.statistics.commentCount").cast(LongType()).alias("comments")
    )

# Write to Postgres
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", POSTGRES_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
