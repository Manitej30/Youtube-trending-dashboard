from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("YouTubeStreamingPipeline")

# Kafka + Postgres configs
KAFKA_BROKER = "kafka:29092"
TOPIC_NAME = "youtube_trending"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_TABLE = "youtube_trending_videos"

CHECKPOINT_DIR = "/tmp/spark-checkpoint-youtube"

# Spark session (NO packages here — we pass in spark-submit)
spark = SparkSession.builder \
    .appName("YouTubeTrendingConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType([
    StructField("id", StringType()),
    StructField("snippet", StructType([
        StructField("title", StringType()),
        StructField("channelTitle", StringType()),
        StructField("publishedAt", StringType())
    ])),
    StructField("statistics", StructType([
        StructField("viewCount", StringType()),
        StructField("likeCount", StringType()),
        StructField("commentCount", StringType())
    ]))
])

# Read from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

logger.info("Kafka stream initialized successfully")

# Parse JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.id").alias("video_id"),
        col("data.snippet.title").alias("title"),
        col("data.snippet.channelTitle").alias("channel_title"),
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

# Start stream
query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

logger.info("Streaming started...")
query.awaitTermination()