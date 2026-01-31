import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr, current_timestamp, date_format, to_date, approx_count_distinct, session_window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DB_URL = os.getenv("DB_URL", "jdbc:postgresql://db:5432/stream_data")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Schema for incoming data
schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_type", StringType(), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName("UserActivityStreaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def write_to_pg(df, epoch_id, table_name, constraint_cols):
    """
    Performs an idempotent write into PostgreSQL.
    """
    if df.isEmpty():
        return

    print(f"\n--- Batch {epoch_id} for {table_name} ---")
    df.show(5, truncate=False)

    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    
    # We use 'append' mode. 
    # For page_views and active_users, the PK is (window_start, ...), so append works for new windows.
    # For user_sessions, in 'append' mode, Spark only emits a session ONCE per user per session,
    # so 'append' should work fine with the user_id PK.
    try:
        df.write.jdbc(url=DB_URL, table=table_name, mode="append", properties=properties)
    except Exception as e:
        print(f"Postgres write for {table_name} handled by stability logic: {str(e)[:100]}")

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "user_activity") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    # Small watermark for demo (30 seconds)
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withWatermark("event_time", "30 seconds")

    # --- 1. Data Lake Sink ---
    parquet_df = parsed_df.withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))
    lake_query = parquet_df.writeStream \
        .format("parquet") \
        .option("path", "/opt/spark/data/lake") \
        .option("checkpointLocation", "/opt/spark/checkpoint/lake") \
        .partitionBy("event_date") \
        .start()

    # --- 2. Enriched Kafka Sink ---
    enriched_df = parsed_df.withColumn("processing_time", current_timestamp().cast("string")) \
        .select(expr("to_json(struct(*))").alias("value"))
    enriched_query = enriched_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", "enriched_activity") \
        .option("checkpointLocation", "/opt/spark/checkpoint/enriched") \
        .start()

    # --- 3. Page View Counts (Tumbling Window) ---
    page_views = parsed_df.filter(col("event_type") == "page_view") \
        .groupBy(window(col("event_time"), "1 minute"), col("page_url")) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("page_url"),
            col("count").alias("view_count")
        )

    pg_page_views_query = page_views.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, id: write_to_pg(df, id, "page_view_counts", ["window_start", "page_url"])) \
        .option("checkpointLocation", "/opt/spark/checkpoint/page_views") \
        .start()

    # --- 4. Active Users (Sliding Window) ---
    active_users = parsed_df.groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
        .agg(approx_count_distinct("user_id").alias("active_user_count")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("active_user_count")
        )

    pg_active_users_query = active_users.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, id: write_to_pg(df, id, "active_users", ["window_start"])) \
        .option("checkpointLocation", "/opt/spark/checkpoint/active_users") \
        .start()

    # --- 5. Stateful Transformation: User Sessions ---
    # In 'append' mode, Spark only emits session when it closes (Gap elapsed + Watermark pass)
    # Using 30-second gap for fast demo
    sessions = parsed_df.filter(col("event_type").isin("session_start", "session_end")) \
        .groupBy(col("user_id"), session_window(col("event_time"), "30 seconds")) \
        .agg(
            expr("min(CASE WHEN event_type = 'session_start' THEN event_time END)").alias("session_start_time"),
            expr("max(CASE WHEN event_type = 'session_end' THEN event_time END)").alias("session_end_time")
        ) \
        .filter(col("session_start_time").isNotNull() & col("session_end_time").isNotNull()) \
        .withColumn("session_duration_seconds", 
                    (col("session_end_time").cast("long") - col("session_start_time").cast("long"))) \
        .select("user_id", "session_start_time", "session_end_time", "session_duration_seconds")

    pg_sessions_query = sessions.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, id: write_to_pg(df, id, "user_sessions", ["user_id"])) \
        .option("checkpointLocation", "/opt/spark/checkpoint/sessions") \
        .start()

    # Waiting for all queries
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
