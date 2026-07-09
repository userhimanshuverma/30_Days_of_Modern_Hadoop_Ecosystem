#!/usr/bin/env python3
"""
Day 18: Spark Structured Streaming Application
Location: Day-18-Spark-Structured-Streaming/source/StreamingApp.py

This script implements a production-grade Structured Streaming application:
1. Consumes JSON events from a Kafka topic.
2. Deserializes the messages and applies a schema.
3. Implements watermarking (10-minute threshold) on event_time.
4. Performs windowed aggregations (10-minute window, 5-minute slide).
5. Writes the aggregated output to a Parquet directory on HDFS in 'Append' mode.
6. Configures structured checkpointing for fault-tolerant query state recovery.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def main():
    bootstrap_servers = "kafka:29092"
    input_topic = "clickstream"
    checkpoint_dir = "hdfs://namenode:9000/tmp/spark-checkpoints/clickstream"
    output_dir = "hdfs://namenode:9000/tmp/spark-outputs/clickstream"

    print(f"Starting Spark Structured Streaming job...")
    print(f"Reading from Kafka bootstrap: {bootstrap_servers}, topic: {input_topic}")
    print(f"Checkpoint location: {checkpoint_dir}")
    print(f"Output location: {output_dir}")

    # Initialize Spark Session (automatically picks up packages from spark-defaults.conf)
    spark = SparkSession.builder \
        .appName("SparkStructuredStreamingLab") \
        .getOrCreate()

    # Set log level to WARN to reduce terminal noise
    spark.sparkContext.setLogLevel("WARN")

    # Define Schema for the incoming Clickstream Events
    # Example raw JSON: {"event_time": "2026-07-09T18:00:00Z", "user_id": "U1001", "action": "click", "page": "homepage"}
    clickstream_schema = StructType([
        StructField("event_time", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("page", StringType(), True)
    ])

    # 1. Read Stream from Kafka Topic
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Extract Value string, parse JSON, and apply schema
    parsed_stream_df = kafka_stream_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), clickstream_schema).alias("data")) \
        .select("data.*")

    # 3. Apply event-time watermarking & windowing
    # - Watermark: 10 minutes (allows Spark to drop states for events older than 10 minutes)
    # - Window: 10 minutes duration, sliding every 5 minutes
    windowed_aggregations = parsed_stream_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes", "5 minutes"),
            col("action")
        ) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("action"),
            col("count"),
            current_timestamp().alias("processed_time")
        )

    # 4. Write Stream to Parquet Sink (requires Append mode for file sinks with aggregations)
    # NOTE: File-based output sinks only support "append" mode, which means Spark will
    # only write a window's aggregation output when the watermark passes the window end time.
    parquet_query = windowed_aggregations.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_dir) \
        .option("path", output_dir) \
        .trigger(processingTime="10 seconds") \
        .start()

    # 5. Optional: Console Sink in Update mode for verification / live debugging
    # This outputs partial/updating aggregates to stdout as events arrive
    console_query = windowed_aggregations.writeStream \
        .format("console") \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .start()

    # Await termination of the queries
    print("Streaming queries started successfully. Awaiting termination...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
