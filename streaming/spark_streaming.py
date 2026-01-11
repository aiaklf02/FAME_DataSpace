"""
FAME Spark Structured Streaming
================================
Real-time stream processing with Apache Spark.

Features:
- Read from Kafka topics
- Transform and enrich data
- Write to multiple sinks (DuckDB, PostgreSQL, Parquet)
- Anomaly detection
- Real-time aggregations
"""

import os
import logging
from datetime import datetime
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try importing PySpark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, from_json, to_json, struct, 
        window, avg, sum, count, max, min,
        when, lit, current_timestamp, expr
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, 
        DoubleType, TimestampType, BooleanType, LongType
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logger.warning("⚠️ PySpark not installed. Run: pip install pyspark")


class FAMESparkStreaming:
    """
    Spark Structured Streaming for FAME Data Space.
    
    Processes real-time financial data from Kafka.
    """
    
    # Kafka configuration
    KAFKA_BOOTSTRAP = "localhost:29092"
    
    # Schema definitions
    STOCK_SCHEMA = None
    TRANSACTION_SCHEMA = None
    
    def __init__(self, app_name: str = "FAME-Streaming"):
        """Initialize Spark session."""
        self.app_name = app_name
        self.spark = None
        
        if SPARK_AVAILABLE:
            self._init_schemas()
            self._init_spark()
        else:
            logger.error("❌ Spark not available")
    
    def _init_schemas(self):
        """Define schemas for Kafka messages."""
        self.STOCK_SCHEMA = StructType([
            StructField("symbol", StringType(), True),
            StructField("company_name", StringType(), True),
            StructField("exchange", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("current_price", DoubleType(), True),
            StructField("previous_close", DoubleType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_percent", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("market_cap", LongType(), True),
            StructField("timestamp", StringType(), True),
            StructField("_source", StringType(), True),
            StructField("_timestamp", StringType(), True)
        ])
        
        self.TRANSACTION_SCHEMA = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("sender_country", StringType(), True),
            StructField("receiver_country", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("is_cross_border", BooleanType(), True),
            StructField("timestamp", StringType(), True),
            StructField("_timestamp", StringType(), True)
        ])
    
    def _init_spark(self):
        """Initialize Spark session with Kafka support."""
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.jars.packages", 
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/fame-checkpoints") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark session initialized")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark: {e}")
            self.spark = None
    
    def read_kafka_stream(self, topic: str):
        """
        Read streaming data from Kafka topic.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Streaming DataFrame
        """
        if not self.spark:
            logger.error("❌ Spark not available")
            return None
        
        logger.info(f"📥 Reading stream from Kafka topic: {topic}")
        
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.KAFKA_BOOTSTRAP) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
    
    def process_stock_stream(self):
        """
        Process real-time stock data.
        
        Pipeline:
        1. Read from Kafka
        2. Parse JSON
        3. Enrich with computed fields
        4. Detect anomalies
        5. Write to sinks
        """
        if not self.spark:
            return None
        
        logger.info("📈 Starting Stock Stream Processing...")
        
        # Read from Kafka
        raw_stream = self.read_kafka_stream("fame-stocks")
        
        if raw_stream is None:
            return None
        
        # Parse JSON and apply schema
        parsed_stream = raw_stream \
            .selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), self.STOCK_SCHEMA).alias("data")) \
            .select("data.*")
        
        # Enrich with computed fields
        enriched_stream = parsed_stream \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("price_eur", 
                when(col("currency") == "USD", col("current_price") / 1.08)
                .when(col("currency") == "GBP", col("current_price") / 0.86)
                .otherwise(col("current_price"))) \
            .withColumn("trend",
                when(col("change_percent") > 0, "UP")
                .when(col("change_percent") < 0, "DOWN")
                .otherwise("STABLE")) \
            .withColumn("is_anomaly",
                (col("change_percent").abs() > 5) | 
                (col("volume") > 100000000))
        
        return enriched_stream
    
    def process_transaction_stream(self):
        """
        Process real-time transaction data.
        
        Pipeline:
        1. Read from Kafka
        2. Parse JSON
        3. Calculate risk scores
        4. Flag suspicious transactions
        """
        if not self.spark:
            return None
        
        logger.info("💳 Starting Transaction Stream Processing...")
        
        # Read from Kafka
        raw_stream = self.read_kafka_stream("fame-transactions")
        
        if raw_stream is None:
            return None
        
        # Parse JSON
        parsed_stream = raw_stream \
            .selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), self.TRANSACTION_SCHEMA).alias("data")) \
            .select("data.*")
        
        # Enrich with risk scoring
        enriched_stream = parsed_stream \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("amount_eur",
                when(col("currency") == "USD", col("amount") / 1.08)
                .when(col("currency") == "GBP", col("amount") / 0.86)
                .otherwise(col("amount"))) \
            .withColumn("risk_level",
                when(col("amount") > 50000, "CRITICAL")
                .when(col("amount") > 10000, "HIGH")
                .when(col("amount") > 1000, "MEDIUM")
                .otherwise("LOW")) \
            .withColumn("requires_aml_check",
                (col("is_cross_border") == True) & (col("amount") > 5000))
        
        return enriched_stream
    
    def aggregate_stocks_windowed(self, stream, window_duration: str = "1 minute"):
        """
        Aggregate stock data in time windows.
        
        Calculates:
        - Average price per symbol
        - Total volume
        - Price range (high/low)
        """
        if stream is None:
            return None
        
        logger.info(f"📊 Creating windowed aggregation ({window_duration})...")
        
        return stream \
            .withWatermark("processing_time", "30 seconds") \
            .groupBy(
                window(col("processing_time"), window_duration),
                col("symbol"),
                col("exchange")
            ) \
            .agg(
                avg("current_price").alias("avg_price"),
                max("current_price").alias("high_price"),
                min("current_price").alias("low_price"),
                sum("volume").alias("total_volume"),
                count("*").alias("quote_count"),
                avg("change_percent").alias("avg_change_pct")
            )
    
    def write_to_console(self, stream, name: str = "output"):
        """Write stream to console for debugging."""
        if stream is None:
            return None
        
        return stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .queryName(name) \
            .start()
    
    def write_to_parquet(self, stream, path: str, checkpoint_path: str):
        """Write stream to Parquet files."""
        if stream is None:
            return None
        
        logger.info(f"💾 Writing stream to Parquet: {path}")
        
        return stream.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", path) \
            .option("checkpointLocation", checkpoint_path) \
            .start()
    
    def start_stock_pipeline(self):
        """
        Start the complete stock processing pipeline.
        
        Reads → Processes → Aggregates → Writes
        """
        logger.info("=" * 60)
        logger.info("🚀 FAME SPARK STREAMING - STOCK PIPELINE")
        logger.info("=" * 60)
        
        # Process stream
        stock_stream = self.process_stock_stream()
        
        if stock_stream is None:
            logger.error("❌ Failed to create stock stream")
            return
        
        # Write raw processed data to console
        console_query = self.write_to_console(stock_stream, "stocks-console")
        
        # Create windowed aggregation
        agg_stream = self.aggregate_stocks_windowed(stock_stream, "1 minute")
        agg_query = self.write_to_console(agg_stream, "stocks-aggregated")
        
        logger.info("✅ Stock pipeline started!")
        logger.info("📊 Press Ctrl+C to stop...")
        
        # Wait for termination
        if console_query:
            console_query.awaitTermination()
    
    def stop(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("✅ Spark session stopped")


# CLI for testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="FAME Spark Streaming")
    parser.add_argument("--pipeline", choices=["stocks", "transactions"], default="stocks")
    args = parser.parse_args()
    
    streaming = FAMESparkStreaming()
    
    try:
        if args.pipeline == "stocks":
            streaming.start_stock_pipeline()
        else:
            logger.info("Transaction pipeline not yet implemented")
    except KeyboardInterrupt:
        logger.info("⏹️ Stopping...")
    finally:
        streaming.stop()
