"""
FAME Data Space - Streaming Module
====================================
Real-time data streaming with Apache Kafka and Spark Structured Streaming.
"""

from .kafka_producer import FAMEKafkaProducer
from .kafka_consumer import FAMEKafkaConsumer
from .spark_streaming import FAMESparkStreaming

__all__ = ['FAMEKafkaProducer', 'FAMEKafkaConsumer', 'FAMESparkStreaming']
