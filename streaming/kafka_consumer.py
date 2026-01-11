"""
FAME Kafka Consumer - Real-time Data Subscriber
================================================
Consumes financial data from Kafka topics for processing.
"""

import json
import logging
from datetime import datetime
from typing import Callable, Dict, Optional
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("⚠️ kafka-python not installed. Run: pip install kafka-python")


class FAMEKafkaConsumer:
    """
    Kafka Consumer for FAME Data Space.
    
    Subscribes to Kafka topics and processes incoming messages.
    """
    
    def __init__(self, 
                 topics: list = None,
                 bootstrap_servers: str = "localhost:29092",
                 group_id: str = "fame-consumer-group"):
        """Initialize Kafka consumer."""
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics or ["fame-stocks", "fame-forex", "fame-transactions"]
        self.consumer = None
        self._running = False
        self._handlers = {}
        
        if KAFKA_AVAILABLE:
            try:
                self.consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=bootstrap_servers,
                    group_id=group_id,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    key_deserializer=lambda k: k.decode('utf-8') if k else None
                )
                logger.info(f"✅ Kafka Consumer connected to {bootstrap_servers}")
                logger.info(f"📥 Subscribed to topics: {self.topics}")
            except Exception as e:
                logger.error(f"❌ Failed to connect to Kafka: {e}")
                self.consumer = None
        else:
            logger.warning("⚠️ Kafka not available")
    
    def register_handler(self, topic: str, handler: Callable[[Dict], None]):
        """Register a message handler for a topic."""
        self._handlers[topic] = handler
        logger.info(f"📝 Registered handler for topic: {topic}")
    
    def _default_handler(self, topic: str, key: str, value: Dict):
        """Default message handler - just logs the message."""
        logger.info(f"📨 [{topic}] {key}: {json.dumps(value)[:100]}...")
    
    def consume(self, timeout_ms: int = 1000):
        """
        Start consuming messages from Kafka.
        
        This runs continuously until stopped.
        """
        if not self.consumer:
            logger.error("❌ No Kafka consumer available")
            return
        
        self._running = True
        logger.info("🚀 Starting Kafka consumer...")
        
        message_count = 0
        
        try:
            while self._running:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=timeout_ms)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        topic = record.topic
                        key = record.key
                        value = record.value
                        
                        # Call registered handler or default
                        if topic in self._handlers:
                            self._handlers[topic](value)
                        else:
                            self._default_handler(topic, key, value)
                        
                        message_count += 1
                        
                        if message_count % 100 == 0:
                            logger.info(f"📊 Processed {message_count} messages")
                            
        except KeyboardInterrupt:
            logger.info("⏹️ Consumer interrupted")
        finally:
            self.stop()
    
    def consume_async(self):
        """Start consuming in a background thread."""
        thread = threading.Thread(target=self.consume, daemon=True)
        thread.start()
        logger.info("🔄 Consumer running in background")
        return thread
    
    def stop(self):
        """Stop consuming."""
        self._running = False
        if self.consumer:
            self.consumer.close()
            logger.info("✅ Kafka Consumer closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


# Example handlers
def stock_handler(data: Dict):
    """Process stock quotes."""
    symbol = data.get('symbol', 'UNKNOWN')
    price = data.get('current_price', 0)
    change = data.get('change_percent', 0)
    
    trend = "📈" if change > 0 else "📉" if change < 0 else "➡️"
    logger.info(f"{trend} Stock: {symbol} = ${price:.2f} ({change:+.2f}%)")


def transaction_handler(data: Dict):
    """Process transactions."""
    tx_id = data.get('transaction_id', 'N/A')
    amount = data.get('amount', 0)
    tx_type = data.get('transaction_type', 'UNKNOWN')
    
    logger.info(f"💳 Transaction: {tx_id[:8]}... | {tx_type} | €{amount:,.2f}")


def alert_handler(data: Dict):
    """Process alerts."""
    alert_type = data.get('alert_type', 'UNKNOWN')
    message = data.get('message', 'No message')
    
    logger.warning(f"🚨 ALERT [{alert_type}]: {message}")


# CLI for testing
if __name__ == "__main__":
    with FAMEKafkaConsumer() as consumer:
        # Register handlers
        consumer.register_handler("fame-stocks", stock_handler)
        consumer.register_handler("fame-transactions", transaction_handler)
        consumer.register_handler("fame-alerts", alert_handler)
        
        # Start consuming
        print("📥 Listening for messages... (Ctrl+C to stop)")
        consumer.consume()
