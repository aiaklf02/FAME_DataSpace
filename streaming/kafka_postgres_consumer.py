"""
═══════════════════════════════════════════════════════════════════════════════
FAME Financial Data Space - Kafka to PostgreSQL Consumer
═══════════════════════════════════════════════════════════════════════════════
Consumes messages from Kafka topics and writes them to PostgreSQL
for persistence and Grafana visualization.

Run with: python streaming/kafka_postgres_consumer.py
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any
import psycopg2
from psycopg2.extras import execute_batch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'fame_transactions'),
    'user': os.getenv('POSTGRES_USER', 'fame_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'fame_password')
}

TOPICS = ['fame-stocks', 'fame-transactions', 'fame-anomalies', 'fame-forex']


class PostgresWriter:
    """Writes streaming data to PostgreSQL."""
    
    def __init__(self):
        self.conn = None
        self._connect()
        self._create_tables()
    
    def _connect(self):
        """Connect to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(**POSTGRES_CONFIG)
            self.conn.autocommit = True
            logger.info("✅ PostgreSQL connected")
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            self.conn = None
    
    def _create_tables(self):
        """Create streaming tables if they don't exist. Always recreate alerts view."""
        if not self.conn:
            return
        with self.conn.cursor() as cur:
            # Create schema
            cur.execute("CREATE SCHEMA IF NOT EXISTS fame_streaming")
            # Stock quotes table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fame_streaming.stock_quotes (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    price DECIMAL(20, 6),
                    previous_close DECIMAL(20, 6),
                    change DECIMAL(20, 6),
                    change_percent DECIMAL(10, 4),
                    volume BIGINT,
                    currency VARCHAR(10),
                    exchange VARCHAR(50),
                    market_state VARCHAR(20),
                    source VARCHAR(50),
                    is_real_data BOOLEAN DEFAULT true,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Create index for fast queries
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_quotes_symbol_time 
                ON fame_streaming.stock_quotes(symbol, timestamp DESC)
            """)
            # Transactions table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fame_streaming.transactions (
                    id SERIAL PRIMARY KEY,
                    transaction_id VARCHAR(100) UNIQUE,
                    amount DECIMAL(20, 2),
                    currency VARCHAR(10),
                    transaction_type VARCHAR(50),
                    status VARCHAR(20),
                    sender_id VARCHAR(100),
                    receiver_id VARCHAR(100),
                    source VARCHAR(50),
                    is_real_data BOOLEAN DEFAULT false,
                    timestamp TIMESTAMP,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Anomalies table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fame_streaming.anomalies (
                    id SERIAL PRIMARY KEY,
                    anomaly_type VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50),
                    value DECIMAL(20, 6),
                    severity VARCHAR(20),
                    message TEXT,
                    z_score DECIMAL(10, 4),
                    mean_value DECIMAL(20, 6),
                    threshold DECIMAL(20, 6),
                    detection_method VARCHAR(50),
                    timestamp TIMESTAMP,
                    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Create index for anomalies
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomalies_time 
                ON fame_streaming.anomalies(timestamp DESC)
            """)
            # Always drop alerts table if it exists (to avoid view conflict)
            cur.execute("DROP TABLE IF EXISTS fame_streaming.alerts CASCADE;")
            # Always drop alerts view if it exists
            cur.execute("DROP VIEW IF EXISTS fame_streaming.alerts CASCADE;")
            # Alerts view for Grafana
            cur.execute("""
                CREATE OR REPLACE VIEW fame_streaming.alerts AS
                SELECT 
                    anomaly_type as alert_type,
                    symbol,
                    message,
                    severity,
                    timestamp
                FROM fame_streaming.anomalies
                ORDER BY timestamp DESC
            """)
            logger.info("✅ Streaming tables created (alerts view recreated)")
    
    def write_stock(self, data: Dict):
        """Write stock quote to PostgreSQL."""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fame_streaming.stock_quotes 
                    (symbol, price, previous_close, change, change_percent, 
                     volume, currency, exchange, market_state, source, is_real_data, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data.get('symbol'),
                    data.get('price'),
                    data.get('previous_close'),
                    data.get('change'),
                    data.get('change_percent'),
                    data.get('volume'),
                    data.get('currency'),
                    data.get('exchange'),
                    data.get('market_state'),
                    data.get('_source', 'kafka'),
                    data.get('_real_data', True),
                    data.get('timestamp', datetime.utcnow().isoformat())
                ))
            return True
        except Exception as e:
            logger.error(f"Stock write error: {e}")
            return False
    
    def write_transaction(self, data: Dict):
        """Write transaction to PostgreSQL."""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fame_streaming.transactions 
                    (transaction_id, amount, currency, transaction_type, status,
                     sender_id, receiver_id, source, is_real_data, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (
                    data.get('transaction_id'),
                    data.get('amount'),
                    data.get('currency'),
                    data.get('transaction_type'),
                    data.get('status'),
                    data.get('sender_id'),
                    data.get('receiver_id'),
                    data.get('_source', 'kafka'),
                    data.get('_real_data', False),
                    data.get('timestamp', datetime.utcnow().isoformat())
                ))
            return True
        except Exception as e:
            logger.error(f"Transaction write error: {e}")
            return False
    
    def write_anomaly(self, data: Dict):
        """Write anomaly to PostgreSQL."""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fame_streaming.anomalies 
                    (anomaly_type, symbol, value, severity, message, 
                     z_score, mean_value, threshold, detection_method, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data.get('type'),
                    data.get('symbol'),
                    data.get('value'),
                    data.get('severity'),
                    data.get('message'),
                    data.get('z_score'),
                    data.get('mean'),
                    data.get('threshold'),
                    data.get('detection_method'),
                    data.get('timestamp', datetime.utcnow().isoformat())
                ))
            return True
        except Exception as e:
            logger.error(f"Anomaly write error: {e}")
            return False
    
    def close(self):
        if self.conn:
            self.conn.close()


class KafkaPostgresConsumer:
    """Consumes Kafka messages and writes to PostgreSQL."""
    
    def __init__(self):
        self.consumer = None
        self.pg_writer = PostgresWriter()
        self.running = False
        self.stats = {
            'stocks': 0,
            'transactions': 0,
            'anomalies': 0,
            'errors': 0
        }
        self._connect_kafka()
    
    def _connect_kafka(self):
        """Connect to Kafka."""
        try:
            from kafka import KafkaConsumer
            self.consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id='fame-postgres-consumer',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                consumer_timeout_ms=1000
            )
            logger.info(f"✅ Kafka connected: {KAFKA_BOOTSTRAP}")
            logger.info(f"📥 Subscribed to: {TOPICS}")
        except ImportError:
            logger.error("❌ kafka-python not installed! Run: pip install kafka-python")
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
    
    def _process_message(self, topic: str, key: str, value: Dict):
        """Process a Kafka message."""
        try:
            if topic == 'fame-stocks':
                if self.pg_writer.write_stock(value):
                    self.stats['stocks'] += 1
            elif topic == 'fame-transactions':
                if self.pg_writer.write_transaction(value):
                    self.stats['transactions'] += 1
            elif topic == 'fame-anomalies':
                if self.pg_writer.write_anomaly(value):
                    self.stats['anomalies'] += 1
                    logger.warning(f"🚨 Anomaly stored: {value.get('message', 'Unknown')}")
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Processing error: {e}")
    
    def consume(self):
        """Start consuming messages."""
        if not self.consumer:
            logger.error("❌ No Kafka consumer available")
            return
        
        self.running = True
        last_status = time.time()
        
        logger.info("\n" + "═" * 60)
        logger.info("🚀 KAFKA → POSTGRESQL CONSUMER STARTED")
        logger.info("═" * 60)
        logger.info("📥 Waiting for messages...")
        logger.info("⏹️  Press Ctrl+C to stop\n")
        
        try:
            while self.running:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        self._process_message(
                            record.topic,
                            record.key,
                            record.value
                        )
                
                # Print status every 30 seconds
                if time.time() - last_status >= 30:
                    logger.info(f"📊 Stats: Stocks={self.stats['stocks']} | "
                               f"Transactions={self.stats['transactions']} | "
                               f"Anomalies={self.stats['anomalies']} | "
                               f"Errors={self.stats['errors']}")
                    last_status = time.time()
                    
        except KeyboardInterrupt:
            logger.info("\n⏹️ Consumer interrupted")
        finally:
            self.stop()
    
    def stop(self):
        """Stop consumer."""
        self.running = False
        
        if self.consumer:
            self.consumer.close()
        
        self.pg_writer.close()
        
        logger.info("\n" + "═" * 60)
        logger.info("📊 FINAL STATISTICS")
        logger.info("═" * 60)
        logger.info(f"   Stocks Written:       {self.stats['stocks']}")
        logger.info(f"   Transactions Written: {self.stats['transactions']}")
        logger.info(f"   Anomalies Written:    {self.stats['anomalies']}")
        logger.info(f"   Errors:               {self.stats['errors']}")
        logger.info("═" * 60)
        logger.info("✅ Consumer stopped")


def main():
    consumer = KafkaPostgresConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
