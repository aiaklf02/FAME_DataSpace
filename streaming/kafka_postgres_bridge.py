"""
FAME Kafka to PostgreSQL Bridge
================================
Consumes Kafka streaming data and inserts into PostgreSQL for Superset visualization.
"""

import json
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import threading
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("⚠️ kafka-python-ng not installed")


class KafkaPostgresBridge:
    """Bridge between Kafka streaming and PostgreSQL for Superset."""
    
    def __init__(self):
        self.pg_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'fame_transactions',
            'user': 'fame_user',
            'password': 'fame_password'
        }
        self.kafka_servers = 'localhost:29092'
        self._running = False
        
    def get_pg_connection(self):
        """Get PostgreSQL connection."""
        return psycopg2.connect(**self.pg_config)
    
    def insert_stock_quote(self, data: dict):
        """Insert stock quote into PostgreSQL."""
        try:
            conn = self.get_pg_connection()
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO fame_streaming.stock_quotes 
                (symbol, price, volume, change_percent, bid, ask, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                data.get('symbol'),
                data.get('price'),
                data.get('volume'),
                data.get('change_percent'),
                data.get('bid'),
                data.get('ask'),
                datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat()))
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"  📈 Inserted: {data.get('symbol')} @ ${data.get('price')}")
            
        except Exception as e:
            logger.error(f"❌ Insert error: {e}")
    
    def insert_alert(self, data: dict):
        """Insert alert into PostgreSQL."""
        try:
            conn = self.get_pg_connection()
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO fame_streaming.alerts 
                (alert_type, symbol, message, severity, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                data.get('alert_type', 'INFO'),
                data.get('symbol'),
                data.get('message'),
                data.get('severity', 'LOW'),
                datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat()))
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"  🚨 Alert: {data.get('symbol')} - {data.get('message')}")
            
        except Exception as e:
            logger.error(f"❌ Alert insert error: {e}")
    
    def consume_and_store(self, duration_seconds: int = 60):
        """Consume from Kafka and store in PostgreSQL."""
        if not KAFKA_AVAILABLE:
            logger.error("❌ Kafka not available")
            return
        
        print("\n" + "="*60)
        print("🔄 FAME Kafka → PostgreSQL → Superset Bridge")
        print("="*60)
        print(f"📡 Kafka: {self.kafka_servers}")
        print(f"🐘 PostgreSQL: {self.pg_config['host']}:{self.pg_config['port']}")
        print(f"⏱️  Duration: {duration_seconds} seconds")
        print("="*60 + "\n")
        
        try:
            consumer = KafkaConsumer(
                'fame-stocks',
                'fame-alerts',
                bootstrap_servers=self.kafka_servers,
                group_id='fame-postgres-bridge',
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            
            logger.info("✅ Connected to Kafka")
            
            start_time = time.time()
            message_count = 0
            
            while time.time() - start_time < duration_seconds:
                for message in consumer:
                    topic = message.topic
                    data = message.value
                    
                    if topic == 'fame-stocks':
                        self.insert_stock_quote(data)
                    elif topic == 'fame-alerts':
                        self.insert_alert(data)
                    
                    message_count += 1
                    
                    if time.time() - start_time >= duration_seconds:
                        break
                
                time.sleep(0.1)
            
            consumer.close()
            
            print("\n" + "="*60)
            print(f"✅ Bridge completed - {message_count} messages processed")
            print("="*60)
            
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}")


def run_bridge_with_producer():
    """Run bridge alongside producer for demo."""
    from streaming.kafka_producer import FAMEKafkaProducer
    
    print("\n" + "="*70)
    print("🚀 FAME Real-Time Demo: Kafka → PostgreSQL → Superset")
    print("="*70)
    
    # Start producer in background
    producer = FAMEKafkaProducer()
    producer_thread = threading.Thread(
        target=producer.stream_real_time_data,
        kwargs={'duration_seconds': 60, 'interval': 5}
    )
    producer_thread.daemon = True
    producer_thread.start()
    
    time.sleep(2)  # Wait for producer to start
    
    # Run bridge
    bridge = KafkaPostgresBridge()
    bridge.consume_and_store(duration_seconds=60)
    
    # Show final counts
    try:
        conn = bridge.get_pg_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fame_streaming.stock_quotes")
        quotes = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fame_streaming.alerts")
        alerts = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        print("\n📊 Data in PostgreSQL (visible in Superset):")
        print(f"   • Stock Quotes: {quotes}")
        print(f"   • Alerts: {alerts}")
        print("\n🔗 Open Superset: http://localhost:8088")
        print("   Query: SELECT * FROM fame_streaming.stock_quotes ORDER BY timestamp DESC")
        
    except Exception as e:
        logger.error(f"Count error: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--demo':
        run_bridge_with_producer()
    else:
        bridge = KafkaPostgresBridge()
        bridge.consume_and_store(duration_seconds=120)
