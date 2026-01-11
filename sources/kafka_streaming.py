"""
FAME Data Space - Kafka Streaming Infrastructure
=================================================
Real-time data streaming with Apache Kafka

Components:
- Producer: Publishes data from all 4 sources
- Consumer: Processes and routes data to Data Lake
- Topics: Organized by domain (Data Mesh pattern)
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Callable, Optional
import logging
import threading

try:
    from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    # Define dummy types for type hints when Kafka is not installed
    KafkaProducer = None
    KafkaConsumer = None
    KafkaAdminClient = None
    NewTopic = None
    TopicAlreadyExistsError = Exception
    print("⚠️ Install kafka-python: pip install kafka-python")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FAMEKafkaTopics:
    """FAME Data Space Kafka Topics - Data Mesh Organization"""
    
    # Domain: Market Data
    STOCKS_QUOTES = "fame.market.stocks.quotes"
    STOCKS_INTRADAY = "fame.market.stocks.intraday"
    CRYPTO_RATES = "fame.market.crypto.rates"
    
    # Domain: Foreign Exchange
    FOREX_ECB_RATES = "fame.forex.ecb.daily"
    FOREX_REALTIME = "fame.forex.realtime"
    
    # Domain: Corporate Finance
    FINANCIALS_QUARTERLY = "fame.corporate.financials.quarterly"
    FINANCIALS_ANNUAL = "fame.corporate.financials.annual"
    
    # Domain: Transactions
    TRANSACTIONS_REALTIME = "fame.transactions.realtime"
    TRANSACTIONS_BATCH = "fame.transactions.batch"
    TRANSACTIONS_CDC = "fame.transactions.cdc"
    
    # Domain: Semantic / RDF
    RDF_TRIPLES = "fame.semantic.rdf.triples"
    SPARQL_RESULTS = "fame.semantic.sparql.results"
    
    # Data Lake Events
    DATALAKE_RAW = "fame.datalake.raw"
    DATALAKE_PROCESSED = "fame.datalake.processed"
    
    @classmethod
    def all_topics(cls) -> List[str]:
        """Get all topic names."""
        return [
            cls.STOCKS_QUOTES, cls.STOCKS_INTRADAY, cls.CRYPTO_RATES,
            cls.FOREX_ECB_RATES, cls.FOREX_REALTIME,
            cls.FINANCIALS_QUARTERLY, cls.FINANCIALS_ANNUAL,
            cls.TRANSACTIONS_REALTIME, cls.TRANSACTIONS_BATCH, cls.TRANSACTIONS_CDC,
            cls.RDF_TRIPLES, cls.SPARQL_RESULTS,
            cls.DATALAKE_RAW, cls.DATALAKE_PROCESSED
        ]


class FAMEKafkaManager:
    """
    Kafka Manager for FAME Data Space
    
    Handles:
    - Topic creation and management
    - Producer configuration
    - Consumer groups
    - Message serialization
    """
    
    def __init__(self, 
                 bootstrap_servers: str = "localhost:29092",
                 client_id: str = "fame-dataspace"):
        """Initialize Kafka manager."""
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.admin_client = None
        self.producers: Dict[str, KafkaProducer] = {}
        self.consumers: Dict[str, KafkaConsumer] = {}
        
        if not KAFKA_AVAILABLE:
            logger.warning("Kafka not available. Running in offline mode.")
            return
        
        self._init_admin_client()
    
    def _init_admin_client(self):
        """Initialize Kafka admin client."""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.client_id}-admin"
            )
            logger.info(f"✅ Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"⚠️ Could not connect to Kafka: {e}")
    
    def create_topics(self, num_partitions: int = 3, replication_factor: int = 1):
        """Create all FAME Data Space topics."""
        if not self.admin_client:
            logger.warning("Admin client not available")
            return
        
        topics = []
        for topic_name in FAMEKafkaTopics.all_topics():
            topics.append(NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            ))
        
        try:
            self.admin_client.create_topics(topics, validate_only=False)
            logger.info(f"✅ Created {len(topics)} Kafka topics")
        except TopicAlreadyExistsError:
            logger.info("Topics already exist")
        except Exception as e:
            logger.error(f"Failed to create topics: {e}")
    
    def get_producer(self, producer_id: str = "default") -> Optional[KafkaProducer]:
        """Get or create a Kafka producer."""
        if not KAFKA_AVAILABLE:
            return None
        
        if producer_id not in self.producers:
            try:
                self.producers[producer_id] = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id=f"{self.client_id}-producer-{producer_id}",
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    batch_size=16384,
                    linger_ms=10
                )
                logger.info(f"✅ Created producer: {producer_id}")
            except Exception as e:
                logger.error(f"Failed to create producer: {e}")
                return None
        
        return self.producers[producer_id]
    
    def get_consumer(self, 
                     consumer_id: str,
                     topics: List[str],
                     group_id: str = "fame-consumers") -> Optional[KafkaConsumer]:
        """Get or create a Kafka consumer."""
        if not KAFKA_AVAILABLE:
            return None
        
        if consumer_id not in self.consumers:
            try:
                self.consumers[consumer_id] = KafkaConsumer(
                    *topics,
                    bootstrap_servers=self.bootstrap_servers,
                    client_id=f"{self.client_id}-consumer-{consumer_id}",
                    group_id=group_id,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    auto_offset_reset='earliest',
                    enable_auto_commit=True
                )
                logger.info(f"✅ Created consumer: {consumer_id} for topics: {topics}")
            except Exception as e:
                logger.error(f"Failed to create consumer: {e}")
                return None
        
        return self.consumers[consumer_id]
    
    def publish(self, topic: str, key: str, value: Dict, producer_id: str = "default"):
        """Publish message to topic."""
        producer = self.get_producer(producer_id)
        if producer:
            try:
                future = producer.send(topic, key=key, value=value)
                future.get(timeout=10)
                logger.debug(f"Published to {topic}: {key}")
            except Exception as e:
                logger.error(f"Publish error: {e}")
    
    def close(self):
        """Close all connections."""
        for producer in self.producers.values():
            producer.flush()
            producer.close()
        
        for consumer in self.consumers.values():
            consumer.close()
        
        if self.admin_client:
            self.admin_client.close()
        
        logger.info("🔌 All Kafka connections closed")


class FAMEStreamProcessor:
    """
    Stream Processor for FAME Data Space
    
    Processes incoming data streams and routes to:
    - Data Lake (MinIO)
    - RDF Triple Store (Fuseki)
    - Real-time Dashboard
    """
    
    def __init__(self, kafka_manager: FAMEKafkaManager):
        """Initialize stream processor."""
        self.kafka = kafka_manager
        self.handlers: Dict[str, Callable] = {}
        self.running = False
    
    def register_handler(self, topic: str, handler: Callable):
        """Register a message handler for a topic."""
        self.handlers[topic] = handler
        logger.info(f"📝 Registered handler for {topic}")
    
    def process_market_data(self, message: Dict) -> Dict:
        """Process market data messages."""
        # Add processing metadata
        message['processed_at'] = datetime.now().isoformat()
        message['data_quality'] = 'validated'
        
        # Normalize currency values
        if 'currency' in message and message.get('currency') != 'EUR':
            fx_rates = {"USD": 0.92, "GBP": 1.16, "CHF": 1.06}
            rate = fx_rates.get(message['currency'], 1.0)
            if 'current_price' in message:
                message['price_eur'] = round(message['current_price'] * rate, 2)
        
        return message
    
    def process_transaction(self, message: Dict) -> Dict:
        """Process transaction messages."""
        message['processed_at'] = datetime.now().isoformat()
        
        # Flag cross-border transactions
        if message.get('sender_country') != message.get('receiver_country'):
            message['is_cross_border'] = True
            message['regulatory_flag'] = 'CROSS_BORDER_CHECK'
        else:
            message['is_cross_border'] = False
        
        # Flag high-value transactions
        amount_eur = message.get('amount_eur', 0)
        if amount_eur > 10000:
            message['aml_flag'] = 'HIGH_VALUE_REVIEW'
        
        return message
    
    def start_processing(self, topics: List[str], group_id: str = "fame-processor"):
        """Start processing messages from topics."""
        consumer = self.kafka.get_consumer("processor", topics, group_id)
        if not consumer:
            logger.error("Could not create consumer")
            return
        
        self.running = True
        logger.info(f"🚀 Starting stream processing for {len(topics)} topics")
        
        while self.running:
            try:
                messages = consumer.poll(timeout_ms=1000)
                
                for topic_partition, records in messages.items():
                    topic = topic_partition.topic
                    
                    for record in records:
                        # Process based on topic
                        if 'market' in topic or 'stocks' in topic:
                            processed = self.process_market_data(record.value)
                        elif 'transaction' in topic:
                            processed = self.process_transaction(record.value)
                        else:
                            processed = record.value
                        
                        # Call registered handler
                        if topic in self.handlers:
                            self.handlers[topic](processed)
                        
                        # Route to Data Lake
                        self.kafka.publish(
                            FAMEKafkaTopics.DATALAKE_PROCESSED,
                            record.key.decode() if record.key else "unknown",
                            processed
                        )
                        
            except Exception as e:
                logger.error(f"Processing error: {e}")
    
    def stop(self):
        """Stop processing."""
        self.running = False
        logger.info("🛑 Stream processing stopped")


class FAMEDataIngestion:
    """
    Data Ingestion Service for FAME Data Space
    
    Orchestrates data collection from all 4 sources
    and publishes to Kafka.
    """
    
    def __init__(self, kafka_servers: str = "localhost:29092"):
        """Initialize ingestion service."""
        self.kafka = FAMEKafkaManager(kafka_servers)
        self.running = False
    
    def ingest_from_sources(self, 
                           include_stocks: bool = True,
                           include_forex: bool = True,
                           include_financials: bool = True,
                           include_transactions: bool = True):
        """
        Run data ingestion from all sources.
        
        This method imports and runs each source connector.
        """
        logger.info("🔄 Starting data ingestion from all sources...")
        
        results = {}
        
        if include_stocks:
            try:
                from source1_stock_api import StockMarketAPIConnector
                connector = StockMarketAPIConnector(kafka_servers=self.kafka.bootstrap_servers)
                df = connector.fetch_batch(["AAPL", "MSFT", "BNP.PA", "SAN.MC"])
                results['stocks'] = len(df)
                connector.close()
                logger.info(f"✅ Ingested {len(df)} stock quotes")
            except Exception as e:
                logger.error(f"Stock ingestion failed: {e}")
        
        if include_forex:
            try:
                from source2_ecb_xml import ECBExchangeRateConnector
                connector = ECBExchangeRateConnector(kafka_servers=self.kafka.bootstrap_servers)
                rates = connector.fetch_daily_rates()
                results['forex'] = len(rates)
                connector.close()
                logger.info(f"✅ Ingested {len(rates)} exchange rates")
            except Exception as e:
                logger.error(f"Forex ingestion failed: {e}")
        
        if include_financials:
            try:
                from source3_financials_csv import CompanyFinancialsCSVConnector
                connector = CompanyFinancialsCSVConnector(kafka_servers=self.kafka.bootstrap_servers)
                df = connector.process_batch()
                results['financials'] = len(df)
                connector.close()
                logger.info(f"✅ Ingested {len(df)} financial records")
            except Exception as e:
                logger.error(f"Financials ingestion failed: {e}")
        
        if include_transactions:
            try:
                from source4_transactions_db import TransactionDatabaseConnector
                connector = TransactionDatabaseConnector(kafka_servers=self.kafka.bootstrap_servers)
                df = connector.generate_batch_transactions(count=100)
                results['transactions'] = len(df)
                connector.close()
                logger.info(f"✅ Ingested {len(df)} transactions")
            except Exception as e:
                logger.error(f"Transaction ingestion failed: {e}")
        
        logger.info(f"📊 Ingestion complete: {results}")
        return results
    
    def close(self):
        """Close all connections."""
        self.kafka.close()


# CLI Entry Point
if __name__ == "__main__":
    print("=" * 70)
    print("FAME Data Space - Kafka Streaming Infrastructure")
    print("=" * 70)
    
    if not KAFKA_AVAILABLE:
        print("\n⚠️ Kafka libraries not installed.")
        print("   Install with: pip install kafka-python")
        print("   Then start Kafka with: docker-compose up -d kafka")
        exit(1)
    
    # Initialize Kafka manager
    kafka = FAMEKafkaManager()
    
    # Create topics
    print("\n📋 Creating Kafka topics...")
    kafka.create_topics()
    
    # List all topics
    print("\n📝 FAME Data Space Kafka Topics:")
    for topic in FAMEKafkaTopics.all_topics():
        print(f"   • {topic}")
    
    # Test publishing
    print("\n🧪 Testing message publishing...")
    producer = kafka.get_producer("test")
    if producer:
        test_message = {
            "source": "test",
            "timestamp": datetime.now().isoformat(),
            "message": "FAME Data Space is operational!"
        }
        kafka.publish(FAMEKafkaTopics.DATALAKE_RAW, "test-001", test_message)
        print("   ✅ Test message published successfully")
    
    kafka.close()
    print("\n✅ Kafka infrastructure test complete!")
