"""
═══════════════════════════════════════════════════════════════════════════════
FAME Financial Data Space - Kafka Real-Time Streaming
═══════════════════════════════════════════════════════════════════════════════
Architecture similaire à Energy DataSpace:

┌─────────────────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES (4 Types)                                 │
├──────────────┬──────────────┬───────────────┬──────────────────────────────┤
│  Yahoo API   │   ECB XML    │  CSV Files    │    PostgreSQL DB             │
│  (Real-time) │ (Daily Forex)│  (Financials) │   (Transactions)             │
└──────┬───────┴──────┬───────┴───────┬───────┴──────────┬───────────────────┘
       │              │               │                  │
       ▼              ▼               ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         APACHE KAFKA                                        │
│   fame-stocks │ fame-forex │ fame-financials │ fame-transactions           │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 SPARK STRUCTURED STREAMING                                  │
│   ┌─────────────┐  ┌─────────────┐  ┌───────────────────────────┐          │
│   │ Parse JSON  │──│  Anomaly    │──│  Enrichment               │          │
│   │             │  │  Detection  │  │  (alerts, aggregates)     │          │
│   └─────────────┘  └─────────────┘  └───────────────────────────┘          │
└────────────────────────┬────────────────────────┬───────────────────────────┘
                         │                        │
              ┌──────────┴──────────┐   ┌────────┴────────┐
              ▼                     ▼   ▼                 ▼
┌────────────────────────┐  ┌────────────────┐  ┌─────────────────────┐
│      PostgreSQL        │  │    DuckDB      │  │   SEMANTIC LAYER    │
│    Time-Series Data    │  │  (Warehouse)   │  │    RDF / SPARQL     │
│    (Hot Path)          │  │  (Cold Path)   │  │   FAME Ontology     │
└────────────────────────┘  └────────────────┘  └─────────────────────┘
         │
         ▼
┌────────────────────────┐
│       GRAFANA          │
│     Dashboards         │
└────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import logging
import argparse
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType
import requests

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# KAFKA CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')

# Topics Kafka (comme Energy DataSpace)
KAFKA_TOPICS = {
    'stocks': 'fame-stocks',           # Real-time stock prices
    'forex': 'fame-forex',             # Exchange rates
    'financials': 'fame-financials',   # Company financials
    'transactions': 'fame-transactions', # Payment transactions
    'alerts': 'fame-alerts',           # Anomaly alerts
    'aggregates': 'fame-aggregates'    # Aggregated metrics
}

# ═══════════════════════════════════════════════════════════════════════════════
# 100+ STOCK SYMBOLS - REAL DATA
# ═══════════════════════════════════════════════════════════════════════════════

STOCK_SYMBOLS = [
    # ═══════════════════════════════════════════════════════════════════════════
    # US TECH GIANTS (FAANG+)
    # ═══════════════════════════════════════════════════════════════════════════
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA",
    "NFLX", "AMD", "INTC", "ORCL", "CRM", "ADBE", "CSCO", "IBM",
    "PYPL", "SQ", "SHOP", "UBER", "LYFT", "ABNB", "SNAP", "PINS",
    "TWLO", "ZM", "DOCU", "OKTA", "CRWD", "NET", "DDOG", "SNOW",
    
    # ═══════════════════════════════════════════════════════════════════════════
    # US FINANCE & BANKS
    # ═══════════════════════════════════════════════════════════════════════════
    "JPM", "BAC", "WFC", "GS", "MS", "C", "USB", "PNC", "TFC", "COF",
    "AXP", "V", "MA", "DFS", "SYF", "BLK", "SCHW", "SPGI", "ICE",
    
    # ═══════════════════════════════════════════════════════════════════════════
    # S&P 500 MAJOR COMPANIES
    # ═══════════════════════════════════════════════════════════════════════════
    "JNJ", "PG", "UNH", "HD", "DIS", "VZ", "KO", "PEP", "MRK", "PFE",
    "ABBV", "TMO", "COST", "WMT", "CVX", "XOM", "LLY", "MCD", "NKE",
    "QCOM", "TXN", "HON", "UPS", "CAT", "BA", "MMM", "GE", "LMT",
    
    # ═══════════════════════════════════════════════════════════════════════════
    # EUROPEAN BANKS & FINANCE
    # ═══════════════════════════════════════════════════════════════════════════
    "BNP.PA", "SAN.MC", "DBK.DE", "HSBA.L", "INGA.AS", "BBVA.MC",
    "UCG.MI", "ISP.MI", "BARC.L", "LLOY.L", "NWG.L", "ABN.AS",
    
    # ═══════════════════════════════════════════════════════════════════════════
    # EUROPEAN TECH & FINTECH
    # ═══════════════════════════════════════════════════════════════════════════
    "ADYEN.AS", "SAP.DE", "ASML.AS", "NXPI.AS",
    
    # ═══════════════════════════════════════════════════════════════════════════
    # ETFs
    # ═══════════════════════════════════════════════════════════════════════════
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "ARKK"
]

# Forex pairs
FOREX_PAIRS = [
    "USD", "GBP", "JPY", "CHF", "AUD", "CAD", "CNY", "HKD",
    "NZD", "SGD", "KRW", "INR", "MXN", "BRL", "ZAR", "TRY",
    "PLN", "SEK", "NOK", "DKK", "CZK", "HUF", "RON", "BGN"
]


class KafkaFinanceProducer:
    """
    Kafka Producer for FAME Financial Data Space.
    Streams real-time data from 4 sources to Kafka topics.
    """
    
    def __init__(self):
        self.producer = None
        self.running = False
        self._init_kafka()
        
    def _init_kafka(self):
        """Initialize Kafka producer."""
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            logger.info(f"✅ Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
            return True
        except ImportError:
            logger.warning("⚠️ kafka-python not installed - using simulation mode")
            return False
        except Exception as e:
            logger.warning(f"⚠️ Kafka connection failed: {e} - using simulation mode")
            return False
    
    def _fetch_yahoo_stock(self, symbol: str) -> Optional[Dict]:
        """Fetch real stock data from Yahoo Finance API."""
        try:
            import requests
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            params = {'interval': '1m', 'range': '1d'}
            
            response = requests.get(url, params=params, timeout=5, headers={
                'User-Agent': 'FAME-DataSpace/1.0'
            })
            
            if response.status_code == 200:
                data = response.json()
                result = data.get('chart', {}).get('result', [])
                
                if result:
                    meta = result[0].get('meta', {})
                    quote = result[0].get('indicators', {}).get('quote', [{}])[0]
                    closes = [c for c in quote.get('close', []) if c is not None]
                    volumes = [v for v in quote.get('volume', []) if v is not None]
                    
                    if closes:
                        current_price = closes[-1]
                        previous_close = meta.get('previousClose', closes[0])
                        change = current_price - previous_close
                        
                        return {
                            "symbol": symbol,
                            "price": round(current_price, 4),
                            "previous_close": round(previous_close, 4),
                            "change": round(change, 4),
                            "change_percent": round((change / previous_close * 100) if previous_close else 0, 4),
                            "volume": volumes[-1] if volumes else 0,
                            "currency": meta.get('currency', 'USD'),
                            "exchange": meta.get('exchangeName', 'UNKNOWN'),
                            "market_cap": meta.get('marketCap'),
                            "timestamp": datetime.utcnow().isoformat(),
                            "_source": "yahoo_finance",
                            "_real_data": True
                        }
        except Exception as e:
            logger.debug(f"Yahoo fetch error for {symbol}: {e}")
        return None
    
    def _fetch_ecb_forex(self) -> List[Dict]:
        """Fetch real forex rates from ECB XML."""
        try:
            import requests
            import xml.etree.ElementTree as ET
            
            url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                ns = {'eurofxref': 'http://www.ecb.int/vocabulary/2002-08-01/eurofxref'}
                
                cube_time = root.find('.//eurofxref:Cube[@time]', ns)
                if cube_time:
                    ref_date = cube_time.get('time')
                    rates = []
                    
                    for cube in cube_time.findall('eurofxref:Cube', ns):
                        currency = cube.get('currency')
                        rate = float(cube.get('rate'))
                        
                        rates.append({
                            "base_currency": "EUR",
                            "target_currency": currency,
                            "rate": rate,
                            "inverse_rate": round(1 / rate, 6),
                            "reference_date": ref_date,
                            "timestamp": datetime.utcnow().isoformat(),
                            "_source": "ecb_xml",
                            "_real_data": True
                        })
                    
                    return rates
        except Exception as e:
            logger.debug(f"ECB fetch error: {e}")
        return []
    
    def _generate_transaction(self) -> Dict:
        """Generate realistic transaction data."""
        import uuid
        
        tx_types = ['PAYMENT', 'TRANSFER', 'WITHDRAWAL', 'DEPOSIT', 'REFUND']
        statuses = ['COMPLETED', 'COMPLETED', 'COMPLETED', 'PENDING', 'FAILED']
        currencies = ['EUR', 'USD', 'GBP', 'CHF']
        
        amount = round(random.uniform(10, 50000), 2)
        
        return {
            "transaction_id": str(uuid.uuid4()),
            "amount": amount,
            "currency": random.choice(currencies),
            "transaction_type": random.choice(tx_types),
            "status": random.choice(statuses),
            "sender_id": f"SENDER_{random.randint(1000, 9999)}",
            "receiver_id": f"RECEIVER_{random.randint(1000, 9999)}",
            "timestamp": datetime.utcnow().isoformat(),
            "_source": "transaction_generator",
            "_real_data": False
        }
    
    def _detect_anomaly(self, data: Dict, data_type: str) -> Optional[Dict]:
        """Detect anomalies in streaming data."""
        alert = None
        
        if data_type == 'stock':
            change_pct = abs(data.get('change_percent', 0))
            if change_pct > 5:
                alert = {
                    "alert_type": "PRICE_SPIKE",
                    "severity": "HIGH" if change_pct > 10 else "MEDIUM",
                    "symbol": data.get('symbol'),
                    "message": f"Price change of {change_pct:.2f}% detected",
                    "value": change_pct,
                    "threshold": 5,
                    "timestamp": datetime.utcnow().isoformat()
                }
        
        elif data_type == 'transaction':
            amount = data.get('amount', 0)
            if amount > 10000:
                alert = {
                    "alert_type": "LARGE_TRANSACTION",
                    "severity": "HIGH" if amount > 25000 else "MEDIUM",
                    "transaction_id": data.get('transaction_id'),
                    "message": f"Large transaction of {amount:.2f} {data.get('currency')}",
                    "value": amount,
                    "threshold": 10000,
                    "timestamp": datetime.utcnow().isoformat()
                }
        
        return alert
    
    def send_to_kafka(self, topic: str, key: str, value: Dict):
        """Send message to Kafka topic."""
        if self.producer:
            try:
                future = self.producer.send(topic, key=key, value=value)
                future.get(timeout=10)
                return True
            except Exception as e:
                logger.error(f"Kafka send error: {e}")
        return False
    
    def stream_stocks(self, interval: int = 10):
        """Stream stock data to Kafka."""
        logger.info(f"📈 Starting STOCK streaming ({len(STOCK_SYMBOLS)} symbols)...")
        
        iteration = 0
        while self.running:
            iteration += 1
            batch_start = time.time()
            success_count = 0
            alerts_sent = 0
            
            # Fetch stocks in batches
            for symbol in STOCK_SYMBOLS:
                if not self.running:
                    break
                    
                stock_data = self._fetch_yahoo_stock(symbol)
                
                if stock_data:
                    # Send to Kafka
                    if self.send_to_kafka(KAFKA_TOPICS['stocks'], symbol, stock_data):
                        success_count += 1
                    
                    # Check for anomalies
                    alert = self._detect_anomaly(stock_data, 'stock')
                    if alert:
                        self.send_to_kafka(KAFKA_TOPICS['alerts'], symbol, alert)
                        alerts_sent += 1
                        logger.warning(f"🚨 ALERT: {alert['message']}")
                
                # Small delay between requests to avoid rate limiting
                time.sleep(0.1)
            
            batch_time = time.time() - batch_start
            logger.info(f"📈 Iteration {iteration}: {success_count}/{len(STOCK_SYMBOLS)} stocks | {alerts_sent} alerts | {batch_time:.1f}s")
            
            # Wait for next interval
            sleep_time = max(0, interval - batch_time)
            time.sleep(sleep_time)
    
    def stream_forex(self, interval: int = 60):
        """Stream forex data to Kafka."""
        logger.info("💱 Starting FOREX streaming...")
        
        iteration = 0
        while self.running:
            iteration += 1
            
            forex_data = self._fetch_ecb_forex()
            
            if forex_data:
                for rate in forex_data:
                    if not self.running:
                        break
                    self.send_to_kafka(
                        KAFKA_TOPICS['forex'],
                        rate['target_currency'],
                        rate
                    )
                
                logger.info(f"💱 Iteration {iteration}: {len(forex_data)} forex rates sent")
            
            time.sleep(interval)
    
    def stream_transactions(self, interval: int = 2):
        """Stream transaction data to Kafka."""
        logger.info("💳 Starting TRANSACTION streaming...")
        
        iteration = 0
        while self.running:
            iteration += 1
            
            # Generate batch of transactions
            batch_size = random.randint(1, 5)
            alerts_sent = 0
            
            for _ in range(batch_size):
                if not self.running:
                    break
                    
                tx = self._generate_transaction()
                self.send_to_kafka(
                    KAFKA_TOPICS['transactions'],
                    tx['transaction_id'],
                    tx
                )
                
                # Check for anomalies
                alert = self._detect_anomaly(tx, 'transaction')
                if alert:
                    self.send_to_kafka(KAFKA_TOPICS['alerts'], tx['transaction_id'], alert)
                    alerts_sent += 1
            
            if iteration % 10 == 0:
                logger.info(f"💳 Iteration {iteration}: {batch_size} transactions | {alerts_sent} alerts")
            
            time.sleep(interval)
    
    def start_streaming(self, 
                       stock_interval: int = 30,
                       forex_interval: int = 300,
                       tx_interval: int = 2):
        """Start all streaming threads."""
        self.running = True
        
        print("""
╔═══════════════════════════════════════════════════════════════════════════════╗
║           🚀 FAME KAFKA REAL-TIME STREAMING STARTED                           ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║                                                                               ║
║  📊 DATA SOURCES:                                                             ║
║     • Yahoo Finance API  →  {stocks} stocks (real-time)                       ║
║     • ECB XML Feed       →  {forex} currencies (daily)                        ║
║     • Transaction Gen    →  Continuous stream                                 ║
║                                                                               ║
║  📡 KAFKA TOPICS:                                                             ║
║     • fame-stocks        →  Real-time stock prices                            ║
║     • fame-forex         →  Exchange rates                                    ║
║     • fame-transactions  →  Payment transactions                              ║
║     • fame-alerts        →  Anomaly detection alerts                          ║
║                                                                               ║
║  ⏱️  INTERVALS:                                                               ║
║     • Stocks: every {s_int}s | Forex: every {f_int}s | Transactions: {t_int}s ║
║                                                                               ║
║  📺 MONITORING:                                                               ║
║     • Kafka UI:  http://localhost:8080                                        ║
║     • Grafana:   http://localhost:3000                                        ║
║                                                                               ║
║  Press Ctrl+C to stop                                                         ║
╚═══════════════════════════════════════════════════════════════════════════════╝
""".format(
            stocks=len(STOCK_SYMBOLS),
            forex=len(FOREX_PAIRS),
            s_int=stock_interval,
            f_int=forex_interval,
            t_int=tx_interval
        ))
        
        threads = [
            threading.Thread(target=self.stream_stocks, args=(stock_interval,), daemon=True),
            threading.Thread(target=self.stream_forex, args=(forex_interval,), daemon=True),
            threading.Thread(target=self.stream_transactions, args=(tx_interval,), daemon=True)
        ]
        
        for t in threads:
            t.start()
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n⏹️ Stopping streaming...")
            self.running = False
            
        if self.producer:
            self.producer.close()
        
        logger.info("✅ Streaming stopped")
    
    def stop(self):
        """Stop streaming."""
        self.running = False


class KafkaFinanceConsumer:
    """
    Kafka Consumer for FAME - writes to PostgreSQL for Grafana.
    """
    
    def __init__(self):
        self.consumer = None
        self.pg_conn = None
        self.running = False
        self._init_kafka()
        self._init_postgres()
    
    def _init_kafka(self):
        """Initialize Kafka consumer."""
        try:
            from kafka import KafkaConsumer
            self.consumer = KafkaConsumer(
                *KAFKA_TOPICS.values(),
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='fame-grafana-consumer'
            )
            logger.info("✅ Kafka Consumer connected")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Kafka Consumer error: {e}")
            return False
    
    def _init_postgres(self):
        """Initialize PostgreSQL connection."""
        try:
            import psycopg2
            self.pg_conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'localhost'),
                port=int(os.getenv('POSTGRES_PORT', 5432)),
                database=os.getenv('POSTGRES_DB', 'fame_transactions'),
                user=os.getenv('POSTGRES_USER', 'fame_user'),
                password=os.getenv('POSTGRES_PASSWORD', 'fame_password')
            )
            self.pg_conn.autocommit = True
            logger.info("✅ PostgreSQL connected")
            return True
        except Exception as e:
            logger.warning(f"⚠️ PostgreSQL error: {e}")
            return False
    
    def _write_to_postgres(self, topic: str, data: Dict):
        """Write Kafka message to PostgreSQL."""
        if not self.pg_conn:
            return
        
        try:
            cursor = self.pg_conn.cursor()
            
            if topic == KAFKA_TOPICS['stocks']:
                cursor.execute("""
                    INSERT INTO fame_streaming.stock_quotes 
                    (symbol, price, volume, change_percent, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    data.get('symbol'),
                    data.get('price'),
                    data.get('volume'),
                    data.get('change_percent'),
                    data.get('timestamp')
                ))
            
            elif topic == KAFKA_TOPICS['alerts']:
                cursor.execute("""
                    INSERT INTO fame_streaming.alerts 
                    (alert_type, symbol, message, severity, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    data.get('alert_type'),
                    data.get('symbol', data.get('transaction_id')),
                    data.get('message'),
                    data.get('severity'),
                    data.get('timestamp')
                ))
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"PostgreSQL write error: {e}")
    
    def start_consuming(self):
        """Start consuming from Kafka and writing to PostgreSQL."""
        if not self.consumer:
            logger.error("❌ Kafka Consumer not initialized")
            return
        
        self.running = True
        logger.info("📥 Starting Kafka Consumer → PostgreSQL...")
        
        msg_count = 0
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                topic = message.topic
                data = message.value
                
                self._write_to_postgres(topic, data)
                msg_count += 1
                
                if msg_count % 100 == 0:
                    logger.info(f"📥 Processed {msg_count} messages")
                    
        except KeyboardInterrupt:
            logger.info("\n⏹️ Consumer stopped")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.pg_conn:
                self.pg_conn.close()


def push_to_prometheus(symbol, price, volume):
    data = f"stock_price{{symbol=\"{symbol}\"}} {price}\n"
    data += f"stock_volume{{symbol=\"{symbol}\"}} {volume}\n"
    response = requests.post(PROMETHEUS_PUSHGATEWAY, data=data, headers={"Content-Type": "text/plain"})
    if response.status_code == 200:
        print(f"Pushed metrics for {symbol} to Prometheus")
    else:
        print(f"Failed to push metrics for {symbol}: {response.status_code}")

def process_stream():
    spark = SparkSession.builder \
        .appName("KafkaStockStreaming") \
        .getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    stock_data = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    def foreach_batch_function(batch_df, batch_id):
        for row in batch_df.collect():
            push_to_prometheus(row.symbol, row.price, row.volume)

    query = stock_data.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()

def main():
    parser = argparse.ArgumentParser(description='FAME Kafka Financial Streaming')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'both'], 
                       default='producer', help='Run mode')
    parser.add_argument('--stock-interval', type=int, default=30,
                       help='Stock fetch interval in seconds')
    parser.add_argument('--forex-interval', type=int, default=300,
                       help='Forex fetch interval in seconds')
    parser.add_argument('--tx-interval', type=int, default=2,
                       help='Transaction generation interval in seconds')
    
    args = parser.parse_args()
    
    if args.mode in ['producer', 'both']:
        producer = KafkaFinanceProducer()
        
        if args.mode == 'both':
            # Start consumer in background thread
            consumer = KafkaFinanceConsumer()
            consumer_thread = threading.Thread(
                target=consumer.start_consuming, 
                daemon=True
            )
            consumer_thread.start()
        
        producer.start_streaming(
            stock_interval=args.stock_interval,
            forex_interval=args.forex_interval,
            tx_interval=args.tx_interval
        )
    
    elif args.mode == 'consumer':
        consumer = KafkaFinanceConsumer()
        consumer.start_consuming()


if __name__ == '__main__':
    main()
