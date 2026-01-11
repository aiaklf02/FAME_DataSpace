"""
═══════════════════════════════════════════════════════════════════════════════
FAME Financial Data Space - REAL-TIME STREAMING SERVICE
═══════════════════════════════════════════════════════════════════════════════
This is the MAIN streaming service that runs continuously to:
1. Fetch REAL-TIME stock data from Yahoo Finance API
2. Stream transactions from PostgreSQL
3. Push ALL metrics to Prometheus for Grafana dashboards
4. Detect anomalies DYNAMICALLY (not static!)
5. Send data to Kafka topics

Run with: python streaming/realtime_streaming.py
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import logging
import threading
import random
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import deque
import uuid

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
PROMETHEUS_PUSHGATEWAY = os.getenv('PROMETHEUS_PUSHGATEWAY', 'http://localhost:9091')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'fame_transactions')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'fame_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'fame_password')

# Kafka Topics
TOPICS = {
    'stocks': 'fame-stocks',
    'forex': 'fame-forex',
    'transactions': 'fame-transactions',
    'alerts': 'fame-alerts',
    'anomalies': 'fame-anomalies'
}

# Stock symbols to stream (reduced for faster updates)
STOCK_SYMBOLS = [
    # Major Tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", 
    # Finance
    "JPM", "GS", "V", "MA",
    # Others
    "DIS", "NFLX", "AMD", "INTC",
    # ETFs
    "SPY", "QQQ"
]

# ═══════════════════════════════════════════════════════════════════════════════
# PROMETHEUS METRICS PUSHER
# ═══════════════════════════════════════════════════════════════════════════════

class PrometheusMetrics:
    """Push real-time metrics to Prometheus Pushgateway for Grafana."""
    
    def __init__(self, pushgateway_url: str = PROMETHEUS_PUSHGATEWAY):
        self.pushgateway_url = pushgateway_url
        self.job_name = "fame_realtime"
        logger.info(f"📊 Prometheus metrics initialized: {pushgateway_url}")
    
    def _push_metrics(self, metrics: str, grouping_key: str = "default"):
        """Push metrics to Prometheus Pushgateway."""
        try:
            import requests
            url = f"{self.pushgateway_url}/metrics/job/{self.job_name}/instance/{grouping_key}"
            response = requests.post(url, data=metrics, headers={
                'Content-Type': 'text/plain'
            }, timeout=5)
            return response.status_code in [200, 202]
        except Exception as e:
            logger.debug(f"Pushgateway error: {e}")
            return False
    
    def push_stock_metrics(self, symbol: str, price: float, change_pct: float, volume: int):
        """Push stock metrics to Prometheus."""
        metrics = f"""# HELP fame_stock_price Current stock price in USD
# TYPE fame_stock_price gauge
fame_stock_price{{symbol="{symbol}"}} {price}
# HELP fame_stock_change_percent Stock price change percentage
# TYPE fame_stock_change_percent gauge
fame_stock_change_percent{{symbol="{symbol}"}} {change_pct}
# HELP fame_stock_volume Trading volume
# TYPE fame_stock_volume gauge
fame_stock_volume{{symbol="{symbol}"}} {volume}
# HELP fame_stock_update_timestamp Last update timestamp
# TYPE fame_stock_update_timestamp gauge
fame_stock_update_timestamp{{symbol="{symbol}"}} {time.time()}
"""
        return self._push_metrics(metrics, f"stock_{symbol}")
    
    def push_transaction_metrics(self, count: int, total_amount: float, 
                                  anomaly_count: int, tx_per_second: float):
        """Push transaction metrics to Prometheus."""
        metrics = f"""# HELP fame_transactions_total Total transactions processed
# TYPE fame_transactions_total counter
fame_transactions_total {count}
# HELP fame_transaction_amount_total Total transaction amount
# TYPE fame_transaction_amount_total counter
fame_transaction_amount_total {total_amount}
# HELP fame_anomalies_detected Number of anomalies detected
# TYPE fame_anomalies_detected gauge
fame_anomalies_detected {anomaly_count}
# HELP fame_transactions_per_second Transactions per second
# TYPE fame_transactions_per_second gauge
fame_transactions_per_second {tx_per_second}
# HELP fame_last_update Last metrics update timestamp
# TYPE fame_last_update gauge
fame_last_update {time.time()}
"""
        return self._push_metrics(metrics, "transactions")
    
    def push_anomaly(self, anomaly_type: str, symbol: str, value: float, 
                     severity: str, details: str):
        """Push anomaly detection metrics."""
        metrics = f"""# HELP fame_anomaly_detected Anomaly detection event
# TYPE fame_anomaly_detected gauge
fame_anomaly_detected{{type="{anomaly_type}",symbol="{symbol}",severity="{severity}"}} {value}
# HELP fame_anomaly_value Anomaly value
# TYPE fame_anomaly_value gauge
fame_anomaly_value{{type="{anomaly_type}",symbol="{symbol}"}} {value}
# HELP fame_anomaly_timestamp Anomaly detection timestamp  
# TYPE fame_anomaly_timestamp gauge
fame_anomaly_timestamp{{type="{anomaly_type}",symbol="{symbol}"}} {time.time()}
"""
        return self._push_metrics(metrics, f"anomaly_{symbol}_{int(time.time())}")
    
    def push_streaming_status(self, stocks_active: bool, transactions_active: bool,
                               kafka_connected: bool, messages_sent: int):
        """Push streaming service status."""
        metrics = f"""# HELP fame_streaming_stocks_active Stock streaming status
# TYPE fame_streaming_stocks_active gauge
fame_streaming_stocks_active {1 if stocks_active else 0}
# HELP fame_streaming_transactions_active Transaction streaming status
# TYPE fame_streaming_transactions_active gauge
fame_streaming_transactions_active {1 if transactions_active else 0}
# HELP fame_kafka_connected Kafka connection status
# TYPE fame_kafka_connected gauge
fame_kafka_connected {1 if kafka_connected else 0}
# HELP fame_kafka_messages_sent Total Kafka messages sent
# TYPE fame_kafka_messages_sent counter
fame_kafka_messages_sent {messages_sent}
"""
        return self._push_metrics(metrics, "status")


# ═══════════════════════════════════════════════════════════════════════════════
# DYNAMIC ANOMALY DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class DynamicAnomalyDetector:
    """
    REAL-TIME anomaly detection using statistical methods.
    NOT static! Continuously learns from streaming data.
    """
    
    def __init__(self, window_size: int = 50):
        self.window_size = window_size
        self.price_history: Dict[str, deque] = {}
        self.volume_history: Dict[str, deque] = {}
        self.transaction_amounts: deque = deque(maxlen=200)
        self.anomaly_count = 0
        self.detected_anomalies: List[Dict] = []
        
    def update_stock(self, symbol: str, price: float, volume: int) -> List[Dict]:
        """Update stock data and detect anomalies."""
        anomalies = []
        
        # Initialize history if needed
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=self.window_size)
            self.volume_history[symbol] = deque(maxlen=self.window_size)
        
        prices = self.price_history[symbol]
        volumes = self.volume_history[symbol]
        
        # Need enough data for statistics
        if len(prices) >= 10:
            # Z-Score anomaly detection for price
            mean_price = statistics.mean(prices)
            stdev_price = statistics.stdev(prices) if len(prices) > 1 else 0
            
            if stdev_price > 0:
                z_score = (price - mean_price) / stdev_price
                
                # Detect price spike (Z-score > 2.5)
                if abs(z_score) > 2.5:
                    anomaly = {
                        'type': 'PRICE_SPIKE',
                        'symbol': symbol,
                        'value': price,
                        'z_score': round(z_score, 2),
                        'mean': round(mean_price, 2),
                        'stdev': round(stdev_price, 4),
                        'severity': 'HIGH' if abs(z_score) > 3.5 else 'MEDIUM',
                        'message': f"{symbol} price {price:.2f} is {abs(z_score):.1f} std devs from mean {mean_price:.2f}",
                        'timestamp': datetime.utcnow().isoformat(),
                        'detection_method': 'z-score'
                    }
                    anomalies.append(anomaly)
                    self.anomaly_count += 1
                    self.detected_anomalies.append(anomaly)
            
            # Volume spike detection
            if len(volumes) >= 10:
                mean_vol = statistics.mean(volumes)
                if mean_vol > 0 and volume > mean_vol * 3:  # 3x average volume
                    anomaly = {
                        'type': 'VOLUME_SPIKE',
                        'symbol': symbol,
                        'value': volume,
                        'average_volume': int(mean_vol),
                        'ratio': round(volume / mean_vol, 2),
                        'severity': 'MEDIUM',
                        'message': f"{symbol} volume {volume:,} is {volume/mean_vol:.1f}x average",
                        'timestamp': datetime.utcnow().isoformat(),
                        'detection_method': 'threshold'
                    }
                    anomalies.append(anomaly)
                    self.anomaly_count += 1
                    self.detected_anomalies.append(anomaly)
        
        # Add to history
        prices.append(price)
        volumes.append(volume)
        
        return anomalies
    
    def update_transaction(self, amount: float) -> Optional[Dict]:
        """Detect transaction anomalies."""
        self.transaction_amounts.append(amount)
        
        if len(self.transaction_amounts) >= 20:
            mean_amount = statistics.mean(self.transaction_amounts)
            stdev_amount = statistics.stdev(self.transaction_amounts) if len(self.transaction_amounts) > 1 else 0
            
            # Large transaction detection
            if amount > mean_amount + (3 * stdev_amount) or amount > 10000:
                anomaly = {
                    'type': 'LARGE_TRANSACTION',
                    'value': amount,
                    'mean': round(mean_amount, 2),
                    'threshold': round(mean_amount + (3 * stdev_amount), 2),
                    'severity': 'HIGH' if amount > 25000 else 'MEDIUM',
                    'message': f"Large transaction: €{amount:,.2f} (avg: €{mean_amount:,.2f})",
                    'timestamp': datetime.utcnow().isoformat(),
                    'detection_method': 'z-score'
                }
                self.anomaly_count += 1
                self.detected_anomalies.append(anomaly)
                return anomaly
        
        return None
    
    def get_recent_anomalies(self, limit: int = 20) -> List[Dict]:
        """Get most recent anomalies."""
        return list(self.detected_anomalies)[-limit:]


# ═══════════════════════════════════════════════════════════════════════════════
# KAFKA PRODUCER
# ═══════════════════════════════════════════════════════════════════════════════

class KafkaProducerWrapper:
    """Wrapper for Kafka producer with connection handling."""
    
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.connected = False
        self.messages_sent = 0
        self._connect()
    
    def _connect(self):
        """Connect to Kafka."""
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                retry_backoff_ms=100
            )
            self.connected = True
            logger.info(f"✅ Kafka connected: {self.bootstrap_servers}")
        except ImportError:
            logger.error("❌ kafka-python not installed! Run: pip install kafka-python")
            self.connected = False
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            self.connected = False
    
    def send(self, topic: str, key: str, value: Dict) -> bool:
        """Send message to Kafka topic."""
        if not self.producer or not self.connected:
            return False
        
        try:
            future = self.producer.send(topic, key=key, value=value)
            future.get(timeout=5)
            self.messages_sent += 1
            return True
        except Exception as e:
            logger.error(f"Kafka send error: {e}")
            return False
    
    def flush(self):
        """Flush pending messages."""
        if self.producer:
            self.producer.flush()


# ═══════════════════════════════════════════════════════════════════════════════
# YAHOO FINANCE REAL-TIME FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

class YahooFinanceStreamer:
    """Fetch real-time stock data from Yahoo Finance API."""
    
    def __init__(self):
        self.session = None
        self._init_session()
    
    def _init_session(self):
        """Initialize requests session."""
        try:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'FAME-DataSpace/2.0 (Financial Analytics)'
            })
        except ImportError:
            logger.error("requests not installed!")
    
    def fetch_stock(self, symbol: str) -> Optional[Dict]:
        """Fetch real-time stock data for a symbol."""
        if not self.session:
            return None
        
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            params = {'interval': '1m', 'range': '1d'}
            
            response = self.session.get(url, params=params, timeout=10)
            
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
                        previous_close = meta.get('previousClose', closes[0] if closes else current_price)
                        
                        if previous_close and previous_close > 0:
                            change = current_price - previous_close
                            change_pct = (change / previous_close) * 100
                        else:
                            change = 0
                            change_pct = 0
                        
                        return {
                            "symbol": symbol,
                            "price": round(current_price, 4),
                            "previous_close": round(previous_close, 4) if previous_close else None,
                            "change": round(change, 4),
                            "change_percent": round(change_pct, 4),
                            "volume": volumes[-1] if volumes else 0,
                            "day_high": meta.get('regularMarketDayHigh'),
                            "day_low": meta.get('regularMarketDayLow'),
                            "currency": meta.get('currency', 'USD'),
                            "exchange": meta.get('exchangeName', 'UNKNOWN'),
                            "market_state": meta.get('marketState', 'UNKNOWN'),
                            "timestamp": datetime.utcnow().isoformat(),
                            "_source": "yahoo_finance_api",
                            "_real_data": True
                        }
        except Exception as e:
            logger.debug(f"Yahoo API error for {symbol}: {e}")
        
        return None
    
    def fetch_multiple(self, symbols: List[str]) -> List[Dict]:
        """Fetch multiple stocks."""
        results = []
        for symbol in symbols:
            data = self.fetch_stock(symbol)
            if data:
                results.append(data)
            time.sleep(0.05)  # Small delay to avoid rate limiting
        return results


# ═══════════════════════════════════════════════════════════════════════════════
# POSTGRESQL TRANSACTION STREAMER
# ═══════════════════════════════════════════════════════════════════════════════

class PostgresTransactionStreamer:
    """Stream transactions from PostgreSQL or generate realistic ones."""
    
    def __init__(self):
        self.conn = None
        self.last_tx_id = 0
        self._connect()
    
    def _connect(self):
        """Connect to PostgreSQL."""
        try:
            import psycopg2
            self.conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("✅ PostgreSQL connected")
        except ImportError:
            logger.warning("⚠️ psycopg2 not installed - using simulated transactions")
        except Exception as e:
            logger.warning(f"⚠️ PostgreSQL connection failed: {e} - using simulated transactions")
    
    def fetch_new_transactions(self, limit: int = 10) -> List[Dict]:
        """Fetch new transactions from database."""
        if self.conn:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, amount, currency, transaction_type, status,
                               sender_id, receiver_id, timestamp
                        FROM transactions
                        WHERE id > %s
                        ORDER BY id ASC
                        LIMIT %s
                    """, (self.last_tx_id, limit))
                    
                    rows = cur.fetchall()
                    transactions = []
                    
                    for row in rows:
                        tx = {
                            "transaction_id": str(row[0]),
                            "amount": float(row[1]),
                            "currency": row[2],
                            "transaction_type": row[3],
                            "status": row[4],
                            "sender_id": row[5],
                            "receiver_id": row[6],
                            "timestamp": row[7].isoformat() if row[7] else datetime.utcnow().isoformat(),
                            "_source": "postgresql",
                            "_real_data": True
                        }
                        transactions.append(tx)
                        self.last_tx_id = max(self.last_tx_id, row[0])
                    
                    return transactions
            except Exception as e:
                logger.debug(f"PostgreSQL query error: {e}")
        
        # Generate simulated transaction
        return [self._generate_transaction()]
    
    def _generate_transaction(self) -> Dict:
        """Generate a realistic transaction."""
        tx_types = ['PAYMENT', 'TRANSFER', 'WITHDRAWAL', 'DEPOSIT', 'REFUND']
        statuses = ['COMPLETED', 'COMPLETED', 'COMPLETED', 'COMPLETED', 'PENDING', 'FAILED']
        currencies = ['EUR', 'USD', 'GBP', 'CHF']
        
        # Generate realistic amounts (most small, some large)
        if random.random() < 0.05:  # 5% chance of large transaction
            amount = round(random.uniform(5000, 50000), 2)
        else:
            amount = round(random.uniform(10, 2000), 2)
        
        return {
            "transaction_id": str(uuid.uuid4()),
            "amount": amount,
            "currency": random.choice(currencies),
            "transaction_type": random.choice(tx_types),
            "status": random.choice(statuses),
            "sender_id": f"USER_{random.randint(10000, 99999)}",
            "receiver_id": f"USER_{random.randint(10000, 99999)}",
            "timestamp": datetime.utcnow().isoformat(),
            "_source": "transaction_simulator",
            "_real_data": False
        }


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN STREAMING SERVICE
# ═══════════════════════════════════════════════════════════════════════════════

class FAMERealtimeStreaming:
    """
    Main real-time streaming service for FAME Data Space.
    
    Runs continuously to:
    1. Stream Yahoo Finance stock data to Kafka
    2. Stream PostgreSQL transactions to Kafka
    3. Push ALL metrics to Prometheus for Grafana
    4. Detect anomalies dynamically
    """
    
    def __init__(self, stock_interval: int = 15, tx_interval: int = 2):
        self.stock_interval = stock_interval  # seconds between stock fetches
        self.tx_interval = tx_interval  # seconds between transaction fetches
        
        # Components
        self.kafka = KafkaProducerWrapper()
        self.prometheus = PrometheusMetrics()
        self.yahoo = YahooFinanceStreamer()
        self.postgres = PostgresTransactionStreamer()
        self.anomaly_detector = DynamicAnomalyDetector()
        
        # State
        self.running = False
        self.stocks_active = False
        self.transactions_active = False
        self.total_stocks_sent = 0
        self.total_transactions_sent = 0
        self.total_anomalies = 0
        self.start_time = None
        
        logger.info("═══════════════════════════════════════════════════════════════")
        logger.info("🚀 FAME Real-Time Streaming Service Initialized")
        logger.info("═══════════════════════════════════════════════════════════════")
        logger.info(f"   Kafka: {KAFKA_BOOTSTRAP}")
        logger.info(f"   Prometheus: {PROMETHEUS_PUSHGATEWAY}")
        logger.info(f"   Stock Symbols: {len(STOCK_SYMBOLS)}")
        logger.info(f"   Stock Interval: {stock_interval}s")
        logger.info(f"   Transaction Interval: {tx_interval}s")
        logger.info("═══════════════════════════════════════════════════════════════")
    
    def _stream_stocks(self):
        """Stock streaming thread."""
        logger.info("📈 Stock streaming thread started")
        self.stocks_active = True
        
        while self.running:
            try:
                cycle_start = time.time()
                stocks_this_cycle = 0
                anomalies_this_cycle = 0
                
                # Fetch all stocks
                stocks = self.yahoo.fetch_multiple(STOCK_SYMBOLS)
                
                for stock in stocks:
                    if not self.running:
                        break
                    
                    symbol = stock['symbol']
                    price = stock['price']
                    volume = stock.get('volume', 0)
                    change_pct = stock.get('change_percent', 0)
                    
                    # Send to Kafka
                    if self.kafka.send(TOPICS['stocks'], symbol, stock):
                        stocks_this_cycle += 1
                        self.total_stocks_sent += 1
                    
                    # Push to Prometheus
                    self.prometheus.push_stock_metrics(symbol, price, change_pct, volume)
                    
                    # Detect anomalies
                    anomalies = self.anomaly_detector.update_stock(symbol, price, volume)
                    
                    for anomaly in anomalies:
                        self.kafka.send(TOPICS['anomalies'], symbol, anomaly)
                        self.prometheus.push_anomaly(
                            anomaly['type'], symbol, anomaly['value'],
                            anomaly['severity'], anomaly['message']
                        )
                        anomalies_this_cycle += 1
                        self.total_anomalies += 1
                        logger.warning(f"🚨 ANOMALY: {anomaly['message']}")
                
                cycle_time = time.time() - cycle_start
                
                if stocks_this_cycle > 0:
                    logger.info(f"📈 Stocks: {stocks_this_cycle} sent | "
                               f"Anomalies: {anomalies_this_cycle} | "
                               f"Total: {self.total_stocks_sent} | "
                               f"Cycle: {cycle_time:.1f}s")
                
                # Wait for next cycle
                sleep_time = max(0, self.stock_interval - cycle_time)
                if sleep_time > 0 and self.running:
                    time.sleep(sleep_time)
                    
            except Exception as e:
                logger.error(f"Stock streaming error: {e}")
                time.sleep(5)
        
        self.stocks_active = False
        logger.info("📈 Stock streaming thread stopped")
    
    def _stream_transactions(self):
        """Transaction streaming thread."""
        logger.info("💳 Transaction streaming thread started")
        self.transactions_active = True
        
        tx_count = 0
        tx_total_amount = 0
        last_status_time = time.time()
        
        while self.running:
            try:
                # Fetch transactions
                transactions = self.postgres.fetch_new_transactions(limit=5)
                
                for tx in transactions:
                    if not self.running:
                        break
                    
                    amount = tx['amount']
                    tx_id = tx['transaction_id']
                    
                    # Send to Kafka
                    if self.kafka.send(TOPICS['transactions'], tx_id, tx):
                        self.total_transactions_sent += 1
                        tx_count += 1
                        tx_total_amount += amount
                    
                    # Detect anomalies
                    anomaly = self.anomaly_detector.update_transaction(amount)
                    if anomaly:
                        self.kafka.send(TOPICS['anomalies'], tx_id, anomaly)
                        self.prometheus.push_anomaly(
                            anomaly['type'], 'transaction', amount,
                            anomaly['severity'], anomaly['message']
                        )
                        self.total_anomalies += 1
                        logger.warning(f"🚨 ANOMALY: {anomaly['message']}")
                
                # Push transaction metrics every 10 seconds
                if time.time() - last_status_time >= 10:
                    elapsed = time.time() - last_status_time
                    tx_per_sec = tx_count / elapsed if elapsed > 0 else 0
                    
                    self.prometheus.push_transaction_metrics(
                        self.total_transactions_sent,
                        tx_total_amount,
                        self.anomaly_detector.anomaly_count,
                        tx_per_sec
                    )
                    
                    logger.info(f"💳 Transactions: {tx_count} | "
                               f"Amount: €{tx_total_amount:,.2f} | "
                               f"Rate: {tx_per_sec:.1f}/s | "
                               f"Total: {self.total_transactions_sent}")
                    
                    tx_count = 0
                    tx_total_amount = 0
                    last_status_time = time.time()
                
                time.sleep(self.tx_interval)
                
            except Exception as e:
                logger.error(f"Transaction streaming error: {e}")
                time.sleep(5)
        
        self.transactions_active = False
        logger.info("💳 Transaction streaming thread stopped")
    
    def _push_status(self):
        """Push service status to Prometheus."""
        while self.running:
            try:
                self.prometheus.push_streaming_status(
                    self.stocks_active,
                    self.transactions_active,
                    self.kafka.connected,
                    self.kafka.messages_sent
                )
                time.sleep(5)
            except Exception as e:
                logger.debug(f"Status push error: {e}")
                time.sleep(5)
    
    def start(self):
        """Start all streaming threads."""
        logger.info("\n" + "═" * 60)
        logger.info("🚀 STARTING FAME REAL-TIME STREAMING")
        logger.info("═" * 60)
        
        self.running = True
        self.start_time = datetime.now()
        
        # Start threads
        threads = [
            threading.Thread(target=self._stream_stocks, name="StockStreamer", daemon=True),
            threading.Thread(target=self._stream_transactions, name="TxStreamer", daemon=True),
            threading.Thread(target=self._push_status, name="StatusPusher", daemon=True)
        ]
        
        for t in threads:
            t.start()
            logger.info(f"   ✅ Started: {t.name}")
        
        logger.info("\n📊 Monitoring URLs:")
        logger.info("   Grafana:    http://localhost:3000 (admin/admin123)")
        logger.info("   Kafka UI:   http://localhost:8080")
        logger.info("   Prometheus: http://localhost:9090")
        logger.info("\n⏹️  Press Ctrl+C to stop\n")
        
        # Keep main thread alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n⏹️ Stopping streaming service...")
            self.stop()
    
    def stop(self):
        """Stop all streaming."""
        self.running = False
        self.kafka.flush()
        
        # Print summary
        runtime = datetime.now() - self.start_time if self.start_time else timedelta(0)
        
        logger.info("\n" + "═" * 60)
        logger.info("📊 STREAMING SESSION SUMMARY")
        logger.info("═" * 60)
        logger.info(f"   Runtime:          {runtime}")
        logger.info(f"   Stocks Sent:      {self.total_stocks_sent}")
        logger.info(f"   Transactions:     {self.total_transactions_sent}")
        logger.info(f"   Anomalies:        {self.total_anomalies}")
        logger.info(f"   Kafka Messages:   {self.kafka.messages_sent}")
        logger.info("═" * 60)
        
        # Show recent anomalies
        recent = self.anomaly_detector.get_recent_anomalies(10)
        if recent:
            logger.info("\n🚨 Recent Anomalies Detected:")
            for a in recent:
                logger.info(f"   [{a['severity']}] {a['type']}: {a['message']}")
        
        logger.info("\n✅ Streaming service stopped")


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='FAME Real-Time Streaming Service')
    parser.add_argument('--stock-interval', type=int, default=15,
                        help='Seconds between stock updates (default: 15)')
    parser.add_argument('--tx-interval', type=int, default=2,
                        help='Seconds between transaction batches (default: 2)')
    
    args = parser.parse_args()
    
    # Create and start streaming service
    service = FAMERealtimeStreaming(
        stock_interval=args.stock_interval,
        tx_interval=args.tx_interval
    )
    
    service.start()


if __name__ == "__main__":
    main()
