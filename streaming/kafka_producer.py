"""
FAME Kafka Producer - Real-time Data Publisher
===============================================
Publishes financial data to Kafka topics in real-time.

Topics:
- fame-stocks: Real-time stock prices from Yahoo Finance
- fame-forex: Currency exchange rates
- fame-transactions: Financial transactions
"""

import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try importing Kafka
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("⚠️ kafka-python not installed. Run: pip install kafka-python")


class FAMEKafkaProducer:
    """
    Kafka Producer for FAME Data Space.
    
    Streams real-time financial data to Kafka topics.
    """
    
    # Kafka Topics
    TOPIC_STOCKS = "fame-stocks"
    TOPIC_FOREX = "fame-forex"
    TOPIC_TRANSACTIONS = "fame-transactions"
    TOPIC_ALERTS = "fame-alerts"
    
    def __init__(self, bootstrap_servers: str = "localhost:29092"):
        """Initialize Kafka producer."""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._running = False
        
        if KAFKA_AVAILABLE:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3
                )
                logger.info(f"✅ Kafka Producer connected to {bootstrap_servers}")
            except Exception as e:
                logger.error(f"❌ Failed to connect to Kafka: {e}")
                self.producer = None
        else:
            logger.warning("⚠️ Kafka not available - running in simulation mode")
    
    def send_stock_quote(self, symbol: str, data: Dict) -> bool:
        """
        Send a stock quote to Kafka.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            data: Stock data dictionary
        """
        if not self.producer:
            logger.debug(f"[SIM] Stock: {symbol} = ${data.get('price', 'N/A')}")
            return False
        
        try:
            # Add metadata
            data['_timestamp'] = datetime.now().isoformat()
            data['_topic'] = self.TOPIC_STOCKS
            
            future = self.producer.send(
                self.TOPIC_STOCKS,
                key=symbol,
                value=data
            )
            future.get(timeout=10)
            logger.info(f"📈 Sent to Kafka: {symbol} = ${data.get('current_price', 'N/A')}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to send {symbol}: {e}")
            return False
    
    def send_forex_rate(self, currency_pair: str, data: Dict) -> bool:
        """Send forex rate to Kafka."""
        if not self.producer:
            logger.debug(f"[SIM] Forex: {currency_pair} = {data.get('rate', 'N/A')}")
            return False
        
        try:
            data['_timestamp'] = datetime.now().isoformat()
            data['_topic'] = self.TOPIC_FOREX
            
            future = self.producer.send(
                self.TOPIC_FOREX,
                key=currency_pair,
                value=data
            )
            future.get(timeout=10)
            logger.info(f"💱 Sent to Kafka: {currency_pair} = {data.get('rate', 'N/A')}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to send {currency_pair}: {e}")
            return False
    
    def send_transaction(self, tx_id: str, data: Dict) -> bool:
        """Send transaction to Kafka."""
        if not self.producer:
            logger.debug(f"[SIM] Transaction: {tx_id}")
            return False
        
        try:
            data['_timestamp'] = datetime.now().isoformat()
            data['_topic'] = self.TOPIC_TRANSACTIONS
            
            future = self.producer.send(
                self.TOPIC_TRANSACTIONS,
                key=tx_id,
                value=data
            )
            future.get(timeout=10)
            logger.info(f"💳 Sent to Kafka: Transaction {tx_id[:8]}...")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to send transaction: {e}")
            return False
    
    def send_alert(self, alert_type: str, data: Dict) -> bool:
        """Send alert to Kafka."""
        if not self.producer:
            logger.warning(f"[SIM] Alert: {alert_type} - {data.get('message', '')}")
            return False
        
        try:
            data['alert_type'] = alert_type
            data['_timestamp'] = datetime.now().isoformat()
            
            future = self.producer.send(
                self.TOPIC_ALERTS,
                key=alert_type,
                value=data
            )
            future.get(timeout=10)
            logger.warning(f"🚨 Alert sent: {alert_type}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to send alert: {e}")
            return False
    
    def stream_real_stocks(self, interval_seconds: int = 10):
        """
        Stream real-time stock data from Yahoo Finance.
        
        Runs continuously, fetching and publishing stock prices.
        """
        import sys
        sys.path.append('..')
        from sources.real_data_fetcher import RealDataFetcher
        
        fetcher = RealDataFetcher()
        self._running = True
        
        logger.info(f"🚀 Starting real-time stock streaming (interval: {interval_seconds}s)")
        
        while self._running:
            try:
                # Fetch real stock data
                stocks = fetcher.fetch_real_stocks()
                
                for stock in stocks:
                    self.send_stock_quote(stock['symbol'], stock)
                    
                    # Check for alerts (price change > 5%)
                    if abs(stock.get('change_percent', 0)) > 5:
                        self.send_alert('PRICE_SPIKE', {
                            'symbol': stock['symbol'],
                            'change_percent': stock['change_percent'],
                            'message': f"{stock['symbol']} moved {stock['change_percent']:.2f}%"
                        })
                
                logger.info(f"📊 Streamed {len(stocks)} stock quotes")
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                logger.info("⏹️ Stopping stock streaming...")
                self._running = False
            except Exception as e:
                logger.error(f"❌ Streaming error: {e}")
                time.sleep(5)
    
    def stop(self):
        """Stop streaming."""
        self._running = False
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("✅ Kafka Producer closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


# CLI for testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="FAME Kafka Producer")
    parser.add_argument("--mode", choices=["stocks", "forex", "test"], default="test")
    parser.add_argument("--interval", type=int, default=10, help="Streaming interval in seconds")
    args = parser.parse_args()
    
    with FAMEKafkaProducer() as producer:
        if args.mode == "stocks":
            producer.stream_real_stocks(interval_seconds=args.interval)
        elif args.mode == "test":
            # Send test message
            producer.send_stock_quote("TEST", {
                "symbol": "TEST",
                "current_price": 100.00,
                "change_percent": 1.5,
                "test": True
            })
            print("✅ Test message sent!")
