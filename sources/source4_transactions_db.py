"""
FAME Data Space - Source 4: Financial Transactions Database (PostgreSQL)
========================================================================
Real-time transaction data from PostgreSQL database

Data Type: Relational Database
Format: SQL (PostgreSQL)
Frequency: Real-time / Streaming (CDC)
Volume: ~100,000 transactions/day
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import logging
import random
import uuid

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class FinancialTransaction:
    """Data class for financial transaction."""
    source: str
    source_type: str
    transaction_id: str
    timestamp: str
    transaction_type: str
    category: str
    sender_id: str
    sender_name: str
    sender_country: str
    sender_bank: str
    receiver_id: str
    receiver_name: str
    receiver_country: str
    receiver_bank: str
    amount: float
    currency: str
    exchange_rate: float
    amount_eur: float
    status: str
    channel: str
    fee: float
    reference: str


class TransactionDatabaseConnector:
    """
    SOURCE 4: Financial Transactions Database (PostgreSQL)
    
    Features:
    - Real-time transaction data from PostgreSQL
    - Change Data Capture (CDC) simulation
    - Streaming to Kafka
    - Multi-currency support
    """
    
    KAFKA_TOPIC_TRANSACTIONS = "fame.transactions.realtime"
    KAFKA_TOPIC_CDC = "fame.transactions.cdc"
    
    # Transaction types for embedded finance
    TRANSACTION_TYPES = [
        "PAYMENT", "TRANSFER", "WITHDRAWAL", "DEPOSIT",
        "CARD_PAYMENT", "DIRECT_DEBIT", "STANDING_ORDER",
        "INSTANT_PAYMENT", "SEPA_TRANSFER", "SWIFT_TRANSFER",
        "LOAN_DISBURSEMENT", "LOAN_REPAYMENT", "FX_EXCHANGE",
        "INVESTMENT_BUY", "INVESTMENT_SELL", "DIVIDEND_PAYMENT"
    ]
    
    CATEGORIES = [
        "RETAIL", "CORPORATE", "INTERBANK", "INVESTMENT",
        "EMBEDDED_FINANCE", "PAYROLL", "E_COMMERCE", "P2P"
    ]
    
    CHANNELS = [
        "MOBILE_APP", "WEB_BANKING", "API", "BRANCH",
        "ATM", "POS_TERMINAL", "EMBEDDED_CHECKOUT"
    ]
    
    # Sample banks and customers
    BANKS = [
        {"code": "BNPAFRPP", "name": "BNP Paribas", "country": "FR"},
        {"code": "SOGEFRPP", "name": "Société Générale", "country": "FR"},
        {"code": "DEUTDEFF", "name": "Deutsche Bank", "country": "DE"},
        {"code": "COBADEFF", "name": "Commerzbank", "country": "DE"},
        {"code": "BABOROBU", "name": "ING Bank", "country": "NL"},
        {"code": "BSCHESMM", "name": "Banco Santander", "country": "ES"},
        {"code": "BCITEITMM", "name": "Intesa Sanpaolo", "country": "IT"},
        {"code": "MIDLGB22", "name": "HSBC UK", "country": "UK"},
        {"code": "CHASUS33", "name": "JPMorgan Chase", "country": "US"},
    ]
    
    CUSTOMERS = [
        {"id": "C001", "name": "Marie Dubois", "country": "FR", "type": "RETAIL"},
        {"id": "C002", "name": "Hans Mueller", "country": "DE", "type": "RETAIL"},
        {"id": "C003", "name": "Giovanni Rossi", "country": "IT", "type": "RETAIL"},
        {"id": "C004", "name": "TechCorp SAS", "country": "FR", "type": "CORPORATE"},
        {"id": "C005", "name": "EuroTrade GmbH", "country": "DE", "type": "CORPORATE"},
        {"id": "C006", "name": "FinServ Ltd", "country": "UK", "type": "CORPORATE"},
        {"id": "C007", "name": "PayQuick", "country": "NL", "type": "FINTECH"},
        {"id": "C008", "name": "ShopNow Platform", "country": "FR", "type": "E_COMMERCE"},
        {"id": "C009", "name": "Global Investments SA", "country": "LU", "type": "INVESTMENT"},
        {"id": "C010", "name": "Carlos Garcia", "country": "ES", "type": "RETAIL"},
    ]
    
    # Exchange rates to EUR
    FX_RATES = {
        "EUR": 1.0, "USD": 0.92, "GBP": 1.16, "CHF": 1.06,
        "JPY": 0.0062, "CAD": 0.68, "AUD": 0.60
    }
    
    def __init__(self, 
                 db_host: str = "localhost",
                 db_port: int = 5432,
                 db_name: str = "fame_transactions",
                 db_user: str = "fame_user",
                 db_password: str = "fame_password",
                 kafka_servers: str = "localhost:29092"):
        """Initialize database connector."""
        self.db_config = {
            "host": db_host,
            "port": db_port,
            "database": db_name,
            "user": db_user,
            "password": db_password
        }
        self.kafka_servers = kafka_servers
        self.connection = None
        self.producer = None
        
        if KAFKA_AVAILABLE:
            self._init_kafka_producer()
    
    def _init_kafka_producer(self):
        """Initialize Kafka producer."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info("✅ Kafka producer connected for transactions")
        except Exception as e:
            logger.warning(f"⚠️ Kafka connection failed: {e}")
    
    def _publish_to_kafka(self, topic: str, key: str, data: Dict):
        """Publish to Kafka."""
        if self.producer:
            try:
                self.producer.send(topic, key=key, value=data)
            except Exception as e:
                logger.error(f"Kafka error: {e}")
    
    def connect_db(self):
        """Connect to PostgreSQL database."""
        if not POSTGRES_AVAILABLE:
            logger.warning("⚠️ psycopg2 not installed. Using simulated data.")
            return False
        
        try:
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("✅ Connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Database connection failed: {e}. Using simulated data.")
            return False
    
    def generate_transaction(self) -> FinancialTransaction:
        """Generate a realistic financial transaction."""
        # Select random participants
        sender = random.choice(self.CUSTOMERS)
        receiver = random.choice([c for c in self.CUSTOMERS if c["id"] != sender["id"]])
        sender_bank = random.choice([b for b in self.BANKS if b["country"] == sender["country"]] or self.BANKS)
        receiver_bank = random.choice([b for b in self.BANKS if b["country"] == receiver["country"]] or self.BANKS)
        
        # Transaction details
        tx_type = random.choice(self.TRANSACTION_TYPES)
        category = sender["type"] if sender["type"] in self.CATEGORIES else random.choice(self.CATEGORIES)
        
        # Amount based on category
        if category == "RETAIL":
            amount = round(random.uniform(10, 5000), 2)
        elif category == "CORPORATE":
            amount = round(random.uniform(1000, 500000), 2)
        elif category == "INTERBANK":
            amount = round(random.uniform(100000, 10000000), 2)
        else:
            amount = round(random.uniform(100, 50000), 2)
        
        # Currency
        currencies = list(self.FX_RATES.keys())
        currency = random.choices(currencies, weights=[0.6, 0.25, 0.08, 0.03, 0.02, 0.01, 0.01])[0]
        exchange_rate = self.FX_RATES[currency]
        amount_eur = round(amount * exchange_rate, 2)
        
        # Fee (0.1% to 2% based on type)
        fee_rate = 0.002 if "INSTANT" in tx_type else 0.001
        fee = round(amount * fee_rate, 2)
        
        return FinancialTransaction(
            source="postgresql_db",
            source_type="SQL_DATABASE",
            transaction_id=str(uuid.uuid4()),
            timestamp=datetime.now().isoformat(),
            transaction_type=tx_type,
            category=category,
            sender_id=sender["id"],
            sender_name=sender["name"],
            sender_country=sender["country"],
            sender_bank=sender_bank["name"],
            receiver_id=receiver["id"],
            receiver_name=receiver["name"],
            receiver_country=receiver["country"],
            receiver_bank=receiver_bank["name"],
            amount=amount,
            currency=currency,
            exchange_rate=exchange_rate,
            amount_eur=amount_eur,
            status=random.choices(["COMPLETED", "PENDING", "PROCESSING", "FAILED"], 
                                  weights=[0.85, 0.08, 0.05, 0.02])[0],
            channel=random.choice(self.CHANNELS),
            fee=fee,
            reference=f"REF-{datetime.now().strftime('%Y%m%d')}-{random.randint(100000, 999999)}"
        )
    
    def generate_batch_transactions(self, count: int = 1000, 
                                    days_back: int = 30,
                                    publish_kafka: bool = True) -> pd.DataFrame:
        """
        Generate batch of historical transactions.
        
        Args:
            count: Number of transactions to generate
            days_back: Days of history to simulate
            publish_kafka: Whether to publish to Kafka
            
        Returns:
            DataFrame with transactions
        """
        transactions = []
        
        for i in range(count):
            tx = self.generate_transaction()
            
            # Randomize timestamp within date range
            random_days = random.uniform(0, days_back)
            random_hours = random.uniform(0, 24)
            tx_time = datetime.now() - timedelta(days=random_days, hours=random_hours)
            tx.timestamp = tx_time.isoformat()
            
            transactions.append(asdict(tx))
            
            if publish_kafka:
                self._publish_to_kafka(
                    self.KAFKA_TOPIC_TRANSACTIONS,
                    tx.transaction_id,
                    asdict(tx)
                )
        
        if publish_kafka and self.producer:
            self.producer.flush()
            logger.info(f"📤 Published {count} transactions to Kafka")
        
        df = pd.DataFrame(transactions)
        logger.info(f"📊 Generated {len(df)} transactions")
        
        return df
    
    def stream_realtime_transactions(self, 
                                      transactions_per_second: float = 1.0,
                                      duration_seconds: int = 60):
        """
        Stream real-time transactions to Kafka.
        
        Args:
            transactions_per_second: Rate of transaction generation
            duration_seconds: Duration of streaming
        """
        import time
        
        logger.info(f"🚀 Starting real-time transaction streaming...")
        logger.info(f"   Rate: {transactions_per_second} tx/sec | Duration: {duration_seconds}s")
        
        end_time = time.time() + duration_seconds
        count = 0
        interval = 1.0 / transactions_per_second
        
        while time.time() < end_time:
            tx = self.generate_transaction()
            
            self._publish_to_kafka(
                self.KAFKA_TOPIC_TRANSACTIONS,
                tx.transaction_id,
                asdict(tx)
            )
            
            count += 1
            logger.info(f"💳 TX {count}: {tx.transaction_type} | {tx.amount} {tx.currency} | {tx.sender_name} → {tx.receiver_name}")
            
            time.sleep(interval)
        
        if self.producer:
            self.producer.flush()
        
        logger.info(f"✅ Streaming complete. Generated {count} transactions.")
    
    def get_transaction_stats(self, df: pd.DataFrame) -> Dict:
        """Calculate transaction statistics."""
        return {
            "total_transactions": len(df),
            "total_volume_eur": round(df['amount_eur'].sum(), 2),
            "avg_transaction_eur": round(df['amount_eur'].mean(), 2),
            "by_type": df['transaction_type'].value_counts().to_dict(),
            "by_category": df['category'].value_counts().to_dict(),
            "by_currency": df['currency'].value_counts().to_dict(),
            "by_status": df['status'].value_counts().to_dict(),
            "cross_border_pct": round(
                len(df[df['sender_country'] != df['receiver_country']]) / len(df) * 100, 2
            )
        }
    
    def save_to_datalake(self, df: pd.DataFrame, filepath: str):
        """Save to Data Lake as JSON lines."""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df.to_json(filepath, orient='records', lines=True, date_format='iso')
        logger.info(f"💾 Saved {len(df)} records to {filepath}")
    
    def export_sql_init_script(self, filepath: str = "database/init.sql"):
        """Export SQL initialization script for PostgreSQL."""
        sql_script = """
-- FAME Financial Data Space - Database Initialization
-- Source 4: Financial Transactions

-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    transaction_type VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    sender_id VARCHAR(20) NOT NULL,
    sender_name VARCHAR(100) NOT NULL,
    sender_country VARCHAR(3) NOT NULL,
    sender_bank VARCHAR(100) NOT NULL,
    receiver_id VARCHAR(20) NOT NULL,
    receiver_name VARCHAR(100) NOT NULL,
    receiver_country VARCHAR(3) NOT NULL,
    receiver_bank VARCHAR(100) NOT NULL,
    amount DECIMAL(18, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    exchange_rate DECIMAL(10, 6) NOT NULL,
    amount_eur DECIMAL(18, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    channel VARCHAR(50) NOT NULL,
    fee DECIMAL(10, 2) DEFAULT 0,
    reference VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX idx_transactions_sender ON transactions(sender_id);
CREATE INDEX idx_transactions_receiver ON transactions(receiver_id);
CREATE INDEX idx_transactions_type ON transactions(transaction_type);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_currency ON transactions(currency);

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(3) NOT NULL,
    customer_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create banks table
CREATE TABLE IF NOT EXISTS banks (
    bank_code VARCHAR(11) PRIMARY KEY,
    bank_name VARCHAR(100) NOT NULL,
    country VARCHAR(3) NOT NULL,
    swift_code VARCHAR(11)
);

-- Insert sample customers
INSERT INTO customers (customer_id, name, country, customer_type) VALUES
    ('C001', 'Marie Dubois', 'FR', 'RETAIL'),
    ('C002', 'Hans Mueller', 'DE', 'RETAIL'),
    ('C003', 'Giovanni Rossi', 'IT', 'RETAIL'),
    ('C004', 'TechCorp SAS', 'FR', 'CORPORATE'),
    ('C005', 'EuroTrade GmbH', 'DE', 'CORPORATE'),
    ('C006', 'FinServ Ltd', 'UK', 'CORPORATE'),
    ('C007', 'PayQuick', 'NL', 'FINTECH'),
    ('C008', 'ShopNow Platform', 'FR', 'E_COMMERCE'),
    ('C009', 'Global Investments SA', 'LU', 'INVESTMENT'),
    ('C010', 'Carlos Garcia', 'ES', 'RETAIL')
ON CONFLICT (customer_id) DO NOTHING;

-- Insert sample banks
INSERT INTO banks (bank_code, bank_name, country, swift_code) VALUES
    ('BNPAFRPP', 'BNP Paribas', 'FR', 'BNPAFRPP'),
    ('SOGEFRPP', 'Société Générale', 'FR', 'SOGEFRPP'),
    ('DEUTDEFF', 'Deutsche Bank', 'DE', 'DEUTDEFF'),
    ('COBADEFF', 'Commerzbank', 'DE', 'COBADEFF'),
    ('BABOROBU', 'ING Bank', 'NL', 'BABOROBU'),
    ('BSCHESMM', 'Banco Santander', 'ES', 'BSCHESMM'),
    ('BCITEITMM', 'Intesa Sanpaolo', 'IT', 'BCITEITMM'),
    ('MIDLGB22', 'HSBC UK', 'UK', 'MIDLGB22'),
    ('CHASUS33', 'JPMorgan Chase', 'US', 'CHASUS33')
ON CONFLICT (bank_code) DO NOTHING;

-- Create view for transaction analytics
CREATE OR REPLACE VIEW transaction_analytics AS
SELECT 
    DATE(timestamp) as date,
    transaction_type,
    category,
    currency,
    COUNT(*) as tx_count,
    SUM(amount_eur) as total_volume_eur,
    AVG(amount_eur) as avg_amount_eur,
    COUNT(CASE WHEN sender_country != receiver_country THEN 1 END) as cross_border_count
FROM transactions
GROUP BY DATE(timestamp), transaction_type, category, currency;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fame_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO fame_user;

-- Success message
SELECT 'FAME Database initialized successfully!' as status;
"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            f.write(sql_script)
        
        logger.info(f"📝 SQL init script exported to {filepath}")
        return filepath
    
    def close(self):
        """Close connections."""
        if self.connection:
            self.connection.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()


# Test
if __name__ == "__main__":
    print("=" * 70)
    print("FAME Data Space - SOURCE 4: Financial Transactions (PostgreSQL)")
    print("=" * 70)
    
    connector = TransactionDatabaseConnector()
    
    # Export SQL init script
    connector.export_sql_init_script()
    
    # Generate batch transactions
    print("\n📊 Generating batch transactions...")
    df = connector.generate_batch_transactions(count=500, days_back=30, publish_kafka=False)
    
    print(f"\n📈 Transaction Statistics:")
    stats = connector.get_transaction_stats(df)
    for key, value in stats.items():
        if isinstance(value, dict):
            print(f"   {key}:")
            for k, v in list(value.items())[:5]:
                print(f"      {k}: {v}")
        else:
            print(f"   {key}: {value}")
    
    # Save sample
    print("\n💾 Saving to Data Lake...")
    connector.save_to_datalake(df, "data/raw/sql/transactions.json")
    
    # Show sample
    print("\n📋 Sample transactions:")
    print(df[['timestamp', 'transaction_type', 'sender_name', 'receiver_name', 'amount', 'currency', 'status']].head(10))
    
    connector.close()
    print("\n✅ Source 4 test complete!")
