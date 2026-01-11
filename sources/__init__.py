"""
FAME Data Space - Sources Package
==================================
Data source connectors for heterogeneous financial data.
"""

# Import actual class names from source files
from .source1_stock_api import StockMarketAPIConnector
from .source2_ecb_xml import ECBExchangeRateConnector
from .source3_financials_csv import CompanyFinancialsCSVConnector
from .source4_transactions_db import TransactionDatabaseConnector
from .real_data_fetcher import RealDataFetcher

# Kafka (optional)
try:
    from .kafka_streaming import FAMEKafkaManager, FAMEKafkaTopics
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    FAMEKafkaManager = None
    FAMEKafkaTopics = None

# Aliases for backward compatibility
StockAPISource = StockMarketAPIConnector
ECBExchangeRateSource = ECBExchangeRateConnector
FinancialsCSVSource = CompanyFinancialsCSVConnector
TransactionDBSource = TransactionDatabaseConnector

__all__ = [
    'StockMarketAPIConnector',
    'StockAPISource',
    'ECBExchangeRateConnector',
    'ECBExchangeRateSource',
    'CompanyFinancialsCSVConnector',
    'FinancialsCSVSource',
    'TransactionDatabaseConnector',
    'TransactionDBSource',
    'RealDataFetcher',
    'FAMEKafkaManager',
    'FAMEKafkaTopics',
    'KAFKA_AVAILABLE'
]
