"""
FAME Data Space - ELT Load Module
==================================
Load raw data into Data Lake Bronze zone and Data Warehouse staging.

Step 2 of EtLT: Load data as-is into storage layers.
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Result of data loading."""
    destination: str
    table_name: str
    record_count: int
    loaded_at: datetime
    status: str
    metadata: Dict[str, Any]


class FAMELoader:
    """
    ELT Loader - Load raw data into Data Lake and Staging tables.
    
    Destinations:
    - Data Lake Bronze Zone (MinIO/S3)
    - Data Warehouse Staging Tables (DuckDB)
    """
    
    def __init__(self, data_lake_path: str = "data", warehouse_path: str = "data/warehouse"):
        """Initialize loader."""
        self.data_lake_path = data_lake_path
        self.warehouse_path = warehouse_path
        self.bronze_path = os.path.join(data_lake_path, "bronze")
        self.staging_path = os.path.join(warehouse_path, "staging")
        
        self._ensure_directories()
        self._init_warehouse()
    
    def _ensure_directories(self):
        """Create necessary directories."""
        os.makedirs(self.staging_path, exist_ok=True)
        os.makedirs(os.path.join(self.warehouse_path, "dimensions"), exist_ok=True)
        os.makedirs(os.path.join(self.warehouse_path, "facts"), exist_ok=True)
    
    def _init_warehouse(self):
        """Initialize DuckDB warehouse."""
        try:
            import duckdb
            self.db_path = os.path.join(self.warehouse_path, "fame_warehouse.duckdb")
            self.conn = duckdb.connect(self.db_path)
            self.duckdb_available = True
            self._create_staging_tables()
            logger.info(f"✅ DuckDB warehouse initialized: {self.db_path}")
        except ImportError:
            logger.warning("⚠️ DuckDB not installed. Run: pip install duckdb")
            self.duckdb_available = False
            self.conn = None
    
    def _create_staging_tables(self):
        """Create staging tables in DuckDB."""
        if not self.duckdb_available:
            return
        
        # Staging table for stocks (flexible schema to accept Yahoo Finance data)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_stocks (
                symbol VARCHAR,
                company_name VARCHAR,
                exchange VARCHAR(50),
                currency VARCHAR(10),
                current_price DECIMAL(18,4),
                previous_close DECIMAL(18,4),
                change DECIMAL(18,4),
                change_percent DECIMAL(8,4),
                volume BIGINT,
                market_cap BIGINT,
                fifty_two_week_high DECIMAL(18,4),
                fifty_two_week_low DECIMAL(18,4),
                timestamp VARCHAR,
                _source VARCHAR(50),
                _source_type VARCHAR(20),
                _format VARCHAR(20),
                _is_real_data BOOLEAN,
                _extracted_at TIMESTAMP,
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Staging table for forex
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_forex (
                base_currency VARCHAR(3),
                target_currency VARCHAR(3),
                rate DECIMAL(18,6),
                reference_date DATE,
                _extracted_at TIMESTAMP,
                _source VARCHAR(50),
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Staging table for financials
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_financials (
                company_name VARCHAR,
                ticker VARCHAR(20),
                sector VARCHAR(50),
                country VARCHAR(2),
                fiscal_year INTEGER,
                fiscal_quarter VARCHAR(2),
                revenue_millions DECIMAL(18,2),
                net_income_millions DECIMAL(18,2),
                total_assets_millions DECIMAL(18,2),
                profit_margin_pct DECIMAL(8,2),
                roe_pct DECIMAL(8,2),
                _extracted_at TIMESTAMP,
                _source VARCHAR(50),
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Staging table for transactions
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_transactions (
                transaction_id VARCHAR,
                amount DECIMAL(18,2),
                currency VARCHAR(3),
                sender_id VARCHAR(50),
                sender_name VARCHAR(200),
                sender_country VARCHAR(2),
                receiver_id VARCHAR(50),
                receiver_name VARCHAR(200),
                receiver_country VARCHAR(2),
                transaction_type VARCHAR(20),
                status VARCHAR(20),
                is_cross_border BOOLEAN,
                timestamp TIMESTAMP,
                _extracted_at TIMESTAMP,
                _source VARCHAR(50),
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        logger.info("   Staging tables created: stg_stocks, stg_forex, stg_financials, stg_transactions")
    
    # =========================================================================
    # LOAD TO DATA LAKE (Bronze Zone)
    # =========================================================================
    
    def load_to_bronze(self, data: List[Dict], source_type: str, source_name: str) -> LoadResult:
        """
        Load raw data to Data Lake Bronze zone.
        
        Data is stored as-is in JSON format (schema-on-read).
        """
        logger.info(f"📦 Loading to Bronze: {source_name}...")
        
        # Create directory for source
        source_dir = os.path.join(self.bronze_path, source_type)
        os.makedirs(source_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{source_name}_{timestamp}.json"
        filepath = os.path.join(source_dir, filename)
        
        # Save as JSON (raw format)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        return LoadResult(
            destination="data_lake_bronze",
            table_name=source_name,
            record_count=len(data),
            loaded_at=datetime.now(),
            status="success",
            metadata={"file_path": filepath, "format": "json"}
        )
    
    # =========================================================================
    # LOAD TO DATA WAREHOUSE (Staging)
    # =========================================================================
    
    def load_stocks_to_staging(self, data: List[Dict]) -> LoadResult:
        """Load stock data to DuckDB staging table."""
        logger.info("📦 Loading stocks to staging...")
        
        if not self.duckdb_available:
            return self._save_as_parquet(data, "stg_stocks")
        
        df = pd.DataFrame(data)
        
        # Add extracted_at if not present
        if '_extracted_at' not in df.columns:
            df['_extracted_at'] = datetime.now()
        
        # Drop and recreate table dynamically based on DataFrame columns
        self.conn.execute("DROP TABLE IF EXISTS stg_stocks")
        self.conn.execute("CREATE TABLE stg_stocks AS SELECT * FROM df")
        
        return LoadResult(
            destination="warehouse_staging",
            table_name="stg_stocks",
            record_count=len(data),
            loaded_at=datetime.now(),
            status="success",
            metadata={"database": "duckdb"}
        )
    
    def load_forex_to_staging(self, data: List[Dict]) -> LoadResult:
        """Load forex data to DuckDB staging table."""
        logger.info("📦 Loading forex to staging...")
        
        if not self.duckdb_available:
            return self._save_as_parquet(data, "stg_forex")
        
        df = pd.DataFrame(data)
        
        # Add extracted_at if not present
        if '_extracted_at' not in df.columns:
            df['_extracted_at'] = datetime.now()
        
        # Drop and recreate table dynamically
        self.conn.execute("DROP TABLE IF EXISTS stg_forex")
        self.conn.execute("CREATE TABLE stg_forex AS SELECT * FROM df")
        
        return LoadResult(
            destination="warehouse_staging",
            table_name="stg_forex",
            record_count=len(data),
            loaded_at=datetime.now(),
            status="success",
            metadata={"database": "duckdb"}
        )
    
    def load_financials_to_staging(self, data: List[Dict]) -> LoadResult:
        """Load financial data to DuckDB staging table."""
        logger.info("📦 Loading financials to staging...")
        
        if not self.duckdb_available:
            return self._save_as_parquet(data, "stg_financials")
        
        df = pd.DataFrame(data)
        
        # Add extracted_at if not present
        if '_extracted_at' not in df.columns:
            df['_extracted_at'] = datetime.now()
        
        # Drop and recreate table dynamically
        self.conn.execute("DROP TABLE IF EXISTS stg_financials")
        self.conn.execute("CREATE TABLE stg_financials AS SELECT * FROM df")
        
        return LoadResult(
            destination="warehouse_staging",
            table_name="stg_financials",
            record_count=len(data),
            loaded_at=datetime.now(),
            status="success",
            metadata={"database": "duckdb"}
        )
    
    def load_transactions_to_staging(self, data: List[Dict]) -> LoadResult:
        """Load transaction data to DuckDB staging table."""
        logger.info("📦 Loading transactions to staging...")
        
        if not self.duckdb_available:
            return self._save_as_parquet(data, "stg_transactions")
        
        df = pd.DataFrame(data)
        
        # Add extracted_at if not present
        if '_extracted_at' not in df.columns:
            df['_extracted_at'] = datetime.now()
        
        # Drop and recreate table dynamically
        self.conn.execute("DROP TABLE IF EXISTS stg_transactions")
        self.conn.execute("CREATE TABLE stg_transactions AS SELECT * FROM df")
        
        return LoadResult(
            destination="warehouse_staging",
            table_name="stg_transactions",
            record_count=len(data),
            loaded_at=datetime.now(),
            status="success",
            metadata={"database": "duckdb"}
        )
        
        return LoadResult(
            destination="warehouse_staging",
            table_name="stg_transactions",
            record_count=len(data),
            loaded_at=datetime.now(),
            status="success",
            metadata={"database": "duckdb"}
        )
    
    def _save_as_parquet(self, data: List[Dict], table_name: str) -> LoadResult:
        """Fallback: save as Parquet if DuckDB unavailable."""
        df = pd.DataFrame(data)
        filepath = os.path.join(self.staging_path, f"{table_name}.parquet")
        df.to_parquet(filepath, index=False)
        
        return LoadResult(
            destination="staging_parquet",
            table_name=table_name,
            record_count=len(data),
            loaded_at=datetime.now(),
            status="success",
            metadata={"file_path": filepath, "format": "parquet"}
        )
    
    # =========================================================================
    # LOAD ALL
    # =========================================================================
    
    def load_all_to_staging(self, extracted_data: Dict[str, List[Dict]]) -> List[LoadResult]:
        """Load all extracted data to staging tables."""
        logger.info("=" * 60)
        logger.info("📦 FAME ELT - LOAD PHASE")
        logger.info("=" * 60)
        
        results = []
        
        if "stocks" in extracted_data:
            results.append(self.load_stocks_to_staging(extracted_data["stocks"]))
        
        if "forex" in extracted_data:
            results.append(self.load_forex_to_staging(extracted_data["forex"]))
        
        if "financials" in extracted_data:
            results.append(self.load_financials_to_staging(extracted_data["financials"]))
        
        if "transactions" in extracted_data:
            results.append(self.load_transactions_to_staging(extracted_data["transactions"]))
        
        total = sum(r.record_count for r in results)
        logger.info(f"\n✅ Load complete: {total} records to {len(results)} staging tables")
        
        return results
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


# CLI Test
if __name__ == "__main__":
    loader = FAMELoader()
    
    # Test with sample data
    sample_data = {
        "stocks": [{"symbol": "AAPL", "price": 185.5, "volume": 1000000, "_extracted_at": datetime.now().isoformat(), "_source": "test"}],
        "forex": [{"base_currency": "EUR", "target_currency": "USD", "rate": 1.08, "reference_date": "2024-01-10", "_extracted_at": datetime.now().isoformat(), "_source": "test"}]
    }
    
    results = loader.load_all_to_staging(sample_data)
    
    print("\n📋 Load Summary:")
    for r in results:
        print(f"   {r.table_name}: {r.record_count} records → {r.destination}")
    
    loader.close()
