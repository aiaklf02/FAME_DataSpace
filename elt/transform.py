"""
FAME Data Space - ELT Transform Module
========================================
Transform data IN the Data Warehouse using SQL (DuckDB).

Step 3 of EtLT: Transform data using warehouse compute power.

Key difference from ETL:
- ETL: Transform BEFORE loading (in memory, limited)
- ELT: Transform AFTER loading (in warehouse, scalable)
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TransformResult:
    """Result of data transformation."""
    source_table: str
    target_table: str
    record_count: int
    transformed_at: datetime
    status: str
    sql_query: str


class FAMETransformer:
    """
    ELT Transformer - Transform data IN the Data Warehouse.
    
    Uses DuckDB SQL for transformations:
    - Staging → Silver (cleaned, validated)
    - Silver → Gold (aggregated, enriched)
    - Gold → Dimensional Model (star schema)
    """
    
    def __init__(self, warehouse_path: str = "data/warehouse"):
        """Initialize transformer with warehouse connection."""
        self.warehouse_path = warehouse_path
        self._init_connection()
    
    def _init_connection(self):
        """Connect to DuckDB warehouse."""
        try:
            import duckdb
            db_path = os.path.join(self.warehouse_path, "fame_warehouse.duckdb")
            self.conn = duckdb.connect(db_path)
            self.duckdb_available = True
            logger.info(f"✅ Connected to DuckDB: {db_path}")
        except ImportError:
            logger.warning("⚠️ DuckDB not available")
            self.duckdb_available = False
            self.conn = None
    
    # =========================================================================
    # SILVER LAYER - Cleaned & Validated Data
    # =========================================================================
    
    def transform_stocks_to_silver(self) -> TransformResult:
        """
        Transform stocks: Staging → Silver
        
        Transformations:
        - Validate prices (> 0)
        - Standardize currency codes
        - Add computed fields
        """
        logger.info("🔄 Transforming stocks: Staging → Silver...")
        
        sql = """
        CREATE OR REPLACE TABLE silver_stocks AS
        SELECT 
            symbol,
            company_name,
            current_price as price,
            previous_close,
            change as price_change,
            change_percent as change_pct,
            volume,
            market_cap,
            UPPER(currency) as currency,
            exchange,
            timestamp,
            
            -- Computed fields
            CASE 
                WHEN currency = 'USD' THEN current_price / 1.08
                WHEN currency = 'GBP' THEN current_price / 0.86
                ELSE current_price 
            END as price_eur,
            
            CASE 
                WHEN change_percent > 0 THEN 'UP'
                WHEN change_percent < 0 THEN 'DOWN'
                ELSE 'STABLE'
            END as trend,
            
            -- Data quality flags
            CASE WHEN current_price > 0 THEN TRUE ELSE FALSE END as is_valid_price,
            
            -- Metadata
            _extracted_at,
            _source,
            CURRENT_TIMESTAMP as _transformed_at,
            'silver' as _layer
            
        FROM stg_stocks
        WHERE current_price > 0  -- Filter invalid prices
        """
        
        if self.duckdb_available:
            self.conn.execute(sql)
            count = self.conn.execute("SELECT COUNT(*) FROM silver_stocks").fetchone()[0]
        else:
            count = 0
        
        return TransformResult(
            source_table="stg_stocks",
            target_table="silver_stocks",
            record_count=count,
            transformed_at=datetime.now(),
            status="success",
            sql_query=sql
        )
    
    def transform_forex_to_silver(self) -> TransformResult:
        """
        Transform forex: Staging → Silver
        
        Transformations:
        - Calculate inverse rates
        - Add rate change indicators
        """
        logger.info("🔄 Transforming forex: Staging → Silver...")
        
        sql = """
        CREATE OR REPLACE TABLE silver_forex AS
        SELECT 
            base_currency,
            target_currency,
            rate,
            reference_date,
            
            -- Computed fields
            1.0 / rate as inverse_rate,
            CONCAT(base_currency, '/', target_currency) as currency_pair,
            
            -- Metadata
            _extracted_at,
            _source,
            CURRENT_TIMESTAMP as _transformed_at,
            'silver' as _layer
            
        FROM stg_forex
        WHERE rate > 0
        """
        
        if self.duckdb_available:
            self.conn.execute(sql)
            count = self.conn.execute("SELECT COUNT(*) FROM silver_forex").fetchone()[0]
        else:
            count = 0
        
        return TransformResult(
            source_table="stg_forex",
            target_table="silver_forex",
            record_count=count,
            transformed_at=datetime.now(),
            status="success",
            sql_query=sql
        )
    
    def transform_financials_to_silver(self) -> TransformResult:
        """
        Transform financials: Staging → Silver
        
        Handles multiple CSV sources (SP500, GDP, NASDAQ, NYSE) with different schemas.
        """
        logger.info("🔄 Transforming financials: Staging → Silver...")
        
        # Create silver table with all columns from staging (schema-on-read approach)
        sql = """
        CREATE OR REPLACE TABLE silver_financials AS
        SELECT 
            *,
            CURRENT_TIMESTAMP as _transformed_at,
            'silver' as _layer
        FROM stg_financials
        """
        
        if self.duckdb_available:
            self.conn.execute(sql)
            count = self.conn.execute("SELECT COUNT(*) FROM silver_financials").fetchone()[0]
        else:
            count = 0
        
        return TransformResult(
            source_table="stg_financials",
            target_table="silver_financials",
            record_count=count,
            transformed_at=datetime.now(),
            status="success",
            sql_query=sql
        )
    
    def transform_transactions_to_silver(self) -> TransformResult:
        """
        Transform transactions: Staging → Silver
        
        Transformations:
        - Convert all amounts to EUR
        - Add risk flags
        - Categorize transactions
        """
        logger.info("🔄 Transforming transactions: Staging → Silver...")
        
        sql = """
        CREATE OR REPLACE TABLE silver_transactions AS
        SELECT 
            transaction_id,
            amount,
            currency,
            
            -- Use existing amount_eur or calculate
            COALESCE(amount_eur, 
                CASE 
                    WHEN currency = 'EUR' THEN amount
                    WHEN currency = 'USD' THEN amount / 1.08
                    WHEN currency = 'GBP' THEN amount / 0.86
                    ELSE amount
                END
            ) as amount_eur,
            
            sender_id,
            sender_name,
            UPPER(sender_country) as sender_country,
            receiver_id,
            receiver_name,
            UPPER(receiver_country) as receiver_country,
            transaction_type,
            status,
            is_cross_border,
            created_at as timestamp,
            
            -- Risk flags
            CASE 
                WHEN amount > 10000 THEN 'HIGH'
                WHEN amount > 1000 THEN 'MEDIUM'
                ELSE 'LOW'
            END as amount_risk_level,
            
            CASE 
                WHEN is_cross_border AND amount > 5000 THEN TRUE
                ELSE FALSE
            END as requires_aml_check,
            
            -- Time dimensions
            DATE_TRUNC('day', created_at::TIMESTAMP) as tx_date,
            DATE_TRUNC('hour', created_at::TIMESTAMP) as tx_hour,
            
            -- Metadata
            _extracted_at,
            _source,
            CURRENT_TIMESTAMP as _transformed_at,
            'silver' as _layer
            
        FROM stg_transactions
        WHERE status != 'FAILED'
        """
        
        if self.duckdb_available:
            self.conn.execute(sql)
            count = self.conn.execute("SELECT COUNT(*) FROM silver_transactions").fetchone()[0]
        else:
            count = 0
        
        return TransformResult(
            source_table="stg_transactions",
            target_table="silver_transactions",
            record_count=count,
            transformed_at=datetime.now(),
            status="success",
            sql_query=sql
        )
    
    # =========================================================================
    # GOLD LAYER - Aggregated & Enriched Data
    # =========================================================================
    
    def transform_to_gold_daily_market(self) -> TransformResult:
        """
        Create Gold layer: Daily Market Summary
        """
        logger.info("🔄 Creating Gold: Daily Market Summary...")
        
        sql = """
        CREATE OR REPLACE TABLE gold_daily_market AS
        SELECT 
            DATE_TRUNC('day', timestamp::TIMESTAMP) as market_date,
            exchange,
            currency,
            COUNT(*) as quote_count,
            AVG(price) as avg_price,
            AVG(price_eur) as avg_price_eur,
            SUM(volume) as total_volume,
            COUNT(CASE WHEN trend = 'UP' THEN 1 END) as stocks_up,
            COUNT(CASE WHEN trend = 'DOWN' THEN 1 END) as stocks_down,
            CURRENT_TIMESTAMP as _created_at
        FROM silver_stocks
        GROUP BY 1, 2, 3
        """
        
        if self.duckdb_available:
            self.conn.execute(sql)
            count = self.conn.execute("SELECT COUNT(*) FROM gold_daily_market").fetchone()[0]
        else:
            count = 0
        
        return TransformResult(
            source_table="silver_stocks",
            target_table="gold_daily_market",
            record_count=count,
            transformed_at=datetime.now(),
            status="success",
            sql_query=sql
        )
    
    def transform_to_gold_tx_summary(self) -> TransformResult:
        """
        Create Gold layer: Transaction Summary by Country
        """
        logger.info("🔄 Creating Gold: Transaction Summary...")
        
        sql = """
        CREATE OR REPLACE TABLE gold_tx_summary AS
        SELECT 
            tx_date,
            sender_country,
            receiver_country,
            transaction_type,
            is_cross_border,
            COUNT(*) as tx_count,
            SUM(amount_eur) as total_volume_eur,
            AVG(amount_eur) as avg_amount_eur,
            COUNT(CASE WHEN requires_aml_check THEN 1 END) as aml_flagged,
            CURRENT_TIMESTAMP as _created_at
        FROM silver_transactions
        GROUP BY 1, 2, 3, 4, 5
        """
        
        if self.duckdb_available:
            self.conn.execute(sql)
            count = self.conn.execute("SELECT COUNT(*) FROM gold_tx_summary").fetchone()[0]
        else:
            count = 0
        
        return TransformResult(
            source_table="silver_transactions",
            target_table="gold_tx_summary",
            record_count=count,
            transformed_at=datetime.now(),
            status="success",
            sql_query=sql
        )
    
    # =========================================================================
    # DIMENSIONAL MODEL (Star Schema)
    # =========================================================================
    
    def create_dim_company(self) -> TransformResult:
        """Create dimension table: DIM_COMPANY from S&P500 data."""
        logger.info("🔄 Creating Dimension: DIM_COMPANY...")
        
        # Use columns that exist in sp500_companies.csv: Symbol, Name, Sector
        sql = """
        CREATE OR REPLACE TABLE dim_company AS
        SELECT 
            ROW_NUMBER() OVER () as company_sk,
            Symbol as company_id,
            Name as company_name,
            Sector as sector,
            'US' as country,
            CURRENT_TIMESTAMP as valid_from,
            NULL as valid_to,
            TRUE as is_current
        FROM silver_financials
        WHERE _csv_source = 'sp500' AND Symbol IS NOT NULL
        GROUP BY Symbol, Name, Sector
        """
        
        if self.duckdb_available:
            try:
                self.conn.execute(sql)
                count = self.conn.execute("SELECT COUNT(*) FROM dim_company").fetchone()[0]
            except Exception as e:
                logger.warning(f"⚠️ Could not create dim_company: {str(e)[:100]}")
                count = 0
        else:
            count = 0
        
        return TransformResult(
            source_table="silver_financials",
            target_table="dim_company",
            record_count=count,
            transformed_at=datetime.now(),
            status="success" if count > 0 else "skipped",
            sql_query=sql
        )
    
    def create_dim_currency(self) -> TransformResult:
        """Create dimension table: DIM_CURRENCY"""
        logger.info("🔄 Creating Dimension: DIM_CURRENCY...")
        
        sql = """
        CREATE OR REPLACE TABLE dim_currency AS
        SELECT 
            ROW_NUMBER() OVER () as currency_sk,
            target_currency as currency_code,
            rate as latest_rate_vs_eur,
            reference_date as rate_date
        FROM silver_forex
        
        UNION ALL
        
        SELECT 
            0 as currency_sk,
            'EUR' as currency_code,
            1.0 as latest_rate_vs_eur,
            CURRENT_DATE as rate_date
        """
        
        if self.duckdb_available:
            self.conn.execute(sql)
            count = self.conn.execute("SELECT COUNT(*) FROM dim_currency").fetchone()[0]
        else:
            count = 0
        
        return TransformResult(
            source_table="silver_forex",
            target_table="dim_currency",
            record_count=count,
            transformed_at=datetime.now(),
            status="success",
            sql_query=sql
        )
    
    def create_dim_date(self) -> TransformResult:
        """Create dimension table: DIM_DATE"""
        logger.info("🔄 Creating Dimension: DIM_DATE...")
        
        sql = """
        CREATE OR REPLACE TABLE dim_date AS
        SELECT 
            CAST(STRFTIME(d, '%Y%m%d') AS INTEGER) as date_sk,
            d as full_date,
            YEAR(d) as year,
            QUARTER(d) as quarter,
            MONTH(d) as month,
            DAY(d) as day,
            DAYOFWEEK(d) as day_of_week,
            WEEKOFYEAR(d) as week_of_year,
            CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN FALSE ELSE TRUE END as is_business_day
        FROM (
            SELECT UNNEST(GENERATE_SERIES(
                DATE '2024-01-01', 
                DATE '2026-12-31', 
                INTERVAL 1 DAY
            ))::DATE as d
        )
        """
        
        if self.duckdb_available:
            self.conn.execute(sql)
            count = self.conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
        else:
            count = 0
        
        return TransformResult(
            source_table="generated",
            target_table="dim_date",
            record_count=count,
            transformed_at=datetime.now(),
            status="success",
            sql_query=sql
        )
    
    def create_fact_transactions(self) -> TransformResult:
        """Create fact table: FACT_TRANSACTIONS"""
        logger.info("🔄 Creating Fact: FACT_TRANSACTIONS...")
        
        sql = """
        CREATE OR REPLACE TABLE fact_transactions AS
        SELECT 
            transaction_id,
            CAST(STRFTIME(tx_date, '%Y%m%d') AS INTEGER) as date_sk,
            sender_country,
            receiver_country,
            transaction_type,
            currency,
            amount,
            amount_eur,
            is_cross_border,
            amount_risk_level,
            requires_aml_check,
            status
        FROM silver_transactions
        """
        
        if self.duckdb_available:
            self.conn.execute(sql)
            count = self.conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
        else:
            count = 0
        
        return TransformResult(
            source_table="silver_transactions",
            target_table="fact_transactions",
            record_count=count,
            transformed_at=datetime.now(),
            status="success",
            sql_query=sql
        )
    
    # =========================================================================
    # TRANSFORM ALL
    # =========================================================================
    
    def transform_all(self) -> List[TransformResult]:
        """Run all transformations."""
        logger.info("=" * 60)
        logger.info("🔄 FAME ELT - TRANSFORM PHASE")
        logger.info("=" * 60)
        
        results = []
        
        # Staging → Silver
        logger.info("\n📊 SILVER LAYER (Cleaned)")
        results.append(self.transform_stocks_to_silver())
        results.append(self.transform_forex_to_silver())
        results.append(self.transform_financials_to_silver())
        results.append(self.transform_transactions_to_silver())
        
        # Silver → Gold
        logger.info("\n🏆 GOLD LAYER (Aggregated)")
        results.append(self.transform_to_gold_daily_market())
        results.append(self.transform_to_gold_tx_summary())
        
        # Dimensional Model
        logger.info("\n⭐ DIMENSIONAL MODEL (Star Schema)")
        results.append(self.create_dim_date())
        results.append(self.create_dim_company())
        results.append(self.create_dim_currency())
        results.append(self.create_fact_transactions())
        
        # Export to files
        logger.info("\n📁 EXPORTING TO FILES")
        self._export_to_files()
        
        logger.info(f"\n✅ Transform complete: {len(results)} tables created")
        
        return results
    
    def _export_to_files(self):
        """Export Silver and Gold tables to files in data/silver and data/gold."""
        if not self.duckdb_available:
            logger.warning("DuckDB not available, skipping file export")
            return
        
        import pandas as pd
        
        # Create directories
        base_path = os.path.dirname(self.warehouse_path)
        silver_path = os.path.join(base_path, "silver")
        gold_path = os.path.join(base_path, "gold")
        os.makedirs(silver_path, exist_ok=True)
        os.makedirs(gold_path, exist_ok=True)
        
        # Export Silver tables
        silver_tables = ["silver_stocks", "silver_forex", "silver_financials", "silver_transactions"]
        for table in silver_tables:
            try:
                df = self.conn.execute(f"SELECT * FROM {table}").fetchdf()
                filepath = os.path.join(silver_path, f"{table}.parquet")
                df.to_parquet(filepath, index=False)
                logger.info(f"   ✅ Exported {table} → {filepath} ({len(df)} rows)")
            except Exception as e:
                logger.warning(f"   ⚠️ Could not export {table}: {e}")
        
        # Export Gold tables
        gold_tables = ["gold_daily_market", "gold_tx_summary"]
        for table in gold_tables:
            try:
                df = self.conn.execute(f"SELECT * FROM {table}").fetchdf()
                filepath = os.path.join(gold_path, f"{table}.parquet")
                df.to_parquet(filepath, index=False)
                logger.info(f"   ✅ Exported {table} → {filepath} ({len(df)} rows)")
            except Exception as e:
                logger.warning(f"   ⚠️ Could not export {table}: {e}")
        
        # Export Dimensional tables to warehouse
        dim_tables = ["dim_date", "dim_company", "dim_currency", "fact_transactions"]
        warehouse_dims = os.path.join(self.warehouse_path, "dimensions")
        warehouse_facts = os.path.join(self.warehouse_path, "facts")
        os.makedirs(warehouse_dims, exist_ok=True)
        os.makedirs(warehouse_facts, exist_ok=True)
        
        for table in dim_tables:
            try:
                df = self.conn.execute(f"SELECT * FROM {table}").fetchdf()
                if table.startswith("dim_"):
                    filepath = os.path.join(warehouse_dims, f"{table}.parquet")
                else:
                    filepath = os.path.join(warehouse_facts, f"{table}.parquet")
                df.to_parquet(filepath, index=False)
                logger.info(f"   ✅ Exported {table} → {filepath} ({len(df)} rows)")
            except Exception as e:
                logger.warning(f"   ⚠️ Could not export {table}: {e}")
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


# CLI Test
if __name__ == "__main__":
    transformer = FAMETransformer()
    results = transformer.transform_all()
    
    print("\n📋 Transform Summary:")
    for r in results:
        print(f"   {r.source_table} → {r.target_table}: {r.record_count} records")
    
    transformer.close()
