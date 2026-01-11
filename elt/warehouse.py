"""
FAME Data Space - Data Warehouse Module
========================================
DuckDB-based analytical Data Warehouse with star schema.

Features:
- Fast OLAP queries
- Star schema (dimensions + facts)
- Analytical views
- BI-ready data
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FAMEWarehouse:
    """
    FAME Data Warehouse powered by DuckDB.
    
    Provides:
    - Fast analytical SQL queries
    - Pre-built analytical views
    - BI dashboard data
    - Export capabilities
    """
    
    def __init__(self, warehouse_path: str = "data/warehouse"):
        """Initialize warehouse connection."""
        self.warehouse_path = warehouse_path
        os.makedirs(warehouse_path, exist_ok=True)
        self._init_connection()
    
    def _init_connection(self):
        """Connect to DuckDB."""
        try:
            import duckdb
            db_path = os.path.join(self.warehouse_path, "fame_warehouse.duckdb")
            self.conn = duckdb.connect(db_path)
            self.available = True
            logger.info(f"✅ Warehouse connected: {db_path}")
        except ImportError:
            logger.warning("⚠️ DuckDB not installed")
            self.available = False
            self.conn = None
    
    # =========================================================================
    # ANALYTICAL QUERIES
    # =========================================================================
    
    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame."""
        if not self.available:
            logger.error("DuckDB not available")
            return pd.DataFrame()
        
        return self.conn.execute(sql).fetchdf()
    
    def get_market_overview(self) -> pd.DataFrame:
        """Get market overview for dashboard."""
        sql = """
        SELECT 
            exchange,
            COUNT(*) as stock_count,
            ROUND(AVG(price_eur), 2) as avg_price_eur,
            SUM(volume) as total_volume,
            COUNT(CASE WHEN trend = 'UP' THEN 1 END) as stocks_up,
            COUNT(CASE WHEN trend = 'DOWN' THEN 1 END) as stocks_down
        FROM silver_stocks
        GROUP BY exchange
        ORDER BY total_volume DESC
        """
        return self.query(sql)
    
    def get_transaction_summary(self, days: int = 7) -> pd.DataFrame:
        """Get transaction summary for last N days."""
        sql = f"""
        SELECT 
            tx_date,
            COUNT(*) as tx_count,
            ROUND(SUM(amount_eur), 2) as total_volume_eur,
            ROUND(AVG(amount_eur), 2) as avg_amount_eur,
            COUNT(CASE WHEN is_cross_border THEN 1 END) as cross_border_count,
            COUNT(CASE WHEN requires_aml_check THEN 1 END) as aml_flagged
        FROM silver_transactions
        WHERE tx_date >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY tx_date
        ORDER BY tx_date DESC
        """
        return self.query(sql)
    
    def get_cross_border_flows(self) -> pd.DataFrame:
        """Get cross-border payment flows."""
        sql = """
        SELECT 
            sender_country,
            receiver_country,
            COUNT(*) as tx_count,
            ROUND(SUM(amount_eur), 2) as total_volume_eur,
            ROUND(AVG(amount_eur), 2) as avg_amount_eur
        FROM silver_transactions
        WHERE is_cross_border = TRUE
        GROUP BY sender_country, receiver_country
        ORDER BY total_volume_eur DESC
        LIMIT 20
        """
        return self.query(sql)
    
    def get_company_performance(self) -> pd.DataFrame:
        """Get company financial performance."""
        sql = """
        SELECT 
            company_name,
            ticker,
            sector,
            country,
            revenue_millions,
            net_income_millions,
            profit_margin_pct,
            roe_pct,
            asset_turnover
        FROM silver_financials
        ORDER BY revenue_millions DESC
        """
        return self.query(sql)
    
    def get_forex_rates(self) -> pd.DataFrame:
        """Get current exchange rates."""
        sql = """
        SELECT 
            currency_pair,
            rate,
            inverse_rate,
            reference_date
        FROM silver_forex
        ORDER BY target_currency
        """
        return self.query(sql)
    
    def get_kpis(self) -> Dict[str, Any]:
        """Get key performance indicators."""
        kpis = {}
        
        # Transaction KPIs
        tx_kpis = self.query("""
            SELECT 
                COUNT(*) as total_transactions,
                ROUND(SUM(amount_eur), 2) as total_volume_eur,
                ROUND(AVG(amount_eur), 2) as avg_transaction_eur,
                COUNT(CASE WHEN is_cross_border THEN 1 END) as cross_border_count,
                ROUND(COUNT(CASE WHEN is_cross_border THEN 1 END) * 100.0 / COUNT(*), 1) as cross_border_pct
            FROM silver_transactions
        """)
        
        if not tx_kpis.empty:
            kpis.update(tx_kpis.iloc[0].to_dict())
        
        # Market KPIs
        market_kpis = self.query("""
            SELECT 
                COUNT(*) as stock_count,
                ROUND(AVG(price_eur), 2) as avg_stock_price_eur,
                SUM(volume) as total_volume
            FROM silver_stocks
        """)
        
        if not market_kpis.empty:
            kpis.update(market_kpis.iloc[0].to_dict())
        
        return kpis
    
    # =========================================================================
    # STAR SCHEMA QUERIES
    # =========================================================================
    
    def get_transactions_by_date(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Query fact table with date dimension."""
        sql = """
        SELECT 
            d.full_date,
            d.year,
            d.quarter,
            d.month,
            d.is_business_day,
            COUNT(f.transaction_id) as tx_count,
            SUM(f.amount_eur) as total_amount_eur
        FROM fact_transactions f
        JOIN dim_date d ON f.date_sk = d.date_sk
        GROUP BY d.full_date, d.year, d.quarter, d.month, d.is_business_day
        ORDER BY d.full_date DESC
        LIMIT 30
        """
        return self.query(sql)
    
    # =========================================================================
    # EXPORT
    # =========================================================================
    
    def export_to_parquet(self, table_name: str, output_path: str = None) -> str:
        """Export table to Parquet file."""
        if output_path is None:
            output_path = os.path.join(self.warehouse_path, "exports", f"{table_name}.parquet")
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        self.conn.execute(f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET)")
        logger.info(f"✅ Exported {table_name} → {output_path}")
        
        return output_path
    
    def export_to_csv(self, table_name: str, output_path: str = None) -> str:
        """Export table to CSV file."""
        if output_path is None:
            output_path = os.path.join(self.warehouse_path, "exports", f"{table_name}.csv")
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        self.conn.execute(f"COPY {table_name} TO '{output_path}' (FORMAT CSV, HEADER)")
        logger.info(f"✅ Exported {table_name} → {output_path}")
        
        return output_path
    
    def list_tables(self) -> List[str]:
        """List all tables in warehouse."""
        result = self.conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """Get table schema information."""
        return self.query(f"DESCRIBE {table_name}")
    
    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return result[0] if result else 0
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


# CLI Test
if __name__ == "__main__":
    warehouse = FAMEWarehouse()
    
    print("\n📊 FAME Data Warehouse")
    print("=" * 50)
    
    # List tables
    print("\n📋 Tables:")
    for table in warehouse.list_tables():
        count = warehouse.get_row_count(table)
        print(f"   {table}: {count} rows")
    
    # Get KPIs
    print("\n📈 KPIs:")
    kpis = warehouse.get_kpis()
    for key, value in kpis.items():
        print(f"   {key}: {value}")
    
    warehouse.close()
