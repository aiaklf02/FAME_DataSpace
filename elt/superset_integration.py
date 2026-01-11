"""
FAME Data Space - Superset Data Integration
============================================
Exports all data layers (Bronze, Silver, Gold, Warehouse) to PostgreSQL
for Apache Superset visualization.

Data Flow:
==========
    [Yahoo Finance] ──┐
    [ECB XML]      ───┼──> [Bronze] ──> [Silver] ──> [Gold] ──> [PostgreSQL] ──> [Superset]
    [CSV Files]    ───┤                                              ↑
    [PostgreSQL]   ───┘                                              │
                                                                     │
    [DuckDB Warehouse] ──────────────────────────────────────────────┘
    
    [Kafka Streaming] ──> [Real-time Tables] ──> [PostgreSQL] ──> [Superset]
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL connection
PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'fame_transactions',
    'user': 'fame_user',
    'password': 'fame_password'
}

# Data paths
DATA_DIR = Path(__file__).parent.parent / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
WAREHOUSE_PATH = DATA_DIR / "warehouse" / "fame_warehouse.duckdb"


def get_pg_connection():
    """Get PostgreSQL connection."""
    try:
        import psycopg2
        conn = psycopg2.connect(**PG_CONFIG)
        return conn
    except ImportError:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        return None
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return None


def create_superset_schema(conn):
    """Create schema for Superset data."""
    cursor = conn.cursor()
    
    # Create schema for analytics
    cursor.execute("CREATE SCHEMA IF NOT EXISTS fame_analytics;")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS fame_streaming;")
    
    conn.commit()
    logger.info("✅ Created schemas: fame_analytics, fame_streaming")
    return cursor


def export_silver_to_postgres(conn):
    """Export Silver layer Parquet files to PostgreSQL."""
    logger.info("\n📥 Exporting Silver Layer to PostgreSQL...")
    
    cursor = conn.cursor()
    silver_files = list(SILVER_DIR.glob("*.parquet"))
    
    for parquet_file in silver_files:
        table_name = parquet_file.stem  # e.g., 'stocks', 'forex', etc.
        
        try:
            df = pd.read_parquet(parquet_file)
            
            if df.empty:
                logger.warning(f"  ⚠️ {table_name}: Empty file, skipping")
                continue
            
            # Create table and insert data
            pg_table = f"fame_analytics.silver_{table_name}"
            
            # Drop if exists and create new
            cursor.execute(f"DROP TABLE IF EXISTS {pg_table} CASCADE;")
            
            # Generate CREATE TABLE statement
            columns = []
            for col, dtype in df.dtypes.items():
                col_clean = col.replace(' ', '_').lower()
                if 'int' in str(dtype):
                    pg_type = 'BIGINT'
                elif 'float' in str(dtype):
                    pg_type = 'DOUBLE PRECISION'
                elif 'datetime' in str(dtype):
                    pg_type = 'TIMESTAMP'
                elif 'bool' in str(dtype):
                    pg_type = 'BOOLEAN'
                else:
                    pg_type = 'TEXT'
                columns.append(f'"{col_clean}" {pg_type}')
            
            create_sql = f"CREATE TABLE {pg_table} ({', '.join(columns)});"
            cursor.execute(create_sql)
            
            # Insert data using COPY for better performance
            from io import StringIO
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            col_names = [f'"{c.replace(" ", "_").lower()}"' for c in df.columns]
            cursor.copy_expert(
                f"COPY {pg_table} ({','.join(col_names)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buffer
            )
            
            conn.commit()
            logger.info(f"  ✅ {table_name}: {len(df):,} rows → {pg_table}")
            
        except Exception as e:
            logger.error(f"  ❌ {table_name}: {e}")
            conn.rollback()
    
    return len(silver_files)


def export_gold_to_postgres(conn):
    """Export Gold layer Parquet files to PostgreSQL."""
    logger.info("\n📥 Exporting Gold Layer to PostgreSQL...")
    
    cursor = conn.cursor()
    gold_files = list(GOLD_DIR.glob("*.parquet"))
    
    for parquet_file in gold_files:
        table_name = parquet_file.stem
        
        try:
            df = pd.read_parquet(parquet_file)
            
            if df.empty:
                logger.warning(f"  ⚠️ {table_name}: Empty file, skipping")
                continue
            
            pg_table = f"fame_analytics.gold_{table_name}"
            
            cursor.execute(f"DROP TABLE IF EXISTS {pg_table} CASCADE;")
            
            # Generate CREATE TABLE
            columns = []
            for col, dtype in df.dtypes.items():
                col_clean = col.replace(' ', '_').lower()
                if 'int' in str(dtype):
                    pg_type = 'BIGINT'
                elif 'float' in str(dtype):
                    pg_type = 'DOUBLE PRECISION'
                elif 'datetime' in str(dtype):
                    pg_type = 'TIMESTAMP'
                elif 'bool' in str(dtype):
                    pg_type = 'BOOLEAN'
                else:
                    pg_type = 'TEXT'
                columns.append(f'"{col_clean}" {pg_type}')
            
            create_sql = f"CREATE TABLE {pg_table} ({', '.join(columns)});"
            cursor.execute(create_sql)
            
            # Insert data
            from io import StringIO
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            col_names = [f'"{c.replace(" ", "_").lower()}"' for c in df.columns]
            cursor.copy_expert(
                f"COPY {pg_table} ({','.join(col_names)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buffer
            )
            
            conn.commit()
            logger.info(f"  ✅ {table_name}: {len(df):,} rows → {pg_table}")
            
        except Exception as e:
            logger.error(f"  ❌ {table_name}: {e}")
            conn.rollback()
    
    return len(gold_files)


def export_duckdb_to_postgres(conn):
    """Export DuckDB warehouse tables to PostgreSQL."""
    logger.info("\n📥 Exporting DuckDB Warehouse to PostgreSQL...")
    
    if not WAREHOUSE_PATH.exists():
        logger.warning("  ⚠️ DuckDB warehouse not found")
        return 0
    
    try:
        import duckdb
        duck_conn = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    except ImportError:
        logger.error("duckdb not installed")
        return 0
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        return 0
    
    cursor = conn.cursor()
    
    # Get all tables from DuckDB
    tables = duck_conn.execute("SHOW TABLES").fetchall()
    
    for (table_name,) in tables:
        try:
            df = duck_conn.execute(f"SELECT * FROM {table_name}").df()
            
            if df.empty:
                continue
            
            pg_table = f"fame_analytics.warehouse_{table_name}"
            
            cursor.execute(f"DROP TABLE IF EXISTS {pg_table} CASCADE;")
            
            # Generate CREATE TABLE
            columns = []
            for col, dtype in df.dtypes.items():
                col_clean = col.replace(' ', '_').lower()
                if 'int' in str(dtype):
                    pg_type = 'BIGINT'
                elif 'float' in str(dtype):
                    pg_type = 'DOUBLE PRECISION'
                elif 'datetime' in str(dtype):
                    pg_type = 'TIMESTAMP'
                elif 'bool' in str(dtype):
                    pg_type = 'BOOLEAN'
                else:
                    pg_type = 'TEXT'
                columns.append(f'"{col_clean}" {pg_type}')
            
            create_sql = f"CREATE TABLE {pg_table} ({', '.join(columns)});"
            cursor.execute(create_sql)
            
            # Insert data
            from io import StringIO
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            col_names = [f'"{c.replace(" ", "_").lower()}"' for c in df.columns]
            cursor.copy_expert(
                f"COPY {pg_table} ({','.join(col_names)}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buffer
            )
            
            conn.commit()
            logger.info(f"  ✅ {table_name}: {len(df):,} rows → {pg_table}")
            
        except Exception as e:
            logger.error(f"  ❌ {table_name}: {e}")
            conn.rollback()
    
    duck_conn.close()
    return len(tables)


def create_streaming_tables(conn):
    """Create tables for Kafka streaming data."""
    logger.info("\n📥 Creating Streaming Tables for Kafka...")
    
    cursor = conn.cursor()
    
    # Stock quotes streaming table
    cursor.execute("""
        DROP TABLE IF EXISTS fame_streaming.stock_quotes CASCADE;
        CREATE TABLE fame_streaming.stock_quotes (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            price DOUBLE PRECISION,
            change DOUBLE PRECISION,
            change_percent DOUBLE PRECISION,
            volume BIGINT,
            currency VARCHAR(10),
            market_cap BIGINT,
            timestamp TIMESTAMP DEFAULT NOW(),
            source VARCHAR(50) DEFAULT 'kafka'
        );
        CREATE INDEX idx_stock_quotes_symbol ON fame_streaming.stock_quotes(symbol);
        CREATE INDEX idx_stock_quotes_timestamp ON fame_streaming.stock_quotes(timestamp);
    """)
    
    # Forex rates streaming table
    cursor.execute("""
        DROP TABLE IF EXISTS fame_streaming.forex_rates CASCADE;
        CREATE TABLE fame_streaming.forex_rates (
            id SERIAL PRIMARY KEY,
            base_currency VARCHAR(10),
            target_currency VARCHAR(10),
            rate DOUBLE PRECISION,
            timestamp TIMESTAMP DEFAULT NOW(),
            source VARCHAR(50) DEFAULT 'kafka'
        );
        CREATE INDEX idx_forex_rates_currencies ON fame_streaming.forex_rates(base_currency, target_currency);
    """)
    
    # Alerts streaming table
    cursor.execute("""
        DROP TABLE IF EXISTS fame_streaming.alerts CASCADE;
        CREATE TABLE fame_streaming.alerts (
            id SERIAL PRIMARY KEY,
            alert_type VARCHAR(50),
            symbol VARCHAR(20),
            message TEXT,
            value DOUBLE PRECISION,
            threshold DOUBLE PRECISION,
            severity VARCHAR(20) DEFAULT 'INFO',
            timestamp TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX idx_alerts_type ON fame_streaming.alerts(alert_type);
        CREATE INDEX idx_alerts_timestamp ON fame_streaming.alerts(timestamp);
    """)
    
    conn.commit()
    logger.info("  ✅ Created: stock_quotes, forex_rates, alerts")
    return 3


def create_analytics_views(conn):
    """Create analytical views for Superset dashboards."""
    logger.info("\n📊 Creating Analytics Views for Superset...")
    
    cursor = conn.cursor()
    
    # View: Market Overview
    cursor.execute("""
        DROP VIEW IF EXISTS fame_analytics.v_market_overview CASCADE;
        CREATE OR REPLACE VIEW fame_analytics.v_market_overview AS
        SELECT 
            symbol,
            price as current_price,
            change,
            change_percent,
            volume,
            currency,
            market_cap,
            timestamp as last_update
        FROM fame_streaming.stock_quotes
        WHERE timestamp = (
            SELECT MAX(timestamp) 
            FROM fame_streaming.stock_quotes sq2 
            WHERE sq2.symbol = stock_quotes.symbol
        );
    """)
    logger.info("  ✅ View: v_market_overview")
    
    # View: Transaction Summary
    cursor.execute("""
        DROP VIEW IF EXISTS fame_analytics.v_transaction_summary CASCADE;
        CREATE OR REPLACE VIEW fame_analytics.v_transaction_summary AS
        SELECT 
            DATE(created_at) as date,
            transaction_type,
            COUNT(*) as num_transactions,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            currency
        FROM transactions
        GROUP BY DATE(created_at), transaction_type, currency
        ORDER BY date DESC;
    """)
    logger.info("  ✅ View: v_transaction_summary")
    
    # View: Daily Alerts
    cursor.execute("""
        DROP VIEW IF EXISTS fame_analytics.v_daily_alerts CASCADE;
        CREATE OR REPLACE VIEW fame_analytics.v_daily_alerts AS
        SELECT 
            DATE(timestamp) as date,
            alert_type,
            COUNT(*) as alert_count,
            AVG(value) as avg_value
        FROM fame_streaming.alerts
        GROUP BY DATE(timestamp), alert_type
        ORDER BY date DESC;
    """)
    logger.info("  ✅ View: v_daily_alerts")
    
    conn.commit()
    return 3


def insert_sample_streaming_data(conn):
    """Insert sample streaming data for demo."""
    logger.info("\n📥 Inserting Sample Streaming Data...")
    
    cursor = conn.cursor()
    
    # Sample stock quotes
    stocks = [
        ('AAPL', 259.37, 0.33, 0.13, 45000000, 'USD', 4000000000000),
        ('MSFT', 479.28, 1.17, 0.24, 22000000, 'USD', 3500000000000),
        ('GOOGL', 328.57, 3.13, 0.96, 18000000, 'USD', 2000000000000),
        ('AMZN', 247.38, 1.09, 0.44, 35000000, 'USD', 2500000000000),
        ('META', 653.06, 7.00, 1.08, 15000000, 'USD', 1600000000000),
        ('NVDA', 184.86, -0.18, -0.10, 50000000, 'USD', 4500000000000),
        ('BNP.PA', 87.20, 4.70, 5.70, 5000000, 'EUR', 90000000000),
        ('SAN.MC', 10.25, 0.04, 0.39, 8000000, 'EUR', 85000000000),
        ('DBK.DE', 33.06, -0.17, -0.51, 3000000, 'EUR', 45000000000),
        ('HSBA.L', 1194.20, -1.20, -0.10, 12000000, 'GBp', 180000000000),
    ]
    
    for stock in stocks:
        cursor.execute("""
            INSERT INTO fame_streaming.stock_quotes 
            (symbol, price, change, change_percent, volume, currency, market_cap)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, stock)
    
    # Sample alerts
    alerts = [
        ('PRICE_SPIKE', 'BNP.PA', 'BNP Paribas moved 5.70%', 5.70, 5.0, 'WARNING'),
        ('VOLUME_SURGE', 'NVDA', 'NVIDIA volume surge detected', 50000000, 40000000, 'INFO'),
        ('PRICE_DROP', 'DBK.DE', 'Deutsche Bank price drop', -0.51, -0.5, 'INFO'),
    ]
    
    for alert in alerts:
        cursor.execute("""
            INSERT INTO fame_streaming.alerts 
            (alert_type, symbol, message, value, threshold, severity)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, alert)
    
    conn.commit()
    logger.info(f"  ✅ Inserted {len(stocks)} stock quotes, {len(alerts)} alerts")


def print_superset_summary(conn):
    """Print summary of data available in Superset."""
    cursor = conn.cursor()
    
    print("\n" + "=" * 70)
    print("📊 FAME Data Space - Superset Data Summary")
    print("=" * 70)
    
    # Count tables in each schema
    schemas = ['fame_analytics', 'fame_streaming', 'public']
    
    for schema in schemas:
        cursor.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'
        """)
        tables = cursor.fetchall()
        
        print(f"\n📁 Schema: {schema}")
        print("-" * 40)
        
        for (table_name,) in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            count = cursor.fetchone()[0]
            print(f"   📄 {table_name}: {count:,} rows")
    
    # Views
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.views 
        WHERE table_schema = 'fame_analytics'
    """)
    views = cursor.fetchall()
    
    if views:
        print(f"\n📊 Analytics Views:")
        print("-" * 40)
        for (view_name,) in views:
            print(f"   📈 {view_name}")
    
    print("\n" + "=" * 70)
    print("🔗 Superset Connection Details:")
    print("=" * 70)
    print(f"""
   URL:      http://localhost:8088
   Username: admin
   Password: admin123
   
   Database Connection (PostgreSQL):
   ─────────────────────────────────
   Host:     postgres (or localhost from outside Docker)
   Port:     5432
   Database: fame_transactions
   Username: fame_user
   Password: fame_password
   
   SQLAlchemy URI:
   postgresql://fame_user:fame_password@postgres:5432/fame_transactions
""")


def main():
    """Main function to export all data to PostgreSQL for Superset."""
    print("=" * 70)
    print("🚀 FAME Data Space - Superset Integration")
    print("=" * 70)
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Connect to PostgreSQL
    conn = get_pg_connection()
    if not conn:
        logger.error("❌ Cannot connect to PostgreSQL")
        return 1
    
    logger.info("✅ Connected to PostgreSQL")
    
    try:
        # Create schemas
        create_superset_schema(conn)
        
        # Export Silver layer
        export_silver_to_postgres(conn)
        
        # Export Gold layer
        export_gold_to_postgres(conn)
        
        # Export DuckDB warehouse
        export_duckdb_to_postgres(conn)
        
        # Create streaming tables
        create_streaming_tables(conn)
        
        # Create analytics views
        create_analytics_views(conn)
        
        # Insert sample streaming data
        insert_sample_streaming_data(conn)
        
        # Print summary
        print_superset_summary(conn)
        
        logger.info("\n✅ Superset integration complete!")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
