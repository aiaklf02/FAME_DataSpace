"""
FAME Data Space - Main Entry Point
====================================
Run the complete FAME Data Space EtLT pipeline.

Architecture: Data Lake + Data Fabric + Data Warehouse
Pattern: EtLT (Extract, light Transform, Load, Transform)
"""

import os
import sys
import argparse
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for FAME Data Space."""
    parser = argparse.ArgumentParser(description='FAME Data Space - EtLT Pipeline')
    parser.add_argument('command', choices=['pipeline', 'dashboard', 'sources', 'rdf', 'fabric', 'warehouse', 'streaming', 'help'],
                       help='Command to run')
    parser.add_argument('--skip-extract', action='store_true', help='Skip extraction phase')
    parser.add_argument('--skip-load', action='store_true', help='Skip loading phase')
    parser.add_argument('--skip-transform', action='store_true', help='Skip transformation phase')
    parser.add_argument('--mode', choices=['produce', 'consume', 'spark'], default='produce',
                       help='Streaming mode (produce/consume/spark)')
    
    args = parser.parse_args()
    
    if args.command == 'pipeline':
        print("=" * 70)
        print("🏦 FAME Data Space - EtLT Pipeline")
        print("   Architecture: Data Lake + Data Fabric + Data Warehouse")
        print("=" * 70)
        
        from elt.main_pipeline import FAMEPipeline
        
        pipeline = FAMEPipeline()
        pipeline.run(
            skip_extract=args.skip_extract,
            skip_load=args.skip_load,
            skip_transform=args.skip_transform
        )
    
    elif args.command == 'dashboard':
        print("=" * 70)
        print("🏦 FAME Data Space - Dashboard")
        print("=" * 70)
        print("\nStarting Streamlit dashboard...")
        print("Open http://localhost:8501 in your browser")
        print()
        
        os.system('streamlit run prototype/app.py')
    
    elif args.command == 'sources':
        print("=" * 70)
        print("🏦 FAME Data Space - Data Sources Test")
        print("=" * 70)
        
        from sources import (
            StockAPISource, ECBExchangeRateSource, 
            FinancialsCSVSource, TransactionDBSource
        )
        
        print("\n📈 Testing Source 1: Stock API...")
        stock_source = StockAPISource()
        stock_data = stock_source.fetch_batch_quotes(['AAPL', 'MSFT', 'GOOGL'])
        print(f"   Fetched {len(stock_data)} stock quotes")
        
        print("\n💱 Testing Source 2: ECB XML...")
        ecb_source = ECBExchangeRateSource()
        ecb_data = ecb_source.fetch_latest_rates()
        print(f"   Fetched {len(ecb_data)} exchange rates")
        
        print("\n📊 Testing Source 3: CSV Financials...")
        csv_source = FinancialsCSVSource()
        csv_source.generate_sample_data()
        csv_data = csv_source.read_all_financials()
        print(f"   Generated {len(csv_data)} financial records")
        
        print("\n💳 Testing Source 4: Transaction DB...")
        tx_source = TransactionDBSource()
        tx_source.generate_sample_data()
        tx_data = tx_source.get_transactions(limit=10)
        print(f"   Generated sample transactions")
        
        print("\n✅ All sources tested successfully!")
    
    elif args.command == 'rdf':
        print("=" * 70)
        print("🏦 FAME Data Space - RDF Generation")
        print("=" * 70)
        
        from semantic import FAMERDFGenerator
        import pandas as pd
        
        generator = FAMERDFGenerator()
        
        # Generate sample RDF
        print("\n🔄 Generating sample RDF data...")
        
        stock_data = pd.DataFrame([
            {"symbol": "AAPL", "company_name": "Apple Inc.", "price": 185.50, 
             "volume": 50000000, "currency": "USD", "exchange": "NASDAQ", "source": "api"}
        ])
        
        generator.add_stock_data(stock_data)
        
        os.makedirs('data/rdf', exist_ok=True)
        generator.save_rdf('data/rdf/fame_sample.ttl', format='turtle')
        
        print("\n📊 RDF Graph Statistics:")
        for key, value in generator.get_statistics().items():
            print(f"   {key}: {value}")
        
        print("\n✅ RDF generation complete!")
    
    elif args.command == 'fabric':
        print("=" * 70)
        print("🔗 FAME Data Space - Data Fabric")
        print("=" * 70)
        
        from fabric import FAMEDataFabric
        
        fabric = FAMEDataFabric()
        fabric.register_elt_assets()
        
        print("\n📊 Data Fabric Summary:")
        summary = fabric.get_summary()
        print(f"   Total Assets: {summary['catalog']['total_assets']}")
        print(f"   By Domain: {summary['catalog']['by_domain']}")
        print(f"   By Layer: {summary['catalog']['by_layer']}")
        print(f"   Lineage Records: {summary['lineage_count']}")
        print(f"   Quality Rules: {summary['quality_rules']}")
        
        print("\n✅ Data Fabric initialized!")
    
    elif args.command == 'warehouse':
        print("=" * 70)
        print("🏢 FAME Data Space - Data Warehouse (DuckDB)")
        print("=" * 70)
        
        from elt.warehouse import FAMEWarehouse
        
        warehouse = FAMEWarehouse()
        
        print("\n📋 Warehouse Tables:")
        for table in warehouse.list_tables():
            count = warehouse.get_row_count(table)
            print(f"   {table}: {count} rows")
        
        print("\n📈 KPIs:")
        kpis = warehouse.get_kpis()
        for key, value in kpis.items():
            print(f"   {key}: {value}")
        
        warehouse.close()
        print("\n✅ Warehouse query complete!")
    
    elif args.command == 'streaming':
        print("=" * 70)
        print("🔄 FAME Data Space - Real-time Streaming (Kafka + Spark)")
        print("=" * 70)
        
        if args.mode == 'produce':
            print("\n📤 Starting Kafka Producer...")
            print("   Streaming real-time stock data to Kafka topics")
            print("   Topics: fame-stocks, fame-forex, fame-transactions")
            print("   Press Ctrl+C to stop\n")
            
            from streaming.kafka_producer import FAMEKafkaProducer
            
            with FAMEKafkaProducer() as producer:
                producer.stream_real_stocks(interval_seconds=15)
        
        elif args.mode == 'consume':
            print("\n📥 Starting Kafka Consumer...")
            print("   Listening to Kafka topics...")
            print("   Press Ctrl+C to stop\n")
            
            from streaming.kafka_consumer import FAMEKafkaConsumer, stock_handler, transaction_handler, alert_handler
            
            with FAMEKafkaConsumer() as consumer:
                consumer.register_handler("fame-stocks", stock_handler)
                consumer.register_handler("fame-transactions", transaction_handler)
                consumer.register_handler("fame-alerts", alert_handler)
                consumer.consume()
        
        elif args.mode == 'spark':
            print("\n⚡ Starting Spark Structured Streaming...")
            print("   Processing real-time data with Apache Spark")
            print("   Press Ctrl+C to stop\n")
            
            from streaming.spark_streaming import FAMESparkStreaming
            
            streaming = FAMESparkStreaming()
            try:
                streaming.start_stock_pipeline()
            finally:
                streaming.stop()
    
    elif args.command == 'help':
        print("=" * 70)
        print("🏦 FAME Data Space - Help")
        print("=" * 70)
        print("""
Architecture: Data Lake + Data Fabric + Data Warehouse
Pattern: EtLT (Extract, light Transform, Load, Transform)

Available Commands:
-------------------
  pipeline   - Run the complete EtLT pipeline
  dashboard  - Start the Streamlit visualization dashboard
  sources    - Test all 4 data source connectors
  rdf        - Generate sample RDF data
  fabric     - Initialize and view Data Fabric (catalog, lineage, quality)
  warehouse  - Query the DuckDB Data Warehouse
  help       - Show this help message

Examples:
---------
  python main.py pipeline                # Run full EtLT pipeline
  python main.py pipeline --skip-load    # Only Extract
  python main.py pipeline --skip-extract # Only Load + Transform
  python main.py dashboard               # Start dashboard
  python main.py warehouse               # Query warehouse
  python main.py fabric                  # View data catalog

EtLT vs ETL:
------------
  ETL:  Extract → Transform → Load (transform before load)
  EtLT: Extract → Load → Transform (transform IN warehouse)
  
  EtLT is better for:
  - Big data (warehouse compute is scalable)
  - Reprocessing (raw data preserved)
  - FAME architecture (DuckDB transforms)

Docker:
-------
  docker-compose up -d                 # Start all services
  docker-compose logs -f fame-elt     # View ELT logs
  docker-compose down                  # Stop all services

URLs:
-----
  Dashboard:      http://localhost:8501
  Kafka UI:       http://localhost:8080
  MinIO Console:  http://localhost:9001
  Fuseki:         http://localhost:3030
""")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
