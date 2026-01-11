"""
FAME Data Space - ELT Extract Module
=====================================
Extract data from all 4 heterogeneous sources.

Architecture:
- Source 1 (API/JSON): Yahoo Finance REAL-TIME - no file storage
- Source 2 (XML): ECB existing file - ecb_historical_20years.xml
- Source 3 (CSV): Existing files - sp500, gdp, nasdaq, nyse
- Source 4 (SQL): PostgreSQL DIRECT - no file storage
"""

import os
import json
import pandas as pd
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Result of data extraction."""
    source_name: str
    source_type: str  # api, xml, csv, sql
    record_count: int
    extracted_at: datetime
    file_path: Optional[str]  # None for real-time sources
    status: str
    metadata: Dict[str, Any]
    data: List[Dict] = field(default_factory=list)  # Holds the actual data


class FAMEExtractor:
    """
    ELT Extractor - Extract data from 4 heterogeneous sources.
    
    Sources:
    - Source 1: Stock Market API (JSON) - Yahoo Finance REAL-TIME
    - Source 2: ECB Exchange Rates (XML) - Existing file
    - Source 3: Company Financials (CSV) - Existing files
    - Source 4: Transactions Database (SQL) - PostgreSQL DIRECT
    """
    
    def __init__(self, data_lake_path: str = "data"):
        """Initialize extractor with data lake path."""
        self.data_lake_path = data_lake_path
        self.bronze_path = os.path.join(data_lake_path, "bronze")
        self._ensure_directories()
        
    def _ensure_directories(self):
        """Create necessary directories."""
        for subdir in ["api", "xml", "csv", "sql"]:
            os.makedirs(os.path.join(self.bronze_path, subdir), exist_ok=True)
    
    # =========================================================================
    # SOURCE 1: Stock Market API (JSON) - YAHOO FINANCE REAL-TIME
    # No file created - data fetched live from Internet
    # =========================================================================
    
    def extract_stock_api(self, symbols: List[str] = None) -> ExtractionResult:
        """
        Extract REAL-TIME stock data from Yahoo Finance API.
        
        NO FILE CREATED - Data is fetched live and returned directly.
        """
        logger.info("📈 Extracting Source 1: Yahoo Finance API (REAL-TIME)...")
        
        if symbols is None:
            symbols = [
                "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META",  # US Tech
                "JPM", "BAC", "GS",                                # US Banks  
                "BNP.PA", "DBK.DE", "INGA.AS",                     # EU Banks
                "ADYEN.AS"                                         # EU Fintech
            ]
        
        raw_data = []
        
        try:
            from sources.real_data_fetcher import RealDataFetcher
            fetcher = RealDataFetcher()
            raw_data = fetcher.fetch_real_stocks(symbols)
            logger.info(f"✅ Fetched REAL-TIME data for {len(raw_data)} stocks from Yahoo Finance")
        except Exception as e:
            logger.error(f"❌ Yahoo Finance API failed: {e}")
            raise Exception(f"Real-time API unavailable: {e}")
        
        # Add extraction metadata
        for record in raw_data:
            record["_extracted_at"] = datetime.now().isoformat()
            record["_source"] = "yahoo_finance_api"
            record["_source_type"] = "api"
            record["_is_real_time"] = True
        
        return ExtractionResult(
            source_name="Yahoo Finance API (REAL-TIME)",
            source_type="api",
            record_count=len(raw_data),
            extracted_at=datetime.now(),
            file_path=None,  # No file - real-time
            status="success",
            metadata={"symbols": symbols, "format": "json", "mode": "real_time"},
            data=raw_data
        )
    
    # =========================================================================
    # SOURCE 2: ECB Exchange Rates (XML) - EXISTING FILE
    # Uses: data/bronze/xml/ecb_historical_20years.xml
    # =========================================================================
    
    def extract_ecb_xml(self) -> ExtractionResult:
        """
        Extract ECB exchange rates from EXISTING XML file.
        
        Uses: data/bronze/xml/ecb_historical_20years.xml (20 years of history)
        """
        logger.info("💱 Extracting Source 2: ECB XML (existing file)...")
        
        # Path to existing ECB XML file
        ecb_xml_path = os.path.join(self.bronze_path, "xml", "ecb_historical_20years.xml")
        
        if not os.path.exists(ecb_xml_path):
            raise FileNotFoundError(f"ECB XML file not found: {ecb_xml_path}")
        
        # Parse the existing XML file
        raw_data = self._parse_ecb_xml(ecb_xml_path)
        logger.info(f"✅ Parsed {len(raw_data)} exchange rates from existing ECB XML")
        
        # Add extraction metadata
        for record in raw_data:
            record["_extracted_at"] = datetime.now().isoformat()
            record["_source"] = "ecb_official_xml"
            record["_source_type"] = "xml"
            record["_is_real_data"] = True
        
        return ExtractionResult(
            source_name="ECB Exchange Rates (XML File)",
            source_type="xml",
            record_count=len(raw_data),
            extracted_at=datetime.now(),
            file_path=ecb_xml_path,  # Reference existing file
            status="success",
            metadata={"base_currency": "EUR", "format": "xml", "years": 20},
            data=raw_data
        )
    
    def _parse_ecb_xml(self, xml_path: str) -> List[Dict]:
        """Parse the ECB historical XML file."""
        import xml.etree.ElementTree as ET
        
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # ECB XML namespaces
        ns = {
            'gesmes': 'http://www.gesmes.org/xml/2002-08-01',
            'eurofxref': 'http://www.ecb.int/vocabulary/2002-08-01/eurofxref'
        }
        
        data = []
        
        # Parse each time period
        for cube in root.findall('.//eurofxref:Cube[@time]', ns):
            date = cube.get('time')
            
            for rate_cube in cube.findall('eurofxref:Cube', ns):
                currency = rate_cube.get('currency')
                rate = rate_cube.get('rate')
                
                if currency and rate:
                    data.append({
                        "base_currency": "EUR",
                        "target_currency": currency,
                        "rate": float(rate),
                        "reference_date": date
                    })
        
        return data
    
    # =========================================================================
    # SOURCE 3: Company Financials (CSV) - EXISTING FILES
    # Uses: data/bronze/csv/sp500_companies.csv, nasdaq_listings.csv, etc.
    # =========================================================================
    
    def extract_financials_csv(self) -> ExtractionResult:
        """
        Extract financial data from EXISTING CSV files.
        
        Uses existing files:
        - sp500_companies.csv
        - world_gdp.csv
        - nasdaq_listings.csv
        - nyse_listings.csv
        """
        logger.info("📊 Extracting Source 3: CSV Files (existing)...")
        
        raw_data = []
        files_used = []
        
        # CSV files to read
        csv_files = {
            "sp500": "sp500_companies.csv",
            "gdp": "world_gdp.csv",
            "nasdaq": "nasdaq_listings.csv",
            "nyse": "nyse_listings.csv",
        }
        
        for source_name, filename in csv_files.items():
            filepath = os.path.join(self.bronze_path, "csv", filename)
            
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                records = df.to_dict('records')
                
                # Add source identifier
                for record in records:
                    record["_csv_source"] = source_name
                
                raw_data.extend(records)
                files_used.append(filename)
                logger.info(f"   ✅ Loaded {len(records)} records from {filename}")
        
        if not raw_data:
            raise FileNotFoundError("No CSV files found in bronze/csv/")
        
        logger.info(f"✅ Loaded {len(raw_data)} total records from {len(files_used)} CSV files")
        
        # Add extraction metadata
        for record in raw_data:
            record["_extracted_at"] = datetime.now().isoformat()
            record["_source"] = "financial_csv_files"
            record["_source_type"] = "csv"
            record["_is_real_data"] = True
        
        return ExtractionResult(
            source_name="Financial CSV Files (S&P500, GDP, NASDAQ, NYSE)",
            source_type="csv",
            record_count=len(raw_data),
            extracted_at=datetime.now(),
            file_path=os.path.join(self.bronze_path, "csv"),  # Reference directory
            status="success",
            metadata={"files": files_used, "format": "csv"},
            data=raw_data
        )
    
    # =========================================================================
    # SOURCE 4: Transactions Database (SQL) - POSTGRESQL DIRECT
    # No file created - data queried directly from PostgreSQL
    # =========================================================================
    
    def extract_transactions_sql(self, limit: int = 1000) -> ExtractionResult:
        """
        Extract transactions DIRECTLY from PostgreSQL.
        
        NO FILE CREATED - Data is queried live from database.
        """
        logger.info("💳 Extracting Source 4: PostgreSQL (DIRECT query)...")
        
        raw_data = []
        
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="fame_transactions",
                user="fame_user",
                password="fame_password"
            )
            
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT 
                        transaction_id,
                        amount,
                        currency,
                        amount_eur,
                        sender_id,
                        sender_name,
                        sender_country,
                        sender_iban,
                        sender_bank,
                        receiver_id,
                        receiver_name,
                        receiver_country,
                        receiver_iban,
                        receiver_bank,
                        transaction_type,
                        description,
                        reference,
                        channel,
                        status,
                        is_cross_border,
                        created_at,
                        processed_at
                    FROM transactions
                    ORDER BY created_at DESC
                    LIMIT {limit}
                """)
                raw_data = [dict(row) for row in cur.fetchall()]
            
            conn.close()
            logger.info(f"✅ Extracted {len(raw_data)} transactions from PostgreSQL")
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            raise Exception(f"Database unavailable: {e}")
        
        # Add extraction metadata
        for record in raw_data:
            record["_extracted_at"] = datetime.now().isoformat()
            record["_source"] = "postgresql_direct"
            record["_source_type"] = "sql"
            record["_is_real_time"] = True
        
        return ExtractionResult(
            source_name="PostgreSQL Transactions (DIRECT)",
            source_type="sql",
            record_count=len(raw_data),
            extracted_at=datetime.now(),
            file_path=None,  # No file - direct query
            status="success",
            metadata={"database": "fame_transactions", "format": "sql", "mode": "direct"},
            data=raw_data
        )
    
    # =========================================================================
    # EXTRACT ALL
    # =========================================================================
    
    def extract_all(self) -> List[ExtractionResult]:
        """Extract data from all 4 sources."""
        logger.info("=" * 60)
        logger.info("🚀 FAME ELT - EXTRACT PHASE")
        logger.info("=" * 60)
        
        results = []
        
        # Source 1: API (Yahoo Finance - Real-time)
        results.append(self.extract_stock_api())
        
        # Source 2: XML (ECB - Existing file)
        results.append(self.extract_ecb_xml())
        
        # Source 3: CSV (Financial data - Existing files)
        results.append(self.extract_financials_csv())
        
        # Source 4: SQL (PostgreSQL - Direct)
        results.append(self.extract_transactions_sql())
        
        # Summary
        total_records = sum(r.record_count for r in results)
        logger.info(f"\n✅ Extraction complete: {total_records} records from 4 sources")
        
        return results


# CLI Test
if __name__ == "__main__":
    extractor = FAMEExtractor()
    results = extractor.extract_all()
    
    print("\n📋 Extraction Summary:")
    for r in results:
        file_info = r.file_path if r.file_path else "REAL-TIME (no file)"
        print(f"   {r.source_name}: {r.record_count} records")
        print(f"      → {file_info}")
