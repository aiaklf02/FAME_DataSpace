"""
FAME Data Space - Source 3: Company Financials (CSV Files)
===========================================================
Quarterly financial statements from CSV files

Data Type: File System
Format: CSV
Frequency: Quarterly (batch processing)
Volume: ~500 companies × 4 quarters = 2000 records/year
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import logging
import random

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CompanyFinancials:
    """Data class for company financial data."""
    source: str
    source_type: str
    timestamp: str
    company_id: str
    company_name: str
    ticker: str
    sector: str
    country: str
    fiscal_year: int
    fiscal_quarter: str
    # Income Statement
    revenue: float
    gross_profit: float
    operating_income: float
    net_income: float
    eps: float  # Earnings per Share
    # Balance Sheet
    total_assets: float
    total_liabilities: float
    total_equity: float
    cash_and_equivalents: float
    total_debt: float
    # Ratios
    profit_margin: float
    roe: float  # Return on Equity
    debt_to_equity: float
    current_ratio: float


class CompanyFinancialsCSVConnector:
    """
    SOURCE 3: Company Financial Statements (CSV)
    
    Features:
    - Reads quarterly financial reports from CSV
    - Validates and cleans financial data
    - Calculates financial ratios
    - Batch processing for Data Lake
    """
    
    KAFKA_TOPIC = "fame.financials.quarterly"
    
    # Sample companies for data generation
    COMPANIES = [
        {"id": "FR001", "name": "BNP Paribas SA", "ticker": "BNP.PA", "sector": "Banking", "country": "France"},
        {"id": "ES001", "name": "Banco Santander SA", "ticker": "SAN.MC", "sector": "Banking", "country": "Spain"},
        {"id": "DE001", "name": "Deutsche Bank AG", "ticker": "DBK.DE", "sector": "Banking", "country": "Germany"},
        {"id": "UK001", "name": "HSBC Holdings plc", "ticker": "HSBA.L", "sector": "Banking", "country": "UK"},
        {"id": "FR002", "name": "AXA SA", "ticker": "CS.PA", "sector": "Insurance", "country": "France"},
        {"id": "DE002", "name": "Allianz SE", "ticker": "ALV.DE", "sector": "Insurance", "country": "Germany"},
        {"id": "NL001", "name": "ING Groep NV", "ticker": "INGA.AS", "sector": "Banking", "country": "Netherlands"},
        {"id": "IT001", "name": "Intesa Sanpaolo SpA", "ticker": "ISP.MI", "sector": "Banking", "country": "Italy"},
        {"id": "CH001", "name": "UBS Group AG", "ticker": "UBSG.SW", "sector": "Banking", "country": "Switzerland"},
        {"id": "US001", "name": "JPMorgan Chase & Co", "ticker": "JPM", "sector": "Banking", "country": "USA"},
        {"id": "US002", "name": "Goldman Sachs Group", "ticker": "GS", "sector": "Investment Banking", "country": "USA"},
        {"id": "US003", "name": "Visa Inc", "ticker": "V", "sector": "Payments", "country": "USA"},
        {"id": "US004", "name": "Mastercard Inc", "ticker": "MA", "sector": "Payments", "country": "USA"},
        {"id": "US005", "name": "PayPal Holdings", "ticker": "PYPL", "sector": "Fintech", "country": "USA"},
        {"id": "SE001", "name": "Klarna Bank AB", "ticker": "KLARNA", "sector": "Fintech", "country": "Sweden"},
    ]
    
    def __init__(self, data_dir: str = "data/raw/csv", kafka_servers: str = "localhost:29092"):
        """Initialize CSV connector."""
        self.data_dir = data_dir
        self.kafka_servers = kafka_servers
        self.producer = None
        
        os.makedirs(data_dir, exist_ok=True)
        
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
            logger.info("✅ Kafka producer connected for financials")
        except Exception as e:
            logger.warning(f"⚠️ Kafka connection failed: {e}")
    
    def _publish_to_kafka(self, key: str, data: Dict):
        """Publish to Kafka."""
        if self.producer:
            try:
                self.producer.send(self.KAFKA_TOPIC, key=key, value=data)
            except Exception as e:
                logger.error(f"Kafka error: {e}")
    
    def generate_sample_csv_data(self, years: int = 2) -> str:
        """
        Generate realistic sample financial data CSV.
        
        Args:
            years: Number of years of data to generate
            
        Returns:
            Path to generated CSV file
        """
        all_data = []
        
        quarters = ["Q1", "Q2", "Q3", "Q4"]
        current_year = datetime.now().year
        
        for company in self.COMPANIES:
            # Base financials (varies by sector)
            if company["sector"] == "Banking":
                base_revenue = random.uniform(20_000, 100_000)  # Millions
                margin_range = (0.15, 0.35)
            elif company["sector"] == "Insurance":
                base_revenue = random.uniform(30_000, 80_000)
                margin_range = (0.05, 0.15)
            elif company["sector"] == "Payments":
                base_revenue = random.uniform(5_000, 30_000)
                margin_range = (0.40, 0.55)
            elif company["sector"] == "Fintech":
                base_revenue = random.uniform(1_000, 10_000)
                margin_range = (-0.20, 0.20)
            else:
                base_revenue = random.uniform(10_000, 50_000)
                margin_range = (0.10, 0.25)
            
            for year in range(current_year - years, current_year + 1):
                for quarter in quarters:
                    # Skip future quarters
                    if year == current_year and quarters.index(quarter) > (datetime.now().month - 1) // 3:
                        continue
                    
                    # Seasonal and growth variations
                    growth = 1 + random.uniform(-0.05, 0.10)
                    seasonal = 1 + (0.05 if quarter == "Q4" else -0.02 if quarter == "Q1" else 0)
                    
                    revenue = round(base_revenue * growth * seasonal / 4, 2)
                    profit_margin = random.uniform(*margin_range)
                    
                    # Calculate financials
                    gross_profit = round(revenue * random.uniform(0.3, 0.7), 2)
                    operating_income = round(revenue * (profit_margin + 0.05), 2)
                    net_income = round(revenue * profit_margin, 2)
                    
                    # Balance sheet items
                    total_assets = round(revenue * random.uniform(15, 30), 2)
                    total_equity = round(total_assets * random.uniform(0.05, 0.15), 2)
                    total_liabilities = round(total_assets - total_equity, 2)
                    total_debt = round(total_liabilities * random.uniform(0.3, 0.6), 2)
                    cash = round(total_assets * random.uniform(0.05, 0.20), 2)
                    
                    # Shares and EPS
                    shares = random.uniform(500, 5000)  # Millions
                    eps = round(net_income / shares, 2)
                    
                    # Ratios
                    roe = round(net_income / total_equity * 100, 2) if total_equity > 0 else 0
                    debt_to_equity = round(total_debt / total_equity, 2) if total_equity > 0 else 0
                    current_ratio = round(random.uniform(0.8, 1.5), 2)
                    
                    all_data.append({
                        "company_id": company["id"],
                        "company_name": company["name"],
                        "ticker": company["ticker"],
                        "sector": company["sector"],
                        "country": company["country"],
                        "fiscal_year": year,
                        "fiscal_quarter": quarter,
                        "revenue_millions": revenue,
                        "gross_profit_millions": gross_profit,
                        "operating_income_millions": operating_income,
                        "net_income_millions": net_income,
                        "eps": eps,
                        "total_assets_millions": total_assets,
                        "total_liabilities_millions": total_liabilities,
                        "total_equity_millions": total_equity,
                        "cash_millions": cash,
                        "total_debt_millions": total_debt,
                        "profit_margin_pct": round(profit_margin * 100, 2),
                        "roe_pct": roe,
                        "debt_to_equity": debt_to_equity,
                        "current_ratio": current_ratio
                    })
                    
                    base_revenue *= growth  # Compound growth
        
        # Create DataFrame and save
        df = pd.DataFrame(all_data)
        filepath = os.path.join(self.data_dir, "company_financials.csv")
        df.to_csv(filepath, index=False)
        
        logger.info(f"📊 Generated {len(df)} financial records in {filepath}")
        return filepath
    
    def read_csv(self, filepath: str = None) -> pd.DataFrame:
        """
        Read financial data from CSV file.
        
        Args:
            filepath: Path to CSV file (uses default if None)
            
        Returns:
            DataFrame with financial data
        """
        if filepath is None:
            filepath = os.path.join(self.data_dir, "company_financials.csv")
        
        if not os.path.exists(filepath):
            logger.info("CSV file not found. Generating sample data...")
            self.generate_sample_csv_data()
        
        df = pd.read_csv(filepath)
        logger.info(f"📖 Read {len(df)} records from {filepath}")
        
        return df
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean financial data.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("🔍 Validating financial data...")
        
        # Check for missing values
        missing = df.isnull().sum()
        if missing.any():
            logger.warning(f"Missing values found: {missing[missing > 0].to_dict()}")
        
        # Fill missing values
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
        
        # Validate ratios
        df.loc[df['profit_margin_pct'] > 100, 'profit_margin_pct'] = 100
        df.loc[df['profit_margin_pct'] < -100, 'profit_margin_pct'] = -100
        
        # Add metadata
        df['source'] = 'csv_file'
        df['source_type'] = 'CSV'
        df['processed_at'] = datetime.now().isoformat()
        
        logger.info(f"✅ Validation complete. {len(df)} records processed.")
        return df
    
    def process_batch(self, publish_kafka: bool = True) -> pd.DataFrame:
        """
        Process batch of financial data.
        
        Args:
            publish_kafka: Whether to publish to Kafka
            
        Returns:
            Processed DataFrame
        """
        # Read and validate
        df = self.read_csv()
        df = self.validate_data(df)
        
        # Publish to Kafka if enabled
        if publish_kafka and self.producer:
            for _, row in df.iterrows():
                key = f"{row['company_id']}_{row['fiscal_year']}_{row['fiscal_quarter']}"
                self._publish_to_kafka(key, row.to_dict())
            
            self.producer.flush()
            logger.info(f"📤 Published {len(df)} records to Kafka")
        
        return df
    
    def get_company_summary(self, df: pd.DataFrame, ticker: str) -> Dict:
        """Get summary statistics for a company."""
        company_df = df[df['ticker'] == ticker]
        
        if company_df.empty:
            return {"error": f"Company {ticker} not found"}
        
        latest = company_df.sort_values(['fiscal_year', 'fiscal_quarter']).iloc[-1]
        
        return {
            "company_name": latest['company_name'],
            "ticker": ticker,
            "sector": latest['sector'],
            "country": latest['country'],
            "latest_quarter": f"{latest['fiscal_year']} {latest['fiscal_quarter']}",
            "revenue_millions": latest['revenue_millions'],
            "net_income_millions": latest['net_income_millions'],
            "profit_margin_pct": latest['profit_margin_pct'],
            "total_records": len(company_df)
        }
    
    def save_to_datalake(self, df: pd.DataFrame, filepath: str):
        """Save to Data Lake as JSON lines."""
        df.to_json(filepath, orient='records', lines=True, date_format='iso')
        logger.info(f"💾 Saved {len(df)} records to {filepath}")
    
    def close(self):
        """Close connections."""
        if self.producer:
            self.producer.flush()
            self.producer.close()


# Test
if __name__ == "__main__":
    print("=" * 70)
    print("FAME Data Space - SOURCE 3: Company Financials (CSV)")
    print("=" * 70)
    
    connector = CompanyFinancialsCSVConnector()
    
    # Generate and read data
    print("\n📊 Processing company financials...")
    df = connector.process_batch(publish_kafka=False)
    
    print(f"\n📈 Data Summary:")
    print(f"   Total records: {len(df)}")
    print(f"   Companies: {df['company_name'].nunique()}")
    print(f"   Sectors: {df['sector'].unique().tolist()}")
    print(f"   Date range: {df['fiscal_year'].min()} - {df['fiscal_year'].max()}")
    
    print("\n📋 Sample data (BNP Paribas):")
    summary = connector.get_company_summary(df, "BNP.PA")
    for key, value in summary.items():
        print(f"   {key}: {value}")
    
    # Save to Data Lake format
    os.makedirs("data/raw/csv", exist_ok=True)
    connector.save_to_datalake(df, "data/raw/csv/financials_processed.json")
    
    connector.close()
    print("\n✅ Source 3 test complete!")
