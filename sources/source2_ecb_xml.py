"""
FAME Data Space - Source 2: ECB Exchange Rates (XML Feed)
==========================================================
European Central Bank official exchange rates

Data Type: XML Feed (RSS/Atom)
Format: XML
Frequency: Daily (updated at 16:00 CET)
Volume: ~30 currency pairs/day
"""

import xml.etree.ElementTree as ET
import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import logging

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ExchangeRate:
    """Data class for exchange rate."""
    source: str
    source_type: str
    timestamp: str
    reference_date: str
    base_currency: str
    target_currency: str
    rate: float
    currency_name: str
    region: str


class ECBExchangeRateConnector:
    """
    SOURCE 2: European Central Bank Exchange Rates
    
    Features:
    - Official ECB daily exchange rates
    - XML parsing with namespace handling
    - Historical data support
    - Kafka streaming integration
    """
    
    # ECB Official Data Feed URLs
    ECB_DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
    ECB_HISTORY_90D_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml"
    ECB_HISTORY_FULL_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"
    
    # Kafka topic
    KAFKA_TOPIC = "fame.forex.ecb_rates"
    
    # Currency metadata for semantic enrichment
    CURRENCY_METADATA = {
        "USD": {"name": "US Dollar", "region": "North America", "symbol": "$"},
        "GBP": {"name": "British Pound", "region": "Europe", "symbol": "£"},
        "JPY": {"name": "Japanese Yen", "region": "Asia", "symbol": "¥"},
        "CHF": {"name": "Swiss Franc", "region": "Europe", "symbol": "Fr"},
        "CAD": {"name": "Canadian Dollar", "region": "North America", "symbol": "C$"},
        "AUD": {"name": "Australian Dollar", "region": "Oceania", "symbol": "A$"},
        "CNY": {"name": "Chinese Yuan", "region": "Asia", "symbol": "¥"},
        "INR": {"name": "Indian Rupee", "region": "Asia", "symbol": "₹"},
        "BRL": {"name": "Brazilian Real", "region": "South America", "symbol": "R$"},
        "MXN": {"name": "Mexican Peso", "region": "North America", "symbol": "$"},
        "SGD": {"name": "Singapore Dollar", "region": "Asia", "symbol": "S$"},
        "HKD": {"name": "Hong Kong Dollar", "region": "Asia", "symbol": "HK$"},
        "NOK": {"name": "Norwegian Krone", "region": "Europe", "symbol": "kr"},
        "SEK": {"name": "Swedish Krona", "region": "Europe", "symbol": "kr"},
        "DKK": {"name": "Danish Krone", "region": "Europe", "symbol": "kr"},
        "PLN": {"name": "Polish Zloty", "region": "Europe", "symbol": "zł"},
        "CZK": {"name": "Czech Koruna", "region": "Europe", "symbol": "Kč"},
        "HUF": {"name": "Hungarian Forint", "region": "Europe", "symbol": "Ft"},
        "TRY": {"name": "Turkish Lira", "region": "Europe/Asia", "symbol": "₺"},
        "ZAR": {"name": "South African Rand", "region": "Africa", "symbol": "R"},
        "KRW": {"name": "South Korean Won", "region": "Asia", "symbol": "₩"},
        "NZD": {"name": "New Zealand Dollar", "region": "Oceania", "symbol": "NZ$"},
        "RUB": {"name": "Russian Ruble", "region": "Europe/Asia", "symbol": "₽"},
    }
    
    # XML Namespaces used by ECB
    NAMESPACES = {
        'gesmes': 'http://www.gesmes.org/xml/2002-08-01',
        'eurofxref': 'http://www.ecb.int/vocabulary/2002-08-01/eurofxref'
    }
    
    def __init__(self, kafka_servers: str = "localhost:29092"):
        """Initialize the ECB connector."""
        self.kafka_servers = kafka_servers
        self.session = requests.Session()
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
            logger.info(f"✅ Kafka producer connected for ECB rates")
        except Exception as e:
            logger.warning(f"⚠️ Kafka connection failed: {e}")
            self.producer = None
    
    def _publish_to_kafka(self, key: str, data: Dict):
        """Publish to Kafka."""
        if self.producer:
            try:
                self.producer.send(self.KAFKA_TOPIC, key=key, value=data)
            except Exception as e:
                logger.error(f"Kafka publish error: {e}")
    
    def fetch_daily_rates(self, publish_kafka: bool = True) -> List[ExchangeRate]:
        """
        Fetch today's ECB exchange rates.
        
        Returns:
            List of ExchangeRate objects
        """
        try:
            response = self.session.get(self.ECB_DAILY_URL, timeout=10)
            response.raise_for_status()
            return self._parse_ecb_xml(response.content, publish_kafka)
        except Exception as e:
            logger.warning(f"ECB API failed: {e}. Using simulated data.")
            return self._generate_simulated_rates(publish_kafka)
    
    def fetch_historical_rates(self, days: int = 90, publish_kafka: bool = False) -> pd.DataFrame:
        """
        Fetch historical exchange rates.
        
        Args:
            days: Number of days (90 or full history)
            publish_kafka: Whether to publish to Kafka
            
        Returns:
            DataFrame with historical rates
        """
        url = self.ECB_HISTORY_90D_URL if days <= 90 else self.ECB_HISTORY_FULL_URL
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse XML
            root = ET.fromstring(response.content)
            
            all_rates = []
            cube_path = './/eurofxref:Cube[@time]'
            
            for time_cube in root.findall(cube_path, self.NAMESPACES):
                ref_date = time_cube.attrib['time']
                
                for rate_cube in time_cube.findall('eurofxref:Cube', self.NAMESPACES):
                    currency = rate_cube.attrib['currency']
                    rate = float(rate_cube.attrib['rate'])
                    
                    metadata = self.CURRENCY_METADATA.get(currency, {
                        "name": currency, "region": "Unknown", "symbol": currency
                    })
                    
                    all_rates.append({
                        "source": "ecb_official",
                        "source_type": "XML_FEED",
                        "timestamp": datetime.now().isoformat(),
                        "reference_date": ref_date,
                        "base_currency": "EUR",
                        "target_currency": currency,
                        "rate": rate,
                        "currency_name": metadata["name"],
                        "region": metadata["region"]
                    })
            
            logger.info(f"📊 Fetched {len(all_rates)} historical rates")
            return pd.DataFrame(all_rates)
            
        except Exception as e:
            logger.error(f"Failed to fetch historical rates: {e}")
            return self._generate_historical_dataframe(days)
    
    def _parse_ecb_xml(self, xml_content: bytes, publish_kafka: bool) -> List[ExchangeRate]:
        """Parse ECB XML response."""
        root = ET.fromstring(xml_content)
        rates = []
        
        # Find the Cube element with time attribute
        cube_path = './/eurofxref:Cube[@time]'
        time_cube = root.find(cube_path, self.NAMESPACES)
        
        if time_cube is not None:
            ref_date = time_cube.attrib['time']
            
            for rate_cube in time_cube.findall('eurofxref:Cube', self.NAMESPACES):
                currency = rate_cube.attrib['currency']
                rate_value = float(rate_cube.attrib['rate'])
                
                metadata = self.CURRENCY_METADATA.get(currency, {
                    "name": currency, "region": "Unknown"
                })
                
                exchange_rate = ExchangeRate(
                    source="ecb_official",
                    source_type="XML_FEED",
                    timestamp=datetime.now().isoformat(),
                    reference_date=ref_date,
                    base_currency="EUR",
                    target_currency=currency,
                    rate=rate_value,
                    currency_name=metadata["name"],
                    region=metadata["region"]
                )
                
                rates.append(exchange_rate)
                
                if publish_kafka:
                    self._publish_to_kafka(f"EUR_{currency}", asdict(exchange_rate))
        
        logger.info(f"📊 Parsed {len(rates)} exchange rates from ECB")
        return rates
    
    def _generate_simulated_rates(self, publish_kafka: bool) -> List[ExchangeRate]:
        """Generate realistic simulated exchange rates."""
        import random
        
        # Realistic base rates (EUR to X)
        base_rates = {
            "USD": 1.08, "GBP": 0.86, "JPY": 162.50, "CHF": 0.94,
            "CAD": 1.47, "AUD": 1.65, "CNY": 7.85, "INR": 90.20,
            "BRL": 5.40, "MXN": 18.70, "SGD": 1.45, "HKD": 8.45,
            "NOK": 11.45, "SEK": 11.25, "DKK": 7.46, "PLN": 4.32
        }
        
        rates = []
        ref_date = datetime.now().strftime("%Y-%m-%d")
        
        for currency, base_rate in base_rates.items():
            # Add small random variation
            rate_value = round(base_rate * random.uniform(0.995, 1.005), 4)
            
            metadata = self.CURRENCY_METADATA.get(currency, {"name": currency, "region": "Unknown"})
            
            exchange_rate = ExchangeRate(
                source="ecb_simulated",
                source_type="XML_FEED",
                timestamp=datetime.now().isoformat(),
                reference_date=ref_date,
                base_currency="EUR",
                target_currency=currency,
                rate=rate_value,
                currency_name=metadata["name"],
                region=metadata["region"]
            )
            
            rates.append(exchange_rate)
            
            if publish_kafka:
                self._publish_to_kafka(f"EUR_{currency}", asdict(exchange_rate))
        
        return rates
    
    def _generate_historical_dataframe(self, days: int) -> pd.DataFrame:
        """Generate simulated historical data."""
        import random
        
        base_rates = {"USD": 1.08, "GBP": 0.86, "JPY": 162.50, "CHF": 0.94}
        
        all_rates = []
        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            
            for currency, base_rate in base_rates.items():
                # Random walk simulation
                rate = round(base_rate * random.uniform(0.97, 1.03), 4)
                metadata = self.CURRENCY_METADATA.get(currency, {"name": currency, "region": "Unknown"})
                
                all_rates.append({
                    "source": "ecb_simulated",
                    "source_type": "XML_FEED",
                    "timestamp": datetime.now().isoformat(),
                    "reference_date": date,
                    "base_currency": "EUR",
                    "target_currency": currency,
                    "rate": rate,
                    "currency_name": metadata["name"],
                    "region": metadata["region"]
                })
        
        return pd.DataFrame(all_rates)
    
    def to_dataframe(self, rates: List[ExchangeRate]) -> pd.DataFrame:
        """Convert rates to DataFrame."""
        return pd.DataFrame([asdict(r) for r in rates])
    
    def save_to_xml(self, rates: List[ExchangeRate], filepath: str):
        """Save rates to XML file (preserving original format)."""
        root = ET.Element("exchange_rates")
        root.set("source", "FAME_DataSpace")
        root.set("generated", datetime.now().isoformat())
        
        for rate in rates:
            rate_elem = ET.SubElement(root, "rate")
            rate_elem.set("currency", rate.target_currency)
            rate_elem.set("value", str(rate.rate))
            rate_elem.set("date", rate.reference_date)
            
            name_elem = ET.SubElement(rate_elem, "currency_name")
            name_elem.text = rate.currency_name
            
            region_elem = ET.SubElement(rate_elem, "region")
            region_elem.text = rate.region
        
        tree = ET.ElementTree(root)
        tree.write(filepath, encoding='utf-8', xml_declaration=True)
        logger.info(f"💾 Saved {len(rates)} rates to {filepath}")
    
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
    print("FAME Data Space - SOURCE 2: ECB Exchange Rates (XML)")
    print("=" * 70)
    
    connector = ECBExchangeRateConnector()
    
    # Fetch daily rates
    print("\n💱 Fetching daily ECB exchange rates...")
    rates = connector.fetch_daily_rates(publish_kafka=False)
    
    df = connector.to_dataframe(rates)
    print(df[['target_currency', 'currency_name', 'rate', 'region']].head(10))
    
    # Save sample data
    import os
    os.makedirs("data/raw/xml", exist_ok=True)
    connector.save_to_xml(rates, "data/raw/xml/ecb_rates.xml")
    connector.save_to_datalake(df, "data/raw/xml/ecb_rates.json")
    
    connector.close()
    print("\n✅ Source 2 test complete!")
