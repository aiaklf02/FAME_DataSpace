"""
FAME Data Space - Source 1: Stock Market API Connector
========================================================
Real-time financial market data from Alpha Vantage / Yahoo Finance

Data Type: REST API (JSON)
Frequency: Real-time / Intraday
Volume: ~1000 records/day per symbol
"""

import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StockMarketAPIConnector:
    """
    Connector for real-time stock market data.
    Supports Alpha Vantage (free tier) and Yahoo Finance.
    """
    
    # Free API - Alpha Vantage (get key at: https://www.alphavantage.co/support/#api-key)
    ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
    
    # Alternative: Yahoo Finance (no key required)
    YAHOO_FINANCE_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
    
    def __init__(self, api_key: str = "demo"):
        """
        Initialize the connector.
        
        Args:
            api_key: Alpha Vantage API key (use "demo" for testing)
        """
        self.api_key = api_key
        self.session = requests.Session()
        
    def get_stock_quote(self, symbol: str) -> Dict:
        """
        Get real-time stock quote.
        
        Args:
            symbol: Stock ticker symbol (e.g., "AAPL", "MSFT")
            
        Returns:
            Dictionary with stock data
        """
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key
        }
        
        try:
            response = self.session.get(self.ALPHA_VANTAGE_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "Global Quote" in data:
                quote = data["Global Quote"]
                return {
                    "source": "alpha_vantage_api",
                    "timestamp": datetime.now().isoformat(),
                    "symbol": quote.get("01. symbol"),
                    "open": float(quote.get("02. open", 0)),
                    "high": float(quote.get("03. high", 0)),
                    "low": float(quote.get("04. low", 0)),
                    "price": float(quote.get("05. price", 0)),
                    "volume": int(quote.get("06. volume", 0)),
                    "latest_trading_day": quote.get("07. latest trading day"),
                    "previous_close": float(quote.get("08. previous close", 0)),
                    "change": float(quote.get("09. change", 0)),
                    "change_percent": quote.get("10. change percent", "0%")
                }
            else:
                logger.warning(f"No data found for symbol: {symbol}")
                return self._get_mock_quote(symbol)
                
        except Exception as e:
            logger.error(f"API Error: {e}")
            return self._get_mock_quote(symbol)
    
    def get_intraday_data(self, symbol: str, interval: str = "5min") -> pd.DataFrame:
        """
        Get intraday time series data.
        
        Args:
            symbol: Stock ticker symbol
            interval: Time interval (1min, 5min, 15min, 30min, 60min)
            
        Returns:
            DataFrame with time series data
        """
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": interval,
            "apikey": self.api_key,
            "outputsize": "compact"
        }
        
        try:
            response = self.session.get(self.ALPHA_VANTAGE_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            time_series_key = f"Time Series ({interval})"
            if time_series_key in data:
                df = pd.DataFrame.from_dict(data[time_series_key], orient='index')
                df.columns = ['open', 'high', 'low', 'close', 'volume']
                df.index = pd.to_datetime(df.index)
                df = df.astype(float)
                df['symbol'] = symbol
                df['source'] = 'alpha_vantage_api'
                return df.reset_index().rename(columns={'index': 'timestamp'})
            else:
                return self._get_mock_intraday(symbol)
                
        except Exception as e:
            logger.error(f"API Error: {e}")
            return self._get_mock_intraday(symbol)
    
    def get_crypto_quote(self, symbol: str = "BTC", market: str = "EUR") -> Dict:
        """
        Get cryptocurrency exchange rate.
        
        Args:
            symbol: Crypto symbol (BTC, ETH, etc.)
            market: Target currency (EUR, USD)
            
        Returns:
            Dictionary with crypto data
        """
        params = {
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": symbol,
            "to_currency": market,
            "apikey": self.api_key
        }
        
        try:
            response = self.session.get(self.ALPHA_VANTAGE_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "Realtime Currency Exchange Rate" in data:
                rate = data["Realtime Currency Exchange Rate"]
                return {
                    "source": "alpha_vantage_api",
                    "type": "cryptocurrency",
                    "timestamp": datetime.now().isoformat(),
                    "from_currency": rate.get("1. From_Currency Code"),
                    "from_currency_name": rate.get("2. From_Currency Name"),
                    "to_currency": rate.get("3. To_Currency Code"),
                    "to_currency_name": rate.get("4. To_Currency Name"),
                    "exchange_rate": float(rate.get("5. Exchange Rate", 0)),
                    "last_refreshed": rate.get("6. Last Refreshed"),
                    "bid_price": float(rate.get("8. Bid Price", 0)),
                    "ask_price": float(rate.get("9. Ask Price", 0))
                }
            else:
                return self._get_mock_crypto(symbol, market)
                
        except Exception as e:
            logger.error(f"API Error: {e}")
            return self._get_mock_crypto(symbol, market)
    
    def _get_mock_quote(self, symbol: str) -> Dict:
        """Generate realistic mock data for demonstration."""
        import random
        base_price = {"AAPL": 178.50, "MSFT": 378.20, "GOOGL": 141.80, 
                      "AMZN": 153.40, "BNP.PA": 58.75, "SAN.MC": 4.12}.get(symbol, 100.0)
        
        variation = random.uniform(-0.03, 0.03)
        price = base_price * (1 + variation)
        
        return {
            "source": "mock_api",
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "open": round(price * 0.998, 2),
            "high": round(price * 1.015, 2),
            "low": round(price * 0.985, 2),
            "price": round(price, 2),
            "volume": random.randint(10000000, 50000000),
            "latest_trading_day": datetime.now().strftime("%Y-%m-%d"),
            "previous_close": round(base_price, 2),
            "change": round(price - base_price, 2),
            "change_percent": f"{variation * 100:.2f}%"
        }
    
    def _get_mock_intraday(self, symbol: str) -> pd.DataFrame:
        """Generate mock intraday data."""
        import random
        
        base_price = 150.0
        timestamps = pd.date_range(end=datetime.now(), periods=100, freq='5min')
        
        data = []
        for ts in timestamps:
            variation = random.uniform(-0.02, 0.02)
            base_price *= (1 + variation * 0.1)
            data.append({
                'timestamp': ts,
                'open': round(base_price * 0.999, 2),
                'high': round(base_price * 1.005, 2),
                'low': round(base_price * 0.995, 2),
                'close': round(base_price, 2),
                'volume': random.randint(100000, 500000),
                'symbol': symbol,
                'source': 'mock_api'
            })
        
        return pd.DataFrame(data)
    
    def _get_mock_crypto(self, symbol: str, market: str) -> Dict:
        """Generate mock crypto data."""
        import random
        
        base_rates = {"BTC": 42000.0, "ETH": 2200.0, "XRP": 0.62}
        rate = base_rates.get(symbol, 100.0) * random.uniform(0.98, 1.02)
        
        return {
            "source": "mock_api",
            "type": "cryptocurrency",
            "timestamp": datetime.now().isoformat(),
            "from_currency": symbol,
            "from_currency_name": {"BTC": "Bitcoin", "ETH": "Ethereum"}.get(symbol, symbol),
            "to_currency": market,
            "to_currency_name": {"EUR": "Euro", "USD": "US Dollar"}.get(market, market),
            "exchange_rate": round(rate, 2),
            "last_refreshed": datetime.now().isoformat(),
            "bid_price": round(rate * 0.999, 2),
            "ask_price": round(rate * 1.001, 2)
        }
    
    def fetch_multiple_stocks(self, symbols: List[str]) -> pd.DataFrame:
        """
        Fetch data for multiple stock symbols.
        
        Args:
            symbols: List of stock ticker symbols
            
        Returns:
            DataFrame with all stock data
        """
        all_data = []
        for symbol in symbols:
            quote = self.get_stock_quote(symbol)
            all_data.append(quote)
        
        return pd.DataFrame(all_data)
    
    def save_to_json(self, data: Dict, filepath: str):
        """Save data to JSON file."""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Data saved to {filepath}")


# Example usage and testing
if __name__ == "__main__":
    # Initialize connector (use "demo" for testing without API key)
    connector = StockMarketAPIConnector(api_key="demo")
    
    print("=" * 60)
    print("FAME Data Space - Stock Market API Connector Test")
    print("=" * 60)
    
    # Test 1: Get stock quote
    print("\n📈 Stock Quote (AAPL):")
    quote = connector.get_stock_quote("AAPL")
    for key, value in quote.items():
        print(f"  {key}: {value}")
    
    # Test 2: Get crypto quote
    print("\n₿ Crypto Quote (BTC/EUR):")
    crypto = connector.get_crypto_quote("BTC", "EUR")
    for key, value in crypto.items():
        print(f"  {key}: {value}")
    
    # Test 3: Get intraday data
    print("\n📊 Intraday Data (last 5 rows):")
    intraday = connector.get_intraday_data("MSFT")
    print(intraday.tail())
    
    # Test 4: Multiple stocks
    print("\n📋 Multiple Stocks Portfolio:")
    portfolio = connector.fetch_multiple_stocks(["AAPL", "MSFT", "GOOGL", "BNP.PA"])
    print(portfolio[['symbol', 'price', 'change_percent', 'volume']])
