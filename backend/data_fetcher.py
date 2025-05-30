import upstox_client
import pandas as pd
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import requests
import json

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UpstoxDataFetcher:
    def __init__(self):
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = os.getenv('UPSTOX_ACCESS_TOKEN')
        
        if not self.configuration.access_token:
            raise ValueError("UPSTOX_ACCESS_TOKEN not found in environment variables")
        
        # Initialize API clients
        api_client = upstox_client.ApiClient(self.configuration)
        self.history_api = upstox_client.HistoryApi(api_client)
        self.market_quote_api = upstox_client.MarketQuoteApi(api_client)
        
        # Load valid instrument keys
        self.nifty_50_instruments = self._load_valid_instruments()
        
        logger.info(f"Initialized Upstox Data Fetcher with {len(self.nifty_50_instruments)} instruments")

    def _load_valid_instruments(self):
        """Load valid instrument keys from Upstox JSON API"""
        try:
            url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                import gzip
                import io
                
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
                    json_data = json.loads(gz_file.read().decode('utf-8'))
                
                nifty_symbols = [
                    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "HINDUNILVR",
                    "INFY", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK",
                    "LT", "HCLTECH", "ASIANPAINT", "MARUTI", "AXISBANK",
                    "BAJFINANCE", "TITAN", "SUNPHARMA", "ULTRACEMCO", "WIPRO",
                    "NESTLEIND", "POWERGRID", "NTPC", "TATAMOTORS", "TECHM",
                    "JSWSTEEL", "COALINDIA", "INDUSINDBK", "BAJAJFINSV", "ONGC",
                    "M&M", "TATASTEEL", "CIPLA", "DRREDDY", "GRASIM",
                    "BRITANNIA", "EICHERMOT", "BPCL", "DIVISLAB", "HEROMOTOCO",
                    "ADANIENT", "APOLLOHOSP", "HINDALCO", "UPL", "BAJAJ-AUTO",
                    "SBILIFE", "HDFCLIFE", "ADANIPORTS", "TATACONSUM", "LTIM"
                ]
                
                valid_instruments = {}
                
                for instrument in json_data:
                    if (instrument.get('segment') == 'NSE_EQ' and 
                        instrument.get('instrument_type') == 'EQ' and
                        instrument.get('trading_symbol') in nifty_symbols):
                        
                        symbol = instrument['trading_symbol']
                        instrument_key = instrument['instrument_key']
                        valid_instruments[symbol] = instrument_key
                
                return valid_instruments
                
        except Exception as e:
            logger.error(f"Error loading instruments: {e}")
            
        # Fallback instruments
        return {
            "RELIANCE": "NSE_EQ|INE002A01018",
            "TCS": "NSE_EQ|INE467B01029",
            "HDFCBANK": "NSE_EQ|INE040A01034",
            "ICICIBANK": "NSE_EQ|INE090A01021",
            "INFY": "NSE_EQ|INE009A01021",
            "BHARTIARTL": "NSE_EQ|INE397D01024",
            "KOTAKBANK": "NSE_EQ|INE237A01028",
            "HINDUNILVR": "NSE_EQ|INE030A01027",
            "SBIN": "NSE_EQ|INE062A01020",
            "ITC": "NSE_EQ|INE154A01025"
        }

    def is_market_open(self):
        """Check if market is currently open"""
        now = datetime.now()
        
        # Check if it's a weekday (Monday=0, Sunday=6)
        if now.weekday() >= 5:  # Saturday or Sunday
            return False
        
        # Check if it's within market hours (9:15 AM to 3:30 PM IST)
        market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
        
        return market_start <= now <= market_end

    def get_current_price(self, symbol: str) -> dict:
        """Get current price - live if market open, else last trading day price"""
        try:
            instrument_key = self.nifty_50_instruments.get(symbol)
            if not instrument_key:
                raise ValueError(f"Instrument key not found for {symbol}")
            
            if self.is_market_open():
                # Try to get live quote
                try:
                    response = self.market_quote_api.ltp(api_version="2.0", symbol=instrument_key)
                    if response.status == 'success' and response.data and instrument_key in response.data:
                        price = response.data[instrument_key].last_price
                        return {
                            'symbol': symbol,
                            'price': price,
                            'type': 'live',
                            'timestamp': datetime.now().isoformat()
                        }
                except Exception as e:
                    logger.warning(f"Live quote failed for {symbol}: {e}")
            
            # Fallback to last trading day price
            historical_data = self.get_historical_data(symbol, days=5)
            if not historical_data.empty:
                latest_price = historical_data['Close'].iloc[-1]
                latest_date = historical_data['Date'].iloc[-1]
                
                return {
                    'symbol': symbol,
                    'price': latest_price,
                    'type': 'last_trading_day',
                    'date': latest_date.strftime('%Y-%m-%d'),
                    'timestamp': datetime.now().isoformat()
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            return {}

    def get_historical_data(self, symbol: str, interval: str = "day", days: int = 365) -> pd.DataFrame:
        """Fetch historical data using Upstox API"""
        try:
            instrument_key = self.nifty_50_instruments.get(symbol)
            if not instrument_key:
                raise ValueError(f"Instrument key not found for {symbol}")
            
            to_date = datetime.now().strftime('%Y-%m-%d')
            from_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            # Try with keyword arguments first
            try:
                api_response = self.history_api.get_historical_candle_data1(
                    api_version="2.0",
                    instrument_key=instrument_key,
                    interval=interval,
                    to_date=to_date,
                    from_date=from_date
                )
            except Exception:
                # Fallback to positional arguments
                api_response = self.history_api.get_historical_candle_data1(
                    "2.0", instrument_key, interval, to_date, from_date
                )
            
            if api_response.status == 'success' and api_response.data and api_response.data.candles:
                candles = api_response.data.candles
                
                df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
                df['Date'] = pd.to_datetime(df['timestamp'])
                
                df = df.rename(columns={
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume'
                })
                
                df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].sort_values('Date').reset_index(drop=True)
                df['Symbol'] = symbol
                
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
            return pd.DataFrame()

    def get_nifty_50_data(self, days: int = 365) -> dict:
        """Fetch data for all Nifty 50 stocks"""
        all_data = {}
        
        for symbol in self.nifty_50_instruments.keys():
            logger.info(f"Fetching data for {symbol}")
            data = self.get_historical_data(symbol, days=days)
            if not data.empty:
                all_data[symbol] = data
        
        return all_data

    def save_data(self, data: dict, filepath: str):
        """Save data to CSV files"""
        os.makedirs(filepath, exist_ok=True)
        
        for symbol, df in data.items():
            filename = f"{filepath}/{symbol}_data.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Saved data for {symbol} to {filename}")

    def get_available_symbols(self) -> list:
        """Get list of available symbols"""
        return list(self.nifty_50_instruments.keys())
