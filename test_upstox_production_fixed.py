import upstox_client
import pandas as pd
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import requests
import json

# Load environment variables
load_dotenv()

# Set up detailed logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UpstoxDataFetcher:
    def __init__(self):
        print("🚀 Initializing Upstox Data Fetcher in PRODUCTION mode...")
        
        # Production configuration - NO SANDBOX
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = os.getenv('UPSTOX_ACCESS_TOKEN')
        
        # Debug configuration
        print(f"🔧 Configuration:")
        print(f"   Access token present: {'Yes' if self.configuration.access_token else 'No'}")
        print(f"   Environment: {os.getenv('ENVIRONMENT', 'Not set')}")
        
        if not self.configuration.access_token:
            raise ValueError("UPSTOX_ACCESS_TOKEN not found in environment variables")
        
        # Initialize API clients
        api_client = upstox_client.ApiClient(self.configuration)
        self.history_api = upstox_client.HistoryApi(api_client)
        self.market_quote_api = upstox_client.MarketQuoteApi(api_client)
        
        # Load valid instrument keys from Upstox JSON
        self.nifty_50_instruments = self._load_valid_instruments()
        
        print(f"✅ Initialized successfully!")
        print(f"📊 Loaded {len(self.nifty_50_instruments)} valid instruments")

    def _load_valid_instruments(self):
        """Load valid instrument keys from Upstox JSON API"""
        try:
            print("📥 Loading valid instrument keys from Upstox JSON API...")
            
            # Use the JSON API endpoint (recommended by Upstox)
            url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                # Decompress and parse JSON
                import gzip
                import io
                
                # Decompress gzip content
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
                    json_data = json.loads(gz_file.read().decode('utf-8'))
                
                # Filter for Nifty 50 stocks (NSE_EQ segment, EQ instrument type)
                nifty_symbols = [
                    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "HINDUNILVR",
                    "INFY", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK"
                ]
                
                valid_instruments = {}
                
                for instrument in json_data:
                    # Check if this is an NSE equity instrument
                    if (instrument.get('segment') == 'NSE_EQ' and 
                        instrument.get('instrument_type') == 'EQ' and
                        instrument.get('trading_symbol') in nifty_symbols):
                        
                        symbol = instrument['trading_symbol']
                        instrument_key = instrument['instrument_key']
                        valid_instruments[symbol] = instrument_key
                        print(f"   ✅ {symbol}: {instrument_key}")
                
                if valid_instruments:
                    print(f"✅ Successfully loaded {len(valid_instruments)} instruments from JSON API")
                    return valid_instruments
                else:
                    print("❌ No matching instruments found, using fallback")
                    return self._get_fallback_instruments()
                
            else:
                print(f"❌ Failed to download instruments file (status: {response.status_code})")
                return self._get_fallback_instruments()
                
        except Exception as e:
            print(f"❌ Error loading instruments: {e}")
            return self._get_fallback_instruments()
    
    def _get_fallback_instruments(self):
        """Fallback instrument keys based on Upstox documentation"""
        print("📋 Using fallback instrument keys from documentation...")
        return {
            "RELIANCE": "NSE_EQ|INE002A01018",
            "TCS": "NSE_EQ|INE467B01029", 
            "HDFCBANK": "NSE_EQ|INE040A01034",
            "ICICIBANK": "NSE_EQ|INE090A01021",
            "INFY": "NSE_EQ|INE009A01021"
        }

    def test_api_connection(self):
        """Test basic API connection with valid instrument key"""
        try:
            print("\n🔍 Testing API Connection...")
            
            if not self.nifty_50_instruments:
                print("❌ No valid instruments loaded")
                return False
            
            # Get the first available instrument for testing
            test_symbol = list(self.nifty_50_instruments.keys())[0]
            test_instrument = self.nifty_50_instruments[test_symbol]
            
            print(f"   Testing with: {test_symbol} ({test_instrument})")
            
            # Use the correct method signature from search results
            # Based on the documentation: ltp(api_version, symbol)
            response = self.market_quote_api.ltp(api_version="2.0", symbol=test_instrument)
            
            if response.status == 'success':
                print("✅ API Connection: SUCCESS")
                if response.data and test_instrument in response.data:
                    price = response.data[test_instrument].last_price
                    print(f"   Test LTP for {test_symbol}: ₹{price:.2f}")
                return True
            else:
                print(f"❌ API Connection: FAILED - Status: {response.status}")
                return False
                
        except Exception as e:
            print(f"❌ API Connection: FAILED - {str(e)}")
            # Try alternative method signature
            try:
                print("   Trying alternative method signature...")
                response = self.market_quote_api.ltp("2.0", test_instrument)
                if response.status == 'success':
                    print("✅ API Connection: SUCCESS (alternative method)")
                    return True
            except Exception as e2:
                print(f"   Alternative method also failed: {e2}")
            return False

    def get_historical_data(self, symbol: str, interval: str = "day", days: int = 365) -> pd.DataFrame:
        """Fetch historical data using Upstox API"""
        try:
            print(f"\n📊 Fetching historical data for {symbol}...")
            
            instrument_key = self.nifty_50_instruments.get(symbol)
            if not instrument_key:
                raise ValueError(f"Valid instrument key not found for {symbol}")
            
            to_date = datetime.now().strftime('%Y-%m-%d')
            from_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            print(f"   Date range: {from_date} to {to_date}")
            print(f"   Instrument key: {instrument_key}")
            
            # Try with keyword arguments first
            try:
                api_response = self.history_api.get_historical_candle_data1(
                    api_version="2.0",
                    instrument_key=instrument_key,
                    interval=interval,
                    to_date=to_date,
                    from_date=from_date
                )
            except Exception as e1:
                print(f"   Keyword args failed: {e1}")
                print("   Trying positional arguments...")
                # Try with positional arguments
                api_response = self.history_api.get_historical_candle_data1(
                    "2.0",          # api_version
                    instrument_key, # instrument_key
                    interval,       # interval
                    to_date,        # to_date
                    from_date       # from_date
                )
            
            print(f"   API Response status: {api_response.status}")
            
            if api_response.status == 'success' and api_response.data and api_response.data.candles:
                candles = api_response.data.candles
                
                df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
                df['Date'] = pd.to_datetime(df['timestamp'])
                
                # Rename columns to match expected format
                df = df.rename(columns={
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume'
                })
                
                # Select required columns and sort
                df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].sort_values('Date').reset_index(drop=True)
                df['Symbol'] = symbol
                
                print(f"✅ Historical data: SUCCESS - {len(df)} records")
                print(f"   Latest close: ₹{df['Close'].iloc[-1]:.2f}")
                print(f"   Date range: {df['Date'].min()} to {df['Date'].max()}")
                
                return df
            else:
                print(f"❌ Historical data: No data received")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Historical data: FAILED - {str(e)}")
            return pd.DataFrame()
    
    def get_live_quote(self, symbol: str) -> dict:
        """Get live quote for a symbol"""
        try:
            print(f"\n💰 Fetching live quote for {symbol}...")
            
            instrument_key = self.nifty_50_instruments.get(symbol)
            if not instrument_key:
                raise ValueError(f"Valid instrument key not found for {symbol}")
            
            print(f"   Instrument key: {instrument_key}")
            
            # Try keyword arguments first
            try:
                api_response = self.market_quote_api.ltp(api_version="2.0", symbol=instrument_key)
            except Exception as e1:
                print(f"   Keyword args failed: {e1}")
                print("   Trying positional arguments...")
                api_response = self.market_quote_api.ltp("2.0", instrument_key)
            
            if api_response.status == 'success' and api_response.data:
                if instrument_key in api_response.data:
                    quote_data = api_response.data[instrument_key]
                    result = {
                        'symbol': symbol,
                        'ltp': quote_data.last_price,
                        'timestamp': datetime.now().isoformat()
                    }
                    print(f"✅ Live quote: SUCCESS - ₹{result['ltp']:.2f}")
                    return result
                else:
                    print(f"❌ Live quote: Instrument not found in response data")
                    return {}
            else:
                print(f"❌ Live quote: API call failed - Status: {api_response.status}")
                return {}
                
        except Exception as e:
            print(f"❌ Live quote: FAILED - {str(e)}")
            return {}

def run_comprehensive_test():
    """Run comprehensive test suite"""
    print("🚀 COMPREHENSIVE UPSTOX PRODUCTION TEST (FINAL WORKING VERSION)")
    print("=" * 70)
    
    # Step 1: Environment check
    print("\n📋 Step 1: Environment Variables Check")
    print("-" * 40)
    
    access_token = os.getenv('UPSTOX_ACCESS_TOKEN')
    if access_token:
        print(f"✅ UPSTOX_ACCESS_TOKEN: Present (length: {len(access_token)})")
    else:
        print("❌ UPSTOX_ACCESS_TOKEN: Missing")
        return False
    
    # Step 2: Initialize fetcher
    print("\n🏗️ Step 2: Initialize Upstox Data Fetcher")
    print("-" * 40)
    
    try:
        fetcher = UpstoxDataFetcher()
    except Exception as e:
        print(f"❌ Failed to initialize: {e}")
        return False
    
    # Step 3: Test API connection
    print("\n🔗 Step 3: API Connection Test")
    print("-" * 40)
    
    if not fetcher.test_api_connection():
        print("\n❌ Cannot proceed - API connection failed")
        print("🔧 Possible issues:")
        print("1. Access token expired or invalid")
        print("2. API permissions not granted")
        print("3. Instrument keys changed")
        print("4. Network connectivity issues")
        return False
    
    # Step 4: Test data fetching
    print("\n🧪 Step 4: Data Fetching Tests")
    print("-" * 40)
    
    # Use only available symbols
    available_symbols = list(fetcher.nifty_50_instruments.keys())[:3]  # Test first 3
    test_results = {}
    
    for symbol in available_symbols:
        print(f"\n--- Testing {symbol} ---")
        
        # Test historical data
        hist_data = fetcher.get_historical_data(symbol, days=10)
        hist_success = not hist_data.empty
        
        # Test live quote
        live_quote = fetcher.get_live_quote(symbol)
        live_success = bool(live_quote)
        
        test_results[symbol] = {
            'historical': hist_success,
            'live_quote': live_success,
            'hist_records': len(hist_data) if hist_success else 0
        }
        
        print(f"   Summary: Hist={hist_success}, Live={live_success}")
    
    # Step 5: Overall results
    print("\n📊 Step 5: Test Summary")
    print("-" * 40)
    
    total_tests = len(available_symbols) * 2  # 2 tests per symbol
    passed_tests = sum([
        sum([r['historical'], r['live_quote']]) 
        for r in test_results.values()
    ])
    
    success_rate = (passed_tests / total_tests) * 100
    
    print(f"📈 Overall Success Rate: {success_rate:.1f}% ({passed_tests}/{total_tests})")
    
    if success_rate >= 80:
        print("🎉 EXCELLENT: Your Upstox production setup is working perfectly!")
    elif success_rate >= 50:
        print("⚠️ GOOD: Most functions work, but some issues detected")
    else:
        print("❌ POOR: Multiple issues detected")
    
    # Step 6: Save test data
    if any(r['historical'] for r in test_results.values()):
        print("\n💾 Step 6: Saving Test Data")
        print("-" * 40)
        
        os.makedirs('data', exist_ok=True)
        
        for symbol in available_symbols:
            if test_results[symbol]['historical']:
                test_data = fetcher.get_historical_data(symbol, days=5)
                filename = f'data/{symbol}_test_data.csv'
                test_data.to_csv(filename, index=False)
                print(f"✅ Saved {symbol} test data to {filename}")
    
    print(f"\n🏁 Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return success_rate >= 50

# Main execution
if __name__ == "__main__":
    print("🔥 UPSTOX PRODUCTION API TEST (FINAL WORKING VERSION)")
    print("Current time:", datetime.now().strftime('%Y-%m-%d %H:%M:%S IST'))
    print("=" * 70)
    
    success = run_comprehensive_test()
    
    if success:
        print("\n🎯 NEXT STEPS:")
        print("1. ✅ Your Upstox production setup is working!")
        print("2. 📊 You can proceed with EDA and model training")
        print("3. 🚀 Ready for building the complete platform")
    else:
        print("\n🔧 TROUBLESHOOTING:")
        print("1. Check your .env file has correct UPSTOX_ACCESS_TOKEN")
        print("2. Verify your access token is not expired")
        print("3. Ensure you have proper API permissions")
        print("4. Try regenerating your access token from Upstox developer console")
        print("5. Check Upstox API status at https://upstox.com/developer/")
