import requests
import pandas as pd
from datetime import datetime, timedelta

# Configuration
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzcwMjY3ODM1LCJpYXQiOjE3NzAxODE0MzUsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA2ODczMzk4In0.Pe6jZUYyIuvzKLAmqz1Xk4F7_jdLJuJuJR9bp_tr5fdcH8yKcE0sb-EO91M3Q0oDdQnaZzcp0cdS6Ley4tQK9A"


class DhanAPI:
    def __init__(self, access_token):
        self.base_url = "https://api.dhan.co/v2"
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'access-token': access_token
        }
    
    def get_options_data(self, strike, option_type, from_date, to_date,
                        exchange_segment="NSE_FNO", interval=1, security_id="13",
                        instrument="OPTIDX", expiry_flag="MONTH", expiry_code=1):
        """Fetch expired options data from Dhan API"""
        
        payload = {
            "exchangeSegment": exchange_segment,
            "interval": str(interval),
            "securityId": security_id,
            "instrument": instrument,
            "expiryFlag": expiry_flag,
            "expiryCode": expiry_code,
            "strike": strike,
            "drvOptionType": option_type,
            "requiredData": ["open", "high", "low", "close", "volume", "oi", "iv", "strike", "spot"],
            "fromDate": from_date,
            "toDate": to_date
        }
        
        try:
            response = requests.post(f"{self.base_url}/charts/rollingoption", 
                                    headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"API Error: {e}")
            return None


def convert_to_dataframe(api_response, option_type):
    """Convert API response to pandas DataFrame for backtesting"""
    
    if not api_response or 'data' not in api_response:
        return None
    
    data_key = 'ce' if option_type == 'CALL' else 'pe'
    options_data = api_response['data'].get(data_key)
    
    if not options_data or not options_data.get('timestamp'):
        return None
    
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(options_data['timestamp'], unit='s'),
        'open': options_data.get('open', []),
        'high': options_data.get('high', []),
        'low': options_data.get('low', []),
        'close': options_data.get('close', []),
        'volume': options_data.get('volume', []),
        'oi': options_data.get('oi', []),
        'iv': options_data.get('iv', []),
        'strike': options_data.get('strike', []),
        'spot': options_data.get('spot', [])
    })
    
    df.set_index('timestamp', inplace=True)
    return df


def backtest_strategy(df):
    """
    Implement your backtesting strategy here
    
    Args:
        df: DataFrame with OHLC, volume, oi, iv, strike, spot data
    
    Returns:
        dict: Backtest results
    """
    
    # Example: Simple strategy placeholder
    # TODO: Implement your strategy logic here
    
    print(f"Data shape: {df.shape}")
    print(f"Date range: {df.index[0]} to {df.index[-1]}")
    print(f"\nFirst few rows:\n{df.head()}")
    print(f"\nData summary:\n{df.describe()}")
    
    return {
        'total_candles': len(df),
        'date_range': (df.index[0], df.index[-1])
    }


def main():
    # Initialize API
    dhan = DhanAPI(ACCESS_TOKEN)
    
    # Fetch data
    print("Fetching options data...")
    raw_data = dhan.get_options_data(
        strike="ATM",
        option_type="CALL",
        from_date="2024-01-01",
        to_date="2024-02-01"
    )
    
    # Convert to DataFrame
    df = convert_to_dataframe(raw_data, "CALL")
    
    if df is not None:
        # Run backtest
        results = backtest_strategy(df)
        print(f"\nBacktest Results: {results}")
    else:
        print("Failed to fetch data")


if __name__ == "__main__":
    main()
