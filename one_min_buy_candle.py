import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

# Configuration
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzcwMjY3ODM1LCJpYXQiOjE3NzAxODE0MzUsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA2ODczMzk4In0.Pe6jZUYyIuvzKLAmqz1Xk4F7_jdLJuJuJR9bp_tr5fdcH8yKcE0sb-EO91M3Q0oDdQnaZzcp0cdS6Ley4tQK9A"
SENSEX = 51
NIFTY_50 = 13

class DhanAPI:
    def __init__(self, access_token):
        self.base_url = "https://api.dhan.co/v2"
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'access-token': access_token
        }

      
    
    def get_options_data(self, strike, option_type, from_date, to_date,
                        exchange_segment="NSE_FNO", interval=1, security_id=NIFTY_50,
                        instrument="OPTIDX", expiry_flag="WEEK", expiry_code=1, max_retries=3):
        """Fetch expired options data from Dhan API with retry logic"""
        

        # security_id = NIFTY_50
        # exchange_segment = "NSE_FNO"

        # security_id = self.SENSEX
        # exchange_segment = "BSE_FNO"
        
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
        
        for attempt in range(max_retries):
            try:
                response = requests.post(f"{self.base_url}/charts/rollingoption", 
                                        headers=self.headers, json=payload)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f" (Error: {e}, retrying...)", end="", flush=True)
                    time.sleep(1)
                else:
                    print(f" (Error: {e}, failed after {max_retries} attempts)")
                    return None
        
        return None
    
    def get_all_strikes_data(self, option_type, from_date, to_date, strike_range=10):
        """Fetch data for all strikes from ATM-range to ATM+range with rate limiting"""
        
        all_data = {}
        strikes = ["ATM"] + [f"ATM+{i}" for i in range(1, strike_range + 1)] + [f"ATM-{i}" for i in range(1, strike_range + 1)]
        
        print(f"Fetching {option_type} data for strikes: {', '.join(strikes)}")
        print(f"Rate limit: 10 requests/second (delay: 0.1s between requests)")
        
        for idx, strike in enumerate(strikes):
            print(f"  [{idx+1}/{len(strikes)}] Fetching {strike}...", end="", flush=True)
            data = self.get_options_data(strike, option_type, from_date, to_date)
            if data:
                all_data[strike] = data
                print(" ✓")
            else:
                print(" ✗")
            
            # # Rate limiting: 10 requests per second = 0.1 second delay between requests
            # if idx < len(strikes) - 1:  # Don't sleep after the last request
            #     time.sleep(0.1)
        
        return all_data


def convert_multi_strike_to_dataframe(all_strikes_data, option_type):
    """Convert multi-strike API response to combined DataFrame"""
    
    if not all_strikes_data:
        return None
    
    data_key = 'ce' if option_type == 'CALL' else 'pe'
    all_dfs = []
    
    for strike_label, api_response in all_strikes_data.items():
        if not api_response or 'data' not in api_response:
            continue
        
        options_data = api_response['data'].get(data_key)
        
        if not options_data or not options_data.get('timestamp'):
            continue
        
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(options_data['timestamp'], unit='s', utc=True).tz_convert('Asia/Kolkata'),
            'open': options_data.get('open', []),
            'high': options_data.get('high', []),
            'low': options_data.get('low', []),
            'close': options_data.get('close', []),
            'volume': options_data.get('volume', []),
            'oi': options_data.get('oi', []),
            'iv': options_data.get('iv', []),
            'strike': options_data.get('strike', []),
            'spot': options_data.get('spot', []),
            'strike_label': strike_label  # ATM, ATM+1, etc.
        })
        
        all_dfs.append(df)
    
    if not all_dfs:
        return None
    
    # Combine all strikes data
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.sort_values('timestamp', inplace=True)
    
    return combined_df


def backtest_strategy(df, quantity=1, option_type=""):
    """
    Green Candle Buy at Low Strategy:
    - At 9:15, identify ATM strike and lock it for the day
    - Find green candle (close > open) at day low and BUY at candle close
    - Stop loss: any candle closing below entry-candle-low
    - Re-entry: when green candle forms at a lower low
    - Final exit at 3:29 PM
    """
    
    # Add date and time columns
    df['date'] = df['timestamp'].dt.date
    df['time'] = df['timestamp'].dt.time
    
    # Trading state
    in_trade = False
    entry_price = 0
    entry_strike = 0
    entry_spot = 0
    entry_low = 0
    entry_high = 0
    target_price = 0
    green_candle_low = float('inf')  # Track lowest green candle
    trades = []
    
    print(f"\n{'='*80}")
    print(f"Backtesting Green Candle Buy at Low Strategy")
    print(f"Data: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Total rows: {len(df)}")
    print(f"{'='*80}\n")
    
    # Process each day separately
    for date in df['date'].unique():
        daily_df = df[df['date'] == date].copy()
        
        # Find 9:15 ATM strike for the day
        morning_915 = daily_df[daily_df['time'] >= pd.Timestamp('09:15:00').time()]
        if len(morning_915) == 0:
            print(f"\n--- Trading Day: {date} --- No 9:15 data, skipping")
            continue
        
        atm_at_915 = morning_915[morning_915['strike_label'] == 'ATM'].head(1)
        if len(atm_at_915) == 0:
            print(f"\n--- Trading Day: {date} --- No ATM data at 9:15, skipping")
            continue
        
        locked_strike = atm_at_915.iloc[0]['strike']
        print(f"\n--- Trading Day: {date} --- 9:15 ATM Strike: {locked_strike:.2f}")
        
        # Filter data for this specific strike value throughout the day
        daily_strike_df = daily_df[daily_df['strike'] == locked_strike].copy()
        daily_strike_df = daily_strike_df.sort_values('timestamp')
        
        if len(daily_strike_df) == 0:
            print(f"  No data for strike {locked_strike:.2f}")
            continue
        
        # Reset daily variables
        in_trade = False
        entry_price = 0
        entry_strike = 0
        entry_spot = 0
        entry_low = 0
        entry_high = 0
        target_price = 0
        green_candle_low = float('inf')
        entry_time = None
        
        for idx, row in daily_strike_df.iterrows():
            current_time = row['time']
            timestamp = row['timestamp']
            is_green_candle = row['close'] > row['open']
            
            # Check if it's past 3:29 PM - exit all trades
            if current_time >= pd.Timestamp('15:29:00').time():
                if in_trade:
                    pnl = (row['close'] - entry_price) * quantity  # BUY strategy - profit when price goes up
                    trades.append({
                        'date': date,
                        'entry_time': entry_time,
                        'entry_price': entry_price,
                        'entry_strike': entry_strike,
                        'entry_spot': entry_spot,
                        'exit_time': timestamp,
                        'exit_price': row['close'],
                        'exit_strike': row['strike'],
                        'exit_spot': row['spot'],
                        'exit_reason': 'EOD',
                        'pnl': pnl
                    })
                    print(f"  {current_time} - EOD Exit @ {row['close']:.2f} | Strike: {row['strike']:.2f} | Spot: {row['spot']:.2f} | PnL: {pnl:.2f}")
                    in_trade = False
                break
            
            # Entry Logic: Green candle at new low (first or lower than previous)
            if not in_trade and is_green_candle and row['low'] < green_candle_low:
                entry_price = row['close']
                entry_strike = row['strike']
                entry_spot = row['spot']
                entry_low = row['low']
                entry_high = row['high']
                green_candle_low = row['low']
                entry_time = timestamp
                # Calculate 4x target based on candle height
                candle_height = entry_high - entry_low
                target_price = float(entry_price * 1.1)
                in_trade = True
                print(f"  {current_time} - ENTRY (Green at Low) @ {entry_price:.2f} | Low: {entry_low:.2f} | High: {entry_high:.2f} | Target: {target_price:.2f} | Strike: {entry_strike:.2f} | Spot: {entry_spot:.2f}")
            
            # Exit Logic: Target hit - price reaches 4x candle height
            if in_trade and row['close'] >= target_price:
                pnl = (row['close'] - entry_price) * quantity  # BUY strategy
                trades.append({
                    'date': date,
                    'entry_time': entry_time,
                    'entry_price': entry_price,
                    'entry_strike': entry_strike,
                    'entry_spot': entry_spot,
                    'exit_time': timestamp,
                    'exit_price': row['close'],
                    'exit_strike': row['strike'],
                    'exit_spot': row['spot'],
                    'exit_reason': 'Target Hit',
                    'pnl': pnl
                })
                print(f"  {current_time} - EXIT (Target Hit {target_price:.2f}) @ {row['close']:.2f} | Strike: {row['strike']:.2f} | Spot: {row['spot']:.2f} | PnL: {pnl:.2f}")
                in_trade = False
            
            # Exit Logic: Stop Loss - Any candle closes below entry-candle-low
            elif in_trade and row['close'] < entry_low:
                pnl = (row['close'] - entry_price) * quantity  # BUY strategy
                trades.append({
                    'date': date,
                    'entry_time': entry_time,
                    'entry_price': entry_price,
                    'entry_strike': entry_strike,
                    'entry_spot': entry_spot,
                    'exit_time': timestamp,
                    'exit_price': row['close'],
                    'exit_strike': row['strike'],
                    'exit_spot': row['spot'],
                    'exit_reason': 'Stop Loss',
                    'pnl': pnl
                })
                print(f"  {current_time} - EXIT (SL Hit - Below Entry Low {entry_low:.2f}) @ {row['close']:.2f} | Strike: {row['strike']:.2f} | Spot: {row['spot']:.2f} | PnL: {pnl:.2f}")
                in_trade = False
    
    # Calculate results
    if not trades:
        print("\nNo trades executed!")
        return {'total_trades': 0, 'total_pnl': 0}
    
    trades_df = pd.DataFrame(trades)
    total_pnl = trades_df['pnl'].sum()
    winning_trades = len(trades_df[trades_df['pnl'] > 0])
    losing_trades = len(trades_df[trades_df['pnl'] <= 0])
    win_rate = (winning_trades / len(trades_df) * 100) if len(trades_df) > 0 else 0
    
    print(f"\n{'='*80}")
    print(f"BACKTEST RESULTS")
    print(f"{'='*80}")
    print(f"Total Trades: {len(trades_df)}")
    print(f"Winning Trades: {winning_trades}")
    print(f"Losing Trades: {losing_trades}")
    print(f"Win Rate: {win_rate:.2f}%")
    print(f"Total PnL: {total_pnl:.2f}")
    print(f"Average PnL per Trade: {trades_df['pnl'].mean():.2f}")
    print(f"Max Profit: {trades_df['pnl'].max():.2f}")
    print(f"Max Loss: {trades_df['pnl'].min():.2f}")
    print(f"{'='*80}\n")
    
    print("\nDetailed Trades:")
    print(trades_df.to_string(index=False))
    
    # Add option type column
    trades_df['option_type'] = option_type
    
    return {
        'total_trades': len(trades_df),
        'winning_trades': winning_trades,
        'losing_trades': losing_trades,
        'win_rate': win_rate,
        'total_pnl': total_pnl,
        'avg_pnl': trades_df['pnl'].mean(),
        'trades': trades_df
    }


def main():
    # Initialize API
    dhan = DhanAPI(ACCESS_TOKEN)
    
    # Strategy parameters
    FROM_DATE = "2026-01-01"
    TO_DATE = "2026-02-04"
    QUANTITY = 1  # Number of lots
    STRIKE_RANGE = 10  # Fetch ATM-10 to ATM+10
    
    # Collect all trades from both CALL and PUT
    all_trades = []
    
    # Backtest for both CALL and PUT
    for option_type in ["CALL", "PUT"]:
        print(f"\n{'#'*80}")
        print(f"# BACKTESTING {option_type} OPTIONS")
        print(f"{'#'*80}\n")
        
        # Fetch data for all strikes
        all_strikes_data = dhan.get_all_strikes_data(
            option_type=option_type,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            strike_range=STRIKE_RANGE
        )
        
        # Convert to DataFrame
        df = convert_multi_strike_to_dataframe(all_strikes_data, option_type)
        
        if df is not None and len(df) > 0:
            print(f"\nTotal data points: {len(df)}")
            print(f"Unique strikes: {df['strike'].nunique()}")
            print(f"Strike range: {df['strike'].min():.2f} to {df['strike'].max():.2f}")
            
            # Run backtest
            results = backtest_strategy(df, quantity=QUANTITY, option_type=option_type)
            
            # Collect trades
            if results.get('total_trades', 0) > 0:
                all_trades.append(results['trades'])
        else:
            print(f"Failed to fetch data for {option_type}")
        
        print(f"\n{'#'*80}\n")
    
    # Merge and save all trades to a single CSV
    if all_trades:
        merged_trades = pd.concat(all_trades, ignore_index=True)
        merged_trades = merged_trades.sort_values(['date', 'entry_time'])
        
        # Reorder columns to put option_type after date
        columns = ['date', 'option_type', 'entry_time', 'entry_price', 'entry_strike', 'entry_spot',
                   'exit_time', 'exit_price', 'exit_strike', 'exit_spot', 'exit_reason', 'pnl']
        merged_trades = merged_trades[columns]
        
        os.makedirs('trades/1min_buy', exist_ok=True)
        timestamp_str = datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        csv_filename = f"trades/1min_buy/trades_{timestamp_str}.csv"
        merged_trades.to_csv(csv_filename, index=False)
        
        print(f"\n{'='*80}")
        print(f"All trades merged and saved to: {csv_filename}")
        print(f"Total trades (CALL + PUT): {len(merged_trades)}")
        print(f"{'='*80}\n")
    else:
        print("\nNo trades to save!")


if __name__ == "__main__":
    main()
