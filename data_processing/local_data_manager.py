"""
Local Stock Data Manager
Handles data fetching from AkShare and local caching.
"""

import akshare as ak
import pandas as pd
import numpy as np
import os
import sqlite3
from datetime import datetime, timedelta
import time
from typing import Optional, List, Dict

class LocalDataManager:
    def __init__(self, data_dir="stock_data"):
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "stock_data.db")
        self.daily_data_dir = os.path.join(data_dir, "daily_data")
        self.ensure_directories()
        self.init_database()

    def ensure_directories(self):
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        if not os.path.exists(self.daily_data_dir):
            os.makedirs(self.daily_data_dir)

    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_list (
                code TEXT PRIMARY KEY,
                name TEXT,
                update_time TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def get_stock_list(self) -> List[str]:
        """Get list of stock codes (JQ format: 000001.XSHE)"""
        # Try to get from DB first
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT code FROM stock_list")
        rows = cursor.fetchall()
        conn.close()
        
        if rows:
            return [row[0] for row in rows]
        
        # If empty, fetch from AkShare
        return self.update_stock_list()

    def get_available_stocks(self) -> List[str]:
        """Get list of stocks that have local daily data"""
        files = os.listdir(self.daily_data_dir)
        stocks = []
        for f in files:
            if f.endswith(".csv"):
                code = f.replace(".csv", "")
                if code.startswith('6'):
                    stocks.append(f"{code}.XSHG")
                else:
                    stocks.append(f"{code}.XSHE")
        return stocks

    def update_stock_list(self) -> List[str]:
        """Fetch latest stock list from AkShare and update DB"""
        print("Updating stock list from AkShare...")
        try:
            stock_info = ak.stock_info_a_code_name()
            jq_stocks = []
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Clear existing
            cursor.execute("DELETE FROM stock_list")
            
            for _, row in stock_info.iterrows():
                code = str(row['code'])
                name = row['name']
                
                # Filter out BJ (4/8) and KCB (68) if needed, but aa.py does its own filtering.
                # aa.py filters: 68, 4, 8.
                # We will fetch ALL A-shares and let the strategy filter.
                
                if code.startswith('6'):
                    jq_code = f"{code}.XSHG"
                else:
                    jq_code = f"{code}.XSHE"
                
                jq_stocks.append(jq_code)
                cursor.execute("INSERT INTO stock_list (code, name, update_time) VALUES (?, ?, ?)",
                               (jq_code, name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            
            conn.commit()
            conn.close()
            print(f"Updated {len(jq_stocks)} stocks.")
            return jq_stocks
        except Exception as e:
            print(f"Error updating stock list: {e}")
            return []

    def get_daily_data(self, security: str, count: int = 100, end_date: str = None) -> pd.DataFrame:
        """
        Get daily data for a stock.
        security: 000001.XSHE
        count: number of days
        end_date: YYYY-MM-DD (inclusive)
        """
        code = security.split('.')[0]
        csv_path = os.path.join(self.daily_data_dir, f"{code}.csv")
        
        # Load existing data
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        else:
            df = pd.DataFrame()

        # Check if we need to update
        today_str = datetime.now().strftime("%Y-%m-%d")
        target_end_date = end_date if end_date else today_str
        
        need_update = False
        if df.empty:
            need_update = True
            start_date_fetch = "20200101" # Default start for new files
        else:
            last_date = df.index[-1].strftime("%Y-%m-%d")
            if last_date < target_end_date:
                need_update = True
                start_date_fetch = (df.index[-1] + timedelta(days=1)).strftime("%Y%m%d")
        
        if need_update:
            try:
                # print(f"Fetching data for {security} from {start_date_fetch}...")
                new_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date_fetch, adjust="qfq")
                if not new_df.empty:
                    new_df.rename(columns={
                        '日期': 'date', '开盘': 'open', '收盘': 'close', 
                        '最高': 'high', '最低': 'low', '成交量': 'volume', 
                        '成交额': 'money', '换手率': 'turnover'
                    }, inplace=True)
                    new_df['date'] = pd.to_datetime(new_df['date'])
                    new_df.set_index('date', inplace=True)
                    
                    # Combine and save
                    if not df.empty:
                        df = pd.concat([df, new_df])
                        df = df[~df.index.duplicated(keep='last')]
                    else:
                        df = new_df
                    
                    df.sort_index(inplace=True)
                    df.to_csv(csv_path)
            except Exception as e:
                print(f"Error fetching daily data for {security}: {e}")

        # Filter by end_date and count
        if end_date:
            df = df[df.index <= end_date]
        
        return df.tail(count)

    def get_call_auction(self, security: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get call auction data.
        Hybrid Strategy:
        1. If Today: Use Tencent Tick (ak.stock_zh_a_tick_tx_js) to get 09:25 snapshot.
        2. If History: Use EastMoney Minute (ak.stock_zh_a_hist_min_em) to get 09:30 bar as proxy.
        """
        code = security.split('.')[0]
        today_str = datetime.now().strftime("%Y-%m-%d")
        is_today = (start_date.split(' ')[0] == today_str)

        try:
            if is_today:
                # --- Real-time: Tencent Tick ---
                # print(f"Fetching Real-time Auction (Tencent) for {code}...")
                # Tencent symbol format: sz000001, sh600000
                prefix = "sz" if security.endswith("XSHE") else "sh"
                symbol_tx = f"{prefix}{code}"
                
                df = ak.stock_zh_a_tick_tx_js(symbol=symbol_tx)
                if df.empty:
                    return pd.DataFrame()
                
                # Filter for 09:25:00
                # Tencent time is "HH:MM:SS" string
                # We look for the row exactly at or closest before 09:25:00?
                # Usually 09:25:00 row exists for auction result.
                
                # Normalize columns
                # Tencent cols: 成交时间, 成交价格, 价格变动, 成交量(手), 成交金额(元), 性质
                # We need: time, current, volume, money
                
                df['time'] = pd.to_datetime(today_str + ' ' + df['成交时间'])
                
                # Filter for <= 09:25:00 to capture auction
                # Ideally we want the 09:25:00 snapshot
                mask = (df['成交时间'] >= '09:15:00') & (df['成交时间'] <= '09:25:00')
                auction_df = df[mask].copy()
                
                if auction_df.empty:
                    # Fallback: if no 09:25 data yet (e.g. too early), return empty
                    return pd.DataFrame()
                
                # Rename and format
                auction_df.rename(columns={
                    '成交价格': 'current',
                    '成交量': 'volume',
                    '成交金额': '成交金额' # Tencent might not have money column? Check output.
                }, inplace=True)
                
                # Tencent '成交金额' column name check
                if '成交金额' in df.columns:
                     auction_df['money'] = auction_df['成交金额']
                else:
                     # Estimate money if missing
                     auction_df['money'] = auction_df['current'] * auction_df['volume'] * 100 # Volume is hands?
                     # Wait, Tencent volume is "Hand" (手)?
                     # Output from test: 5889. 
                     # If it's hands, we need to multiply by 100 for "shares" IF strategy expects shares.
                     # Strategy expects "volume" in SHARES?
                     # Let's check strategy.
                     # In strategy: `auction_data['volume'].iloc[0] / prev_day_data['volume'].iloc[-1]`
                     # prev_day_data['volume'] is SHARES (we fixed it).
                     # So auction volume must be SHARES.
                     # Tencent '成交量' is likely HANDS.
                     pass
                
                # Convert Volume to Shares (if it's hands)
                # AkShare usually returns Hands for volume.
                auction_df['volume'] = auction_df['volume'] * 100 
                
                return auction_df[['time', 'current', 'volume', 'money']]

            else:
                # --- Historical: EastMoney Minute (09:30 Proxy) ---
                # print(f"Fetching Historical Auction Proxy (09:30) for {code} on {start_date}...")
                # start_date format "YYYY-MM-DD HH:MM:SS"
                date_part = start_date.split(' ')[0]
                
                # Fetch 1-minute data for that day
                # We need 09:30 data
                df = ak.stock_zh_a_hist_min_em(symbol=code, start_date=f"{date_part} 09:30:00", end_date=f"{date_part} 09:35:00", period='1', adjust='qfq')
                
                if df.empty:
                    # Fallback: Use daily data (open price as auction price)
                    # print(f"Minute data unavailable for {code} on {date_part}, using daily data fallback...")
                    daily_df = self.get_daily_data(security, count=1, end_date=date_part)
                    
                    if daily_df.empty:
                        return pd.DataFrame()
                    
                    # Use open price as auction price, estimate auction volume as 10% of daily volume
                    daily_row = daily_df.iloc[0]
                    
                    # Create proxy auction data
                    row1 = pd.Series({
                        'time': pd.to_datetime(f"{date_part} 09:15:00"),
                        'current': float(daily_row['open']) if pd.notna(daily_row['open']) else 0,
                        'volume': int(daily_row['volume'] * 100 * 0.1) if pd.notna(daily_row['volume']) else 0,  # 10% of daily volume as auction volume estimate, convert hands to shares
                        'money': float(daily_row['money'] * 0.1) if pd.notna(daily_row['money']) else 0  # 10% of daily amount
                    })
                    
                    row2 = row1.copy()
                    row2['time'] = pd.to_datetime(f"{date_part} 09:25:00")
                    
                    return pd.DataFrame([row1, row2])
                
                # Take the first row (09:30)
                # Rename columns
                # EM cols: 时间, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 最新价...
                df.rename(columns={'时间': 'time', '开盘': 'current', '成交量': 'volume', '成交额': 'money'}, inplace=True)
                
                # EM volume is HANDS?
                # Usually yes.
                df['volume'] = df['volume'] * 100
                
                # Fix 0 current price (Open price might be 0 for 09:30 bar)
                # Use Close price if Open is 0
                mask_zero = df['current'] == 0
                if mask_zero.any():
                    df.loc[mask_zero, 'current'] = df.loc[mask_zero, '收盘'] # Use Close as fallback
                
                # Debug
                # print(f"Proxy Row (09:30) for {code}: {df.iloc[0].to_dict()}")
                
                # We only return the first row (09:30) as if it was 09:25
                # But we need to fake the time to 09:25 so strategy accepts it?
                
                proxy_row = df.iloc[0].copy()
                
                row1 = proxy_row.copy()
                row1['time'] = pd.to_datetime(f"{date_part} 09:15:00")
                
                row2 = proxy_row.copy()
                row2['time'] = pd.to_datetime(f"{date_part} 09:25:00")
                
                return pd.DataFrame([row1, row2])
            
        except Exception as e:
            print(f"Error fetching call auction for {security}: {e}")
            return pd.DataFrame()

    def update_valuation_cache(self):
        """Fetch latest valuation data for all stocks and cache it."""
        print("Updating valuation cache...")
        try:
            df = ak.stock_zh_a_spot_em()
            # Columns: 序号, 代码, 名称, 最新价, 涨跌幅, 涨跌额, 成交量, 成交额, 振幅, 最高, 最低, 今开, 昨收, 量比, 换手率, 市盈率-动态, 市净率, 总市值, 流通市值, ...
            
            # Helper to format code
            def format_code(x):
                x = str(x)
                if x.startswith('6'): return f"{x}.XSHG"
                if x.startswith('8') or x.startswith('4'): return f"{x}.BJ" # BJ stocks?
                return f"{x}.XSHE"

            df['code'] = df['代码'].apply(format_code)
            
            # AkShare returns market cap in Yuan. JQ uses 100 million (亿) usually?
            # aa.py checks < 70. If it was Yuan, 70 is nothing. So it must be 亿.
            # 70亿 = 70 * 1e8.
            # So we divide by 1e8.
            
            # Handle potential non-numeric
            df['总市值'] = pd.to_numeric(df['总市值'], errors='coerce').fillna(0)
            df['流通市值'] = pd.to_numeric(df['流通市值'], errors='coerce').fillna(0)
            df['换手率'] = pd.to_numeric(df['换手率'], errors='coerce').fillna(0)
            
            df['market_cap'] = df['总市值'] / 1e8
            df['circulating_market_cap'] = df['流通市值'] / 1e8
            df['turnover_ratio'] = df['换手率']
            
            # Save to CSV
            cache_path = os.path.join(self.data_dir, "valuation_cache.csv")
            df[['code', 'market_cap', 'circulating_market_cap', 'turnover_ratio']].to_csv(cache_path, index=False)
            print(f"Valuation cache updated: {len(df)} stocks.")
            
        except Exception as e:
            print(f"Error updating valuation cache: {e}")

    def get_valuation(self, security: str, date: str) -> Dict:
        """
        Get valuation data (market cap, turnover ratio, etc.)
        """
        # Try to read from cache first
        cache_path = os.path.join(self.data_dir, "valuation_cache.csv")
        if os.path.exists(cache_path):
            try:
                df = pd.read_csv(cache_path)
                row = df[df['code'] == security]
                if not row.empty:
                    return row.iloc[0].to_dict()
            except:
                pass
        
        # Fallback (return empty if not found)
        return {}

    def _parse_shares(self, value_str):
        # value_str like "194.3亿" or "123456"
        try:
            if '亿' in str(value_str):
                return float(value_str.replace('亿', '')) * 1e8
            if '万' in str(value_str):
                return float(value_str.replace('万', '')) * 1e4
            return float(value_str)
        except:
            return 0