"""
Smart Data Updater
Updates only relevant stocks (Limit Up / Broken Limit Up) for recent days.
"""

import sys
import os
# 添加data_processing目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'data_processing'))

from local_data_manager import LocalDataManager
import akshare as ak
import concurrent.futures
import time
from datetime import datetime, timedelta
import pandas as pd

def get_trade_days(end_date, count):
    # Simple approximation
    days = []
    curr = end_date
    while len(days) < count:
        if curr.weekday() <= 4:
            days.append(curr)
        curr -= timedelta(days=1)
    return days

def update_smart():
    dm = LocalDataManager()
    
    # 1. Update Stock List (Fast enough)
    print("Updating stock list...")
    dm.update_stock_list()
    
    # 2. Identify target stocks
    target_stocks = set()
    
    today = datetime.now().date()
    # We need T-1, T-2, T-3
    # Assuming today is T (or T+1 if before market)
    # If running at 11:00, today is T. Yesterday is T-1.
    
    # Let's just fetch last 5 days limit ups to be safe
    days = get_trade_days(today - timedelta(days=1), 5)
    
    print(f"Checking limit ups for: {days}")
    
    for d in days:
        d_str = d.strftime("%Y%m%d")
        try:
            # Limit Up Pool
            df = ak.stock_zt_pool_em(date=d_str)
            if not df.empty:
                for code in df['代码'].values:
                    # code is '000001'
                    if str(code).startswith('6'):
                        target_stocks.add(f"{code}.XSHG")
                    else:
                        target_stocks.add(f"{code}.XSHE")
            
            # Broken Limit Up Pool (only need T-1 usually, but fetching more is fine)
            df_zbgc = ak.stock_zt_pool_zbgc_em(date=d_str)
            if not df_zbgc.empty:
                for code in df_zbgc['代码'].values:
                    if str(code).startswith('6'):
                        target_stocks.add(f"{code}.XSHG")
                    else:
                        target_stocks.add(f"{code}.XSHE")
                        
        except Exception as e:
            print(f"Error fetching pool for {d_str}: {e}")

    print(f"Found {len(target_stocks)} relevant stocks.")
    
    # 3. Fetch Daily Data for these stocks
    print("Updating daily data for target stocks...")
    
    def fetch_one(code):
        try:
            # Fetch last 100 days
            dm.get_daily_data(code, count=100)
            return True
        except Exception as e:
            print(f"Failed {code}: {e}")
            return False

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_one, list(target_stocks)))
    
    success = sum(results)
    print(f"Updated {success}/{len(target_stocks)} stocks in {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    update_smart()
