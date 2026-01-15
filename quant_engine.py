"""
Quant Engine
Mimics JoinQuant API for local execution.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from local_data_manager import LocalDataManager
import akshare as ak

class QuantEngine:
    def __init__(self):
        self.dm = LocalDataManager()
        self.current_dt = datetime.now()
        self.previous_date = self._get_previous_trading_date(self.current_dt.date())

    def _get_previous_trading_date(self, date_obj):
        # Simple approximation: previous weekday
        prev = date_obj - timedelta(days=1)
        while prev.weekday() > 4:
            prev -= timedelta(days=1)
        return prev

    def set_current_dt(self, dt):
        self.current_dt = dt
        self.previous_date = self._get_previous_trading_date(dt.date())

    # --- API Implementation ---

    def attribute_history(self, security, count, unit='1d', fields=None, skip_paused=True):
        if unit != '1d':
            raise NotImplementedError("Only 1d unit is supported")
        
        # Get data up to previous date (simulating running before market close or at open)
        # If running after close, we might want today's data?
        # aa.py runs 'buy' at 9:26. So it only has access to yesterday's close data.
        # But it uses 'get_current_data' for today's open/price.
        # 'attribute_history' in JQ usually excludes today if run during the day?
        # Yes, "skip_paused" etc.
        
        end_date = self.previous_date.strftime("%Y-%m-%d")
        df = self.dm.get_daily_data(security, count=count, end_date=end_date)
        
        # Adjust volume from Hands to Shares (AkShare returns Hands, JQ expects Shares)
        if 'volume' in df.columns:
            df['volume'] = df['volume'] * 100
        
        if fields:
            # Ensure all fields exist
            for f in fields:
                if f not in df.columns:
                    df[f] = 0 # Default or error?
            return df[fields]
        return df

    def get_current_data(self):
        return CurrentData(self.dm, self.current_dt)

    def get_call_auction(self, security, start_date, end_date, fields=None):
        # start_date and end_date are strings "YYYY-MM-DD HH:MM:SS"
        df = self.dm.get_call_auction(security, start_date, end_date)
        if fields and not df.empty:
            return df[fields]
        return df

    def get_valuation(self, security, start_date, end_date, fields=None):
        data = self.dm.get_valuation(security, end_date)
        if not data:
            return pd.DataFrame()
        return pd.DataFrame([data])

    def get_all_securities(self, types, date):
        # Return DataFrame with index as codes
        # Optimization: Only return stocks with data
        codes = self.dm.get_available_stocks()
        if not codes:
            # Fallback if no data (e.g. first run), return full list so we might fetch?
            # But here we want speed.
            codes = self.dm.get_stock_list()
            
        return pd.DataFrame(index=codes)

    def get_trade_days(self, end_date, count):
        # Return list of dates (datetime.date objects)
        days = []
        curr = end_date
        while len(days) < count:
            if curr.weekday() <= 4:
                days.append(curr)
            curr -= timedelta(days=1)
        return sorted(days)

    def get_price(self, security, end_date, frequency, fields, count, panel=False, fill_paused=False, skip_paused=False):
        # Used for get_hl_stock (Limit Up detection)
        # security can be a list
        if not isinstance(security, list):
            security = [security]
            
        dfs = []
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        for s in security:
            # We need 'count' days ending at 'end_date'
            df = self.dm.get_daily_data(s, count=count+1, end_date=end_date_str) # +1 for prev close
            if len(df) < 2:
                continue
                
            # Calculate high_limit
            # Limit = prev_close * 1.1 (approx)
            # We need to handle ST stocks (5%)
            # For now, assume 10% for all to keep it simple, or check name
            
            # Vectorized calculation
            df['prev_close'] = df['close'].shift(1)
            df['high_limit'] = round(df['prev_close'] * 1.1, 2) # Simple 10%
            
            # Drop the extra day
            df = df.iloc[1:]
            
            # Filter fields
            cols = fields if fields else df.columns
            # Ensure fields exist
            for f in cols:
                if f not in df.columns:
                    if f == 'paused':
                        df['paused'] = 0 # Assume not paused
                    else:
                        df[f] = 0
            
            df['code'] = s
            # Ensure code is included in result
            result_cols = list(cols)
            if 'code' not in result_cols:
                result_cols.append('code')
            
            dfs.append(df[result_cols])
            
        if not dfs:
            return pd.DataFrame()
            
        return pd.concat(dfs)

    def get_security_info(self, code):
        return SecurityInfo(code)


class CurrentData:
    def __init__(self, dm, current_dt):
        self.dm = dm
        self.current_dt = current_dt
        self._cache = {}

    def __getitem__(self, code):
        if code not in self._cache:
            self._cache[code] = CurrentStockData(code, self.dm, self.current_dt)
        return self._cache[code]

class CurrentStockData:
    def __init__(self, code, dm, current_dt):
        self.code = code
        self.dm = dm
        self.current_dt = current_dt
        self._name = code  # Default, will be fetched lazily if needed
        
        # Note: high_limit, day_open, is_st, paused are all @property methods
        # so we should not set them here
            
    @property
    def high_limit(self):
        # Calculate based on yesterday's close
        try:
            df = self.dm.get_daily_data(self.code, count=1)
            if not df.empty:
                return round(df['close'].iloc[-1] * 1.1, 2)
        except:
            pass
        return 0

    @property
    def day_open(self):
        # Get today's open.
        # If real-time, fetch spot.
        return 0 # Placeholder

    @property
    def is_st(self):
        return 'ST' in self._name
    
    @property
    def name(self):
        return self._name

class SecurityInfo:
    def __init__(self, code):
        self.display_name = code
        # Fetch start_date from DB or AkShare
        self.start_date = datetime(2000, 1, 1).date() 

