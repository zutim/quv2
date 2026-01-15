import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import akshare as ak
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from optimized_tdx_handler import get_call_auction_data, get_call_auction_batch_concurrent


class FastWebStrategySelector:
    def __init__(self):
        self.data_path = os.path.join(os.path.dirname(__file__), 'full_stock_data', 'daily_data')
        self.auction_data_cache = {}  # 竞价数据缓存

    def get_stock_name(self, stock_code):
        """获取股票名称"""
        try:
            # 尝试获取股票基本信息
            stock_info = ak.stock_individual_info_em(symbol=stock_code)
            if stock_info is not None and not stock_info.empty:
                name_row = stock_info[stock_info['item'] == '股票名称']
                if not name_row.empty:
                    return name_row['value'].iloc[0]
            return f"股票{stock_code}"
        except:
            return f"股票{stock_code}"

    def calculate_technical_indicators(self, df):
        """计算技术指标"""
        # 计算移动平均线等技术指标
        if len(df) >= 5:
            df['MA5'] = df['close'].rolling(window=5).mean()
        if len(df) >= 10:
            df['MA10'] = df['close'].rolling(window=10).mean()
        if len(df) >= 20:
            df['MA20'] = df['close'].rolling(window=20).mean()
        
        # 计算涨跌幅
        df['pct_change'] = df['close'].pct_change()
        
        return df

    def get_limit_up_stocks(self, date_str):
        """
        获取指定日期的涨停股票数据
        添加重试机制以处理网络超时问题
        """
        try:
            # 将日期格式从 YYYY-MM-DD 转换为 YYYYMMDD
            date_formatted = date_str.replace('-', '')
            
            print(f"正在获取 {date_str} 的涨停股票数据...")
            
            # 尝试多次获取数据
            for attempt in range(3):
                try:
                    # 获取涨停股池数据
                    df = ak.stock_zt_pool_em(date=date_formatted)
                    
                    if df.empty:
                        print(f"第 {attempt + 1} 次尝试：获取到空的涨停股票数据")
                        continue
                    
                    # 直接使用列的索引获取股票代码，第1列（索引为1）是股票代码
                    limit_up_stocks = df.iloc[:, 1].tolist()  # '代\u3000码\u3000' 列
                    print(f"第 {attempt + 1} 次尝试：成功获取到 {len(limit_up_stocks)} 只涨停股票")
                    return limit_up_stocks
                except Exception as e:
                    print(f"第 {attempt + 1} 次尝试失败: {e}")
                    if attempt < 2:  # 如果不是最后一次尝试，等待一下再重试
                        import time
                        time.sleep(1)
            
            print(f"获取涨停股票数据失败，经过3次尝试 {date_str}")
            return []
        except Exception as e:
            print(f"获取涨停股票数据失败 {date_str}: {e}")
            return []

    def get_realtime_auction_data(self, stock_code):
        """
        获取实时竞价数据（不带日期参数）
        通过调用外部API获取
        """
        try:
            # 获取当前日期
            current_date = datetime.now().strftime('%Y-%m-%d')
            current_date_numeric = datetime.now().strftime('%Y%m%d')
            
            # 为了确保获取到竞价时段数据，今天的数据也需要带上日期参数
            # 这样API会返回今天的完整数据，包括竞价时段
            url = f"http://localhost:8080/api/trade?code={stock_code}&date={current_date_numeric}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'List' in data['data']:
                    # 获取竞价时段的数据（9:25-9:30之间的数据）
                    trade_list = data['data']['List']
                    if trade_list:
                        # 筛选竞价时段的数据（9:25-9:30）
                        auction_trades = []
                        for trade in trade_list:
                            time_str = trade.get('Time', '')
                            # 检查是否在竞价时段（9:25-9:30）
                            if ('09:25:' in time_str or '09:26:' in time_str or 
                                '09:27:' in time_str or '09:28:' in time_str or 
                                '09:29:' in time_str):
                                auction_trades.append(trade)
                        
                        # 如果找到竞价时段数据，取最接近9:26-9:30的数据
                        if auction_trades:
                            # 按时间排序，取最后一个（最晚的，最接近9:26-9:30开盘的数据）
                            latest_auction = auction_trades[-1]
                            auction_data = {
                                'time': latest_auction.get('Time', ''),
                                'price': latest_auction.get('Price', 0) / 1000 if latest_auction.get('Price') else 0,  # 价格通常需要除以1000
                                'volume': latest_auction.get('Volume', 0),
                                'direction': 'B' if latest_auction.get('Status') == 1 else 'S',  # 1表示买入，0表示卖出
                                'order': latest_auction.get('Number', 0)
                            }
                            return auction_data
                        
                        # 如果没有找到竞价时段数据，但有9点时段的数据，取最接近9:26的数据
                        morning_trades = []
                        for trade in trade_list:
                            time_str = trade.get('Time', '')
                            if '09:' in time_str:  # 早上9点时段的数据
                                morning_trades.append(trade)
                        
                        if morning_trades:
                            # 按时间排序，取最后一个
                            latest_morning = morning_trades[-1]
                            auction_data = {
                                'time': latest_morning.get('Time', ''),
                                'price': latest_morning.get('Price', 0) / 1000 if latest_morning.get('Price') else 0,
                                'volume': latest_morning.get('Volume', 0),
                                'direction': 'B' if latest_morning.get('Status') == 1 else 'S',
                                'order': latest_morning.get('Number', 0)
                            }
                            return auction_data
                        
                        # 如果仍然没有找到早盘数据，取最后一条数据
                        last_trade = trade_list[-1]
                        auction_data = {
                            'time': last_trade.get('Time', ''),
                            'price': last_trade.get('Price', 0) / 1000 if last_trade.get('Price') else 0,
                            'volume': last_trade.get('Volume', 0),
                            'direction': 'B' if last_trade.get('Status') == 1 else 'S',
                            'order': last_trade.get('Number', 0)
                        }
                        return auction_data
                    else:
                        print("API返回空的交易列表")
                        return None
                else:
                    print(f"API返回格式错误: {data}")
                    return None
            else:
                print(f"API请求失败，状态码: {response.status_code}")
                return None
        except Exception as e:
            print(f"调用API获取实时竞价数据失败: {e}")
            return None

    def get_historical_auction_data(self, stock_code, date_str):
        """
        获取历史竞价数据
        通过调用外部API获取
        """
        try:
            # 获取当前日期
            current_date = datetime.now().strftime('%Y-%m-%d')
            current_date_numeric = datetime.now().strftime('%Y%m%d')
            
            # 判断日期是否为今天
            target_date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            target_date_numeric = target_date_obj.strftime('%Y%m%d')
            
            # 如果是今天，不带日期参数；否则带上日期参数
            if target_date_numeric == current_date_numeric:
                # 今天，不带日期参数
                url = f"http://localhost:8080/api/trade?code={stock_code}"
            else:
                # 历史日期，带日期参数
                url = f"http://localhost:8080/api/trade?code={stock_code}&date={target_date_numeric}"
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'List' in data['data']:
                    # 获取竞价时段的数据（9:25-9:26之间的数据）
                    trade_list = data['data']['List']
                    if trade_list:
                        # 筛选竞价时段的数据（9:25-9:30）
                        auction_trades = []
                        for trade in trade_list:
                            time_str = trade.get('Time', '')
                            # 检查是否在竞价时段（9:25-9:30）
                            if ('09:25:' in time_str or '09:26:' in time_str or 
                                '09:27:' in time_str or '09:28:' in time_str or 
                                '09:29:' in time_str):
                                auction_trades.append(trade)
                        
                        # 如果找到竞价时段数据，取最接近9:26-9:30的数据
                        if auction_trades:
                            # 按时间排序，取最后一个（最晚的，最接近9:26-9:30开盘的数据）
                            latest_auction = auction_trades[-1]
                            auction_data = {
                                'time': latest_auction.get('Time', ''),
                                'price': latest_auction.get('Price', 0) / 1000 if latest_auction.get('Price') else 0,  # 价格通常需要除以1000
                                'volume': latest_auction.get('Volume', 0),
                                'direction': 'B' if latest_auction.get('Status') == 1 else 'S',
                                'order': latest_auction.get('Number', 0)
                            }
                            return auction_data
                        
                        # 如果没有找到竞价时段数据，但有9点时段的数据，取最接近9:26的数据
                        morning_trades = []
                        for trade in trade_list:
                            time_str = trade.get('Time', '')
                            if '09:' in time_str:  # 早上9点时段的数据
                                morning_trades.append(trade)
                        
                        if morning_trades:
                            # 按时间排序，取最后一个
                            latest_morning = morning_trades[-1]
                            auction_data = {
                                'time': latest_morning.get('Time', ''),
                                'price': latest_morning.get('Price', 0) / 1000 if latest_morning.get('Price') else 0,
                                'volume': latest_morning.get('Volume', 0),
                                'direction': 'B' if latest_morning.get('Status') == 1 else 'S',
                                'order': latest_morning.get('Number', 0)
                            }
                            return auction_data
                        
                        # 如果仍然没有找到早盘数据，取最后一条数据
                        last_trade = trade_list[-1]
                        auction_data = {
                            'time': last_trade.get('Time', ''),
                            'price': last_trade.get('Price', 0) / 1000 if last_trade.get('Price') else 0,
                            'volume': last_trade.get('Volume', 0),
                            'direction': 'B' if last_trade.get('Status') == 1 else 'S',
                            'order': last_trade.get('Number', 0)
                        }
                        return auction_data
                    else:
                        print("API返回空的交易列表")
                        return None
                else:
                    print(f"API返回格式错误: {data}")
                    return None
            else:
                print(f"API请求失败，状态码: {response.status_code}")
                return None
        except Exception as e:
            print(f"调用API获取历史竞价数据失败: {e}")
            return None

    def filter_kcbj_stock(self, stock_list):
        """过滤科创板和北交所股票"""
        # 返回不是4开头、8开头以及68开头的股票
        return [stock for stock in stock_list if stock[0] != '4' and stock[0] != '8' and stock[:2] != '68']

    def filter_st_paused_stock(self, stock_list, date):
        """过滤ST和停牌股票"""
        # 由于没有实时数据，我们基于股票名称进行简单的ST过滤
        filtered_list = []
        for stock in stock_list:
            stock_name = self.get_stock_name(stock)
            # 过滤ST股票和名称中有"退"字的股票
            if 'ST' not in stock_name and '退' not in stock_name:
                filtered_list.append(stock)
        return filtered_list

    def filter_new_stock(self, stock_list, date, days=50):
        """过滤上市时间较短的新股"""
        # 这里简化处理，假设没有新股
        return stock_list

    def screen_stocks_by_date_with_pool(self, target_date_str, pool_data, max_stocks=5000):
        """
        使用预加载的股票池数据进行选股
        pool_data: 字典，包含涨停股票数据
        {
            "target_date": "2026-01-07",
            "prev_trading_date": "2026-01-06",
            "prev_2_trading_date": "2026-01-05",
            "limit_up_stocks": [...],  # 昨日涨停股票
            "limit_up_2_days_ago": [...],  # 前日涨停股票
            "first_board_stocks": [...]  # 首板股票
        }
        """
        try:
            # 将字符串转换为日期对象
            target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
            
            # 计算前一个交易日和前两个交易日
            prev_date = target_date - timedelta(days=1)
            while prev_date.weekday() >= 5:  # 跳过周末
                prev_date -= timedelta(days=1)
            prev_date_str = prev_date.strftime('%Y-%m-%d')
            
            prev_2_date = prev_date - timedelta(days=1)
            while prev_2_date.weekday() >= 5:  # 跳过周末
                prev_2_date -= timedelta(days=1)
            prev_2_date_str = prev_2_date.strftime('%Y-%m-%d')
            
            # 从预加载的股票池数据中获取涨停股票列表
            limit_up_stocks_today = pool_data.get('limit_up_stocks', [])
            limit_up_stocks_yesterday = pool_data.get('limit_up_stocks', [])  # 昨日涨停即为今天看昨天的涨停
            limit_up_2_days_ago = pool_data.get('limit_up_2_days_ago', [])
            first_board_stocks = pool_data.get('first_board_stocks', [])
            
            # 应用过滤条件
            filtered_first_board_stocks = self.filter_kcbj_stock(first_board_stocks)
            filtered_first_board_stocks = self.filter_st_paused_stock(filtered_first_board_stocks, target_date)
            filtered_first_board_stocks = self.filter_new_stock(filtered_first_board_stocks, target_date)
            
            # 只对首板股票进行处理（来自first_board_stocks）
            stocks_to_process = filtered_first_board_stocks
            
            # 获取数据文件列表
            csv_files = []
            if os.path.exists(self.data_path):
                csv_files = [os.path.join(self.data_path, f) for f in os.listdir(self.data_path) if f.endswith('.csv')]
            
            # 按策略分类的股票列表
            sbgk_stocks = []  # 首板高开
            sbdk_stocks = []  # 首板低开
            rzq_stocks = []  # 弱转强
            
            # 只检查需要处理的股票
            files_to_check = []
            for stock_code in stocks_to_process:
                file_path = os.path.join(self.data_path, f'{stock_code}.csv')
                if os.path.exists(file_path):
                    files_to_check.append(file_path)
            
            for file in files_to_check:
                stock_code = os.path.basename(file).replace('.csv', '').replace('.XSHG', '').replace('.XSHE', '')
                try:
                    df = pd.read_csv(file)
                    # 重命名列以处理中文列名
                    df.rename(columns={
                        '股\u3000\u3000票\u3000\u3000代\u3000\u3000码\u3000': 'code',
                        '股\u3000票\u3000代\u3000码': 'code',
                        '开\u3000\u3000盘': 'open',
                        '开\u3000盘': 'open',
                        '收\u3000\u3000盘': 'close',
                        '收\u3000盘': 'close',
                        '最\u3000\u3000高': 'high',
                        '最\u3000高': 'high',
                        '最\u3000\u3000低': 'low',
                        '最\u3000低': 'low',
                        '成\u3000\u3000交\u3000量': 'volume',
                        '成\u3000交\u3000量': 'volume',
                        '成\u3000\u3000交\u3000额': 'amount',
                        '成\u3000交\u3000额': 'amount',
                        '振\u3000\u3000幅': 'amplitude',
                        '振\u3000幅': 'amplitude',
                        '涨\u3000\u3000跌\u3000\u3000幅': 'pct_change',
                        '涨\u3000跌\u3000幅': 'pct_change',
                        '涨\u3000\u3000跌\u3000\u3000额': 'change',
                        '涨\u3000跌\u3000额': 'change',
                        '换\u3000\u3000手\u3000\u3000率': 'turnover',
                        '换\u3000手\u3000率': 'turnover'
                    }, inplace=True)
                    
                    df = self.calculate_technical_indicators(df)
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # 查找指定日期的数据
                    day_data = df[df['date'] == pd.to_datetime(target_date_str)]
                    if day_data.empty:
                        continue
                    
                    day_data = day_data.iloc[0]
                    current_close = float(day_data.get('close', 0)) if pd.notna(day_data.get('close', 0)) else 0
                    
                    if current_close <= 0:  # 无效价格，跳过
                        continue
                    
                    # 获取股票名称
                    stock_name = self.get_stock_name(stock_code)
                    
                    # 获取前一个交易日的数据
                    prev_day_data = df[df['date'] < pd.to_datetime(target_date_str)].tail(1)
                    if prev_day_data.empty:
                        continue
                    
                    prev_close = float(prev_day_data.iloc[0]['close']) if pd.notna(prev_day_data.iloc[0]['close']) else 0
                    current_open = float(day_data.get('open', 0)) if pd.notna(day_data.get('open', 0)) else 0
                    current_high = float(day_data.get('high', 0)) if pd.notna(day_data.get('high', 0)) else 0
                    current_low = float(day_data.get('low', 0)) if pd.notna(day_data.get('low', 0)) else 0
                    current_volume = float(day_data.get('volume', 0)) if pd.notna(day_data.get('volume', 0)) else 0
                    prev_volume = float(prev_day_data.iloc[0]['volume']) if pd.notna(prev_day_data.iloc[0]['volume']) else 1
                    prev_amount = float(prev_day_data.iloc[0]['amount']) if pd.notna(prev_day_data.iloc[0]['amount']) else 0
                    
                    if prev_close == 0:
                        continue
                    
                    # 使用涨停股票列表来判断是否为涨停板（更准确）
                    is_limit_up_today = stock_code in limit_up_stocks_today
                    was_limit_up_yesterday = stock_code in limit_up_stocks_yesterday
                    was_limit_up_2_days_ago = stock_code in limit_up_stocks_2_days_ago
                    
                    # 检查是否满足首板条件（昨日涨停，前日未涨停）
                    is_first_board = was_limit_up_yesterday and not was_limit_up_2_days_ago
                    
                    if is_first_board:
                        # 获取竞价数据
                        call_auction_data = self.get_historical_auction_data(stock_code, target_date_str)
                        if call_auction_data is not None:
                            auction_price = call_auction_data['price']
                            current_ratio = auction_price / prev_close if prev_close != 0 else 0
                            volume_ratio = call_auction_data['volume'] / prev_volume if prev_volume > 0 else 0
                        else:
                            # Fallback到开盘价
                            current_open = float(day_data.get('open', 0)) if pd.notna(day_data.get('open', 0)) else 0
                            current_ratio = current_open / prev_close if prev_close != 0 else 0
                            volume_ratio = current_volume / prev_volume if prev_volume > 0 else 0

                        # 首板高开条件 - 完整实现aa.py中的逻辑
                        # 条件一：均价，金额，市值，换手率
                        if prev_volume != 0 and prev_amount != 0:
                            avg_price = prev_amount / prev_volume
                            avg_price_increase_value = avg_price / prev_close * 1.1 - 1
                            condition1_part1 = avg_price_increase_value >= 0.07  # 收盘获利比例 >= 7%
                        else:
                            condition1_part1 = False  # 如果金额或成交量为0，条件不满足

                        condition1_part2 = 5.5e8 <= prev_amount <= 20e8  # 成交额在5.5亿-20亿之间
                        condition1 = condition1_part1 and condition1_part2

                        # 条件二：高开,开比
                        condition2_part1 = volume_ratio >= 0.03  # 集合竞价成交量占比 >= 3%
                        condition2_part2 = 1.0 < current_ratio < 1.06  # 开盘价在昨收价1%-6%之间
                        condition2 = condition2_part1 and condition2_part2

                        # 条件三：左压
                        # 获取历史数据计算左压条件
                        # 这里应该是获取到前一日（涨停日）为止的历史数据，用于确定左压周期
                        hst_data = df[df['date'] <= pd.to_datetime(prev_day_data.iloc[0]['date'])]
                        if len(hst_data) >= 2:
                            hst = hst_data.tail(101) if len(hst_data) >= 101 else hst_data
                            if len(hst) >= 2:
                                # 获取前一日的高点（涨停日的高点）
                                prev_high = float(hst.iloc[-1]['high']) if pd.notna(hst.iloc[-1]['high']) else 0

                                # 计算zyts_0：从倒数第2个开始往前，找到高点大于等于前一日高点的天数
                                zyts_0 = 100  # 默认值
                                for i in range(len(hst)-2, -1, -1):  # 从倒数第2个开始往前
                                    if i >= 0 and pd.notna(hst.iloc[i]['high']):
                                        if float(hst.iloc[i]['high']) >= prev_high:
                                            zyts_0 = len(hst) - 1 - i  # 计算天数
                                            break

                                zyts = zyts_0 + 5
                                # 获取高点以来的成交量数据（基于涨停日的历史数据）
                                volume_data_historical = hst['volume'].tail(zyts) if len(hst) >= zyts else hst['volume']

                                if len(volume_data_historical) >= 2:
                                    max_prev_vol = volume_data_historical.iloc[:-1].max() if len(volume_data_historical) > 1 else volume_data_historical.iloc[0]

                                    # 使用竞价数据中的成交量来检查左压条件
                                    if call_auction_data is not None:
                                        # 根据数据单位差异，对竞价成交量进行调整
                                        auction_volume = call_auction_data['volume'] * 10
                                    else:
                                        # 如果没有竞价数据，使用开盘后的成交量作为近似
                                        auction_volume = float(day_data.get('volume', 0)) if pd.notna(day_data.get('volume', 0)) else 0

                                    if max_prev_vol > 0:
                                        condition3 = auction_volume > max_prev_vol * 0.9  # 检查竞价成交量是否放大
                                    else:
                                        condition3 = True  # 如果前期成交量为0，条件通过
                                else:
                                    condition3 = False  # 如果数据不足，条件不满足
                            else:
                                condition3 = False  # 如果数据不足，条件不满足
                        else:
                            condition3 = False  # 如果数据不足，条件不满足

                        # 所有条件都满足
                        if condition1 and condition2 and condition3:
                            sbgk_stocks.append(stock_code)
                        
                        # 首板低开条件 - 昨日涨停，前日未涨停，今日低开（-9.5%到-3%）
                        # 同样在is_first_board条件下处理
                        # 获取竞价数据（如果还没获取的话）
                        if 'open_ratio' not in locals():
                            call_auction_data = self.get_historical_auction_data(stock_code, target_date_str)
                            if call_auction_data is not None:
                                auction_price = call_auction_data['price']
                                open_ratio = (auction_price - prev_close) / prev_close
                            else:
                                current_open = float(day_data.get('open', 0)) if pd.notna(day_data.get('open', 0)) else 0
                                open_ratio = (current_open - prev_close) / prev_close
                        
                        if -0.095 <= open_ratio <= -0.03:
                            # 类似地应用其他条件，如成交量等
                            prev_day_volume = float(prev_day_data.iloc[0]['volume']) if pd.notna(prev_day_data.iloc[0]['volume']) else 1
                            volume_ratio = current_volume / prev_day_volume if prev_volume > 0 else 0
                            if volume_ratio >= 0.03 and 'ST' not in stock_name and '退' not in stock_name:
                                sbdk_stocks.append(stock_code)
                    
                    # 弱转强条件 - 模拟aa.py中的弱转强逻辑
                    # 首先检查是否为炸板股（昨日曾涨停但未封板）
                    prev_high_val = float(prev_day_data.iloc[0]['high']) if pd.notna(prev_day_data.iloc[0]['high']) else 0
                    prev_close_val = float(prev_day_data.iloc[0]['close']) if pd.notna(prev_day_data.iloc[0]['close']) else 0
                    expected_limit_up = prev_close_val * 1.1  # 涨停价
                    tolerance = expected_limit_up * 0.01  # 容差
                    was_ever_limit_up_yesterday = (
                        abs(prev_high_val - expected_limit_up) < tolerance and 
                        abs(prev_close_val - expected_limit_up) > tolerance
                    )
                    
                    if was_ever_limit_up_yesterday:
                        # 获取最近4天的数据
                        recent_df = df[df['date'] <= pd.to_datetime(prev_day_data.iloc[0]['date'])].tail(4)
                        if len(recent_df) >= 4:
                            past_4_close = [float(row['close']) if pd.notna(row['close']) else 0 for _, row in recent_df.iterrows()]
                            prev_open_val = float(prev_day_data.iloc[0]['open']) if pd.notna(prev_day_data.iloc[0]['open']) else 0
                            
                            if len(past_4_close) >= 4 and all(c > 0 for c in past_4_close):
                                # 检查前3日涨幅 ≤ 28%
                                increase_ratio = (past_4_close[-1] - past_4_close[0]) / past_4_close[0]
                                if increase_ratio <= 0.28:  # 前3日涨幅 ≤ 28%
                                    # 检查前一日跌幅 ≥ -5%
                                    open_close_ratio = (prev_close_val - prev_open_val) / prev_open_val if prev_open_val != 0 else 0
                                    if open_close_ratio >= -0.05:  # 前一日跌幅 ≥ -5%
                                        # 获取竞价数据
                                        if call_auction_data is not None:
                                            # 计算开盘价相对于涨停价的比例
                                            limit_price = expected_limit_up
                                            current_ratio_to_limit = call_auction_data['price'] / limit_price if limit_price != 0 else 0
                                            
                                            # 检查竞价是否在合理范围（相对于涨停价）
                                            if 0.98 <= current_ratio_to_limit <= 1.09:  # -2% 到 +9%
                                                # 检查成交量占比
                                                volume_condition = call_auction_data['volume'] / prev_volume >= 0.03 if prev_volume > 0 else False
                                                if volume_condition:
                                                    # 检查均价涨幅
                                                    avg_price_increase_value = prev_amount / prev_volume / prev_close - 1 if prev_volume != 0 and prev_close != 0 else 0
                                                    if avg_price_increase_value >= -0.04:  # 均价涨幅 ≥ -4%
                                                        # 检查成交额
                                                        if 3e8 <= prev_amount <= 19e8:  # 成交额在3亿-19亿之间
                                                            # 检查左压条件
                                                            hst_data = df[df['date'] <= pd.to_datetime(prev_day_data.iloc[0]['date'])]
                                                            if len(hst_data) >= 2:
                                                                hst = hst_data.tail(101) if len(hst_data) >= 101 else hst_data
                                                                if len(hst) >= 2:
                                                                    # 获取前一日的高点
                                                                    prev_high = float(hst.iloc[-1]['high']) if pd.notna(hst.iloc[-1]['high']) else 0
                                                                    
                                                                    # 计算zyts_0
                                                                    zyts_0 = 100  # 默认值
                                                                    for i in range(len(hst)-2, -1, -1):  # 从倒数第2个开始往前
                                                                        if i >= 0 and pd.notna(hst.iloc[i]['high']):
                                                                            if float(hst.iloc[i]['high']) >= prev_high:
                                                                                zyts_0 = len(hst) - 1 - i  # 计算天数
                                                                                break
                                                                            
                                                                    zyts = zyts_0 + 5
                                                                    # 获取高点以来的成交量数据
                                                                    volume_data = hst['volume'].tail(zyts) if len(hst) >= zyts else hst['volume']
                                                                    
                                                                    if len(volume_data) >= 2 and pd.notna(volume_data.iloc[-1]):
                                                                        max_prev_vol = volume_data.iloc[:-1].max() if len(volume_data) > 1 else volume_data.iloc[0]
                                                                        # 检查前一日的成交量是否放大
                                                                        current_vol = float(hst.iloc[-1]['volume']) if pd.notna(hst.iloc[-1]['volume']) else 0
                                                                        
                                                                        if max_prev_vol > 0:
                                                                            left_pressure_condition = current_vol > max_prev_vol * 0.9
                                                                        else:
                                                                            left_pressure_condition = True  # 如果前期成交量为0，条件通过
                                                                    else:
                                                                        left_pressure_condition = False  # 如果数据不足，条件不满足
                                                                else:
                                                                    left_pressure_condition = False  # 如果数据不足，条件不满足
                                                            else:
                                                                left_pressure_condition = False  # 如果数据不足，条件不满足
                                                            
                                                            if left_pressure_condition:
                                                                rzq_stocks.append(stock_code)
                                        
                except Exception as e:
                    continue  # 跳过有问题的股票文件
            
            # 合并所有选中的股票
            all_qualified = sbgk_stocks + sbdk_stocks + rzq_stocks
            
            # 返回结果
            result = []
            for stock_code in all_qualified:
                stock_name = self.get_stock_name(stock_code)
                if stock_code in sbgk_stocks:
                    strategy_name = 'First Board High Open'
                elif stock_code in sbdk_stocks:
                    strategy_name = 'First Board Low Open'
                elif stock_code in rzq_stocks:
                    strategy_name = 'Weak to Strong'
                else:
                    strategy_name = 'Mixed Strategy'
                
                result.append({
                    'code': stock_code,
                    'name': stock_name,
                    'date': target_date_str,
                    'strategy': strategy_name
                })
            
            return result
        except Exception as e:
            print(f'选股过程中出错: {e}')
            return []

    def screen_stocks_by_date(self, target_date_str, max_stocks=5000):
        """按指定日期执行股票筛选，使用优化的算法（保留旧接口）"""
        # 获取股票池数据
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
        
        # 计算前一个交易日和前两个交易日
        prev_date = target_date - timedelta(days=1)
        while prev_date.weekday() >= 5:  # 跳过周末
            prev_date -= timedelta(days=1)
        prev_date_str = prev_date.strftime('%Y-%m-%d')
        
        prev_2_date = prev_date - timedelta(days=1)
        while prev_2_date.weekday() >= 5:  # 跳过周末
            prev_2_date -= timedelta(days=1)
        prev_2_date_str = prev_2_date.strftime('%Y-%m-%d')
        
        # 获取涨停股票数据
        limit_up_stocks_today = self.get_limit_up_stocks(target_date_str)
        limit_up_stocks_yesterday = self.get_limit_up_stocks(prev_date_str)
        limit_up_stocks_2_days_ago = self.get_limit_up_stocks(prev_2_date_str)
        
        # 创建股票池数据结构
        pool_data = {
            'today': limit_up_stocks_today,
            'yesterday': limit_up_stocks_yesterday,
            'two_days_ago': limit_up_stocks_2_days_ago
        }
        
        # 使用新方法进行筛选
        return self.screen_stocks_by_date_with_pool(target_date_str, pool_data, max_stocks)

    def select_stocks_consistent(self, target_date_str, max_workers=8):
        """使用与Web应用一致的算法选择股票"""
        return self.screen_stocks_by_date(target_date_str)

    def get_trade_data(self, stock_code, date_str=None):
        """
        对应 http://localhost:8080/api/trade?code=000001&date=20260108 接口
        如果没有日期参数，则获取实时数据
        """
        if date_str is None or date_str == "":
            # 获取实时数据（今天）
            return self.get_realtime_auction_data(stock_code)
        else:
            # 使用传入的日期
            return self.get_historical_auction_data(stock_code, date_str)