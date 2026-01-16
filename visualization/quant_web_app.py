import os
import json
import pandas as pd
from flask import Flask, render_template, request, jsonify
import glob
from datetime import datetime, timedelta
import akshare as ak
import numpy as np
import sqlite3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sys
import importlib
import time

# 初始化Flask应用
app = Flask(__name__)

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 移除pytdx相关代码
PYTDX_AVAILABLE = False
print("pytdx已被禁用")

# 移除TDX数据获取器
TDX_FETCHER_AVAILABLE = False
print("TDX数据获取器已被禁用")

# TDX连接池和缓存
tdx_connection = None
auction_data_cache = {}  # 竞价数据缓存

def get_tdx_connection():
    """获取TDX连接，复用现有连接 - 已禁用"""
    print("TDX功能已被禁用")
    return None

def close_tdx_connection():
    """关闭TDX连接 - 已禁用"""
    global tdx_connection
    print("TDX功能已被禁用")
    tdx_connection = None

def get_call_auction_data(stock_code, date_str):
    """
    获取指定股票和日期的开盘竞价数据（9:26之前的数据）
    使用新的API接口 http://localhost:8080/api/trade?code=301408
    """
    import requests
    from datetime import datetime
    
    # 检查缓存
    cache_key = f"{stock_code}_{date_str}"
    if cache_key in auction_data_cache:
        return auction_data_cache[cache_key]
    
    try:
        # 将日期格式从 YYYY-MM-DD 转换为 YYYYMMDD
        target_date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        target_date_numeric = target_date_obj.strftime('%Y%m%d')

        # 使用新的API接口
        url = f"http://localhost:8080/api/trade?code={stock_code}&date={target_date_numeric}"
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'List' in data['data']:
                # 从API响应中提取竞价时段的数据
                trade_list = data['data']['List']
                if trade_list:
                    # 筛选竞价时段的数据（9:25-9:29）
                    auction_trades = []
                    for trade in trade_list:
                        time_str = trade.get('Time', '')
                        # 检查是否在竞价时段（9:25-9:29）
                        if (' 09:25:' in time_str or ' 09:26:' in time_str or 
                            ' 09:27:' in time_str or ' 09:28:' in time_str or 
                            ' 09:29:' in time_str):
                            auction_trades.append(trade)

                    # 如果找到竞价时段数据，取最接近9:26-9:30的数据
                    if auction_trades:
                        # 按时间排序，取最后一个（最晚的，最接近9:30开盘的数据）
                        latest_auction = auction_trades[-1]
                        auction_data = {
                            'time': latest_auction.get('Time', ''),
                            'price': latest_auction.get('Price', 0) / 1000 if latest_auction.get('Price') else 0,  # 价格通常需要除以1000
                            'volume': latest_auction.get('Volume', 0) * 100,
                            'direction': 'B' if latest_auction.get('Status') == 1 else 'S',
                            'order': latest_auction.get('Number', 0)
                        }
                        # 缓存结果
                        auction_data_cache[cache_key] = auction_data
                        return auction_data

                    # 如果没有找到竞价时段数据，但有9点时段的数据，取最接近9:26的数据
                    morning_trades = []
                    for trade in trade_list:
                        time_str = trade.get('Time', '')
                        if ' 09:' in time_str:  # 早上9点时段的数据
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
                        # 缓存结果
                        auction_data_cache[cache_key] = auction_data
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
                    # 缓存结果
                    auction_data_cache[cache_key] = auction_data
                    return auction_data
                else:
                    print(f"API返回空的交易列表")
                    return get_call_auction_data_fallback(stock_code, date_str)  # 回退到原方案
            else:
                print(f"API响应格式不正确: {data}")
                return get_call_auction_data_fallback(stock_code, date_str)  # 回退到原方案
        else:
            print(f"API请求失败，状态码: {response.status_code}")
            return get_call_auction_data_fallback(stock_code, date_str)  # 回退到原方案
    except Exception as e:
        print(f"获取竞价数据时发生错误: {e}")
        return get_call_auction_data_fallback(stock_code, date_str)  # 回退到原方案

def get_call_auction_data_fallback(stock_code, date_str):
    """
    获取指定股票和日期的开盘竞价数据（9:26之前的数据）- 回落方案
    使用akshare的接口模拟JoinQuant的get_call_auction()功能
    """
    import time
    
    # 重试逻辑：最多重试2次
    max_retries = 2
    for attempt in range(max_retries + 1):  # 总共尝试3次（1次初始 + 2次重试）
        try:
            # 根据股票代码确定市场前缀
            if stock_code.startswith('6'):
                symbol = f"sh{stock_code}"
            else:
                symbol = f"sz{stock_code}"
            
            # 获取分时交易数据
            tick_df = ak.stock_zh_a_tick_tx_js(symbol=symbol)
            
            # 检查是否成功获取到数据
            if tick_df is None or tick_df.empty:
                if attempt < max_retries:
                    print(f"获取 {stock_code} 的分时数据失败，正在重试 ({attempt + 1}/{max_retries})...")
                    time.sleep(1)  # 等待1秒后重试
                    continue
                else:
                    print(f"获取 {stock_code} 的分时数据失败，已达到最大重试次数 ({max_retries})，中断执行")
                    return None
            
            # 筛选9:26之前的数据（竞价通常在9:25开始到9:26:00之前）
            # 需要找到9:26之前最新的数据点
            before_926_data = tick_df[
                (tick_df.iloc[:, 0].str.startswith('09:25:')) | 
                (tick_df.iloc[:, 0].str.startswith('09:26:00')) |
                (tick_df.iloc[:, 0].str.startswith('09:26:01')) |
                (tick_df.iloc[:, 0].str.startswith('09:26:02')) |
                (tick_df.iloc[:, 0].str.startswith('09:26:03')) |
                (tick_df.iloc[:, 0].str.startswith('09:26:04')) |
                (tick_df.iloc[:, 0].str.startswith('09:26:05'))
            ]
            
            if not before_926_data.empty:
                # 找到9:26之前最新的数据点
                # 首先确保时间列格式正确，然后按时间排序取最后一条
                # 将时间列转换为datetime格式进行排序
                time_col_idx = 0  # 第一列是时间
                sorted_data = before_926_data.sort_values(by=before_926_data.columns[time_col_idx])
                latest_row = sorted_data.iloc[-1]  # 取最新的数据
                
                result = {
                    'time': latest_row.iloc[0],  # 成交时间
                    'price': float(latest_row.iloc[1]),  # 成交价格
                    'change': float(latest_row.iloc[2]),  # 价格变动
                    'volume': int(latest_row.iloc[3]),  # 成交量
                    'amount': int(latest_row.iloc[4]),  # 成交金额
                    'nature': latest_row.iloc[5]  # 性质
                }
                
                # 缓存结果
                cache_key = f"{stock_code}_{date_str}"
                auction_data_cache[cache_key] = result
                return result
            else:
                # 如果没有9:26之前的数据，尝试重试
                if attempt < max_retries:
                    print(f"未找到 {stock_code} 在9:26之前的竞价数据，正在重试 ({attempt + 1}/{max_retries})...")
                    time.sleep(1)  # 等待1秒后重试
                    continue
                else:
                    print(f"未找到 {stock_code} 在9:26之前的竞价数据，已达到最大重试次数 ({max_retries})，中断执行")
                    return None
        except Exception as e:
            # 记录错误但不输出过多日志
            if attempt < max_retries:
                print(f"获取 {stock_code} 的竞价数据时发生错误: {str(e)}，正在重试 ({attempt + 1}/{max_retries})...")
                time.sleep(1)  # 等待1秒后重试
                continue
            else:
                print(f"获取 {stock_code} 的竞价数据时发生错误: {str(e)}，已达到最大重试次数 ({max_retries})，中断执行")
                return None
    
    # 如果所有重试都失败，返回None
    return None

def get_call_auction_batch(stock_codes, date_str):
    """
    批量获取多只股票的开盘竞价数据
    模拟JoinQuant的get_call_auction()批量功能
    """
    results = {}
    for code in stock_codes:
        results[code] = get_call_auction_data(code, date_str)
    return results

def get_limit_up_stocks(date_str):
    """
    获取指定日期的涨停股票数据
    """
    try:
        # 将日期格式从 YYYY-MM-DD 转换为 YYYYMMDD
        date_formatted = date_str.replace('-', '')
        
        # 获取涨停股池数据
        df = ak.stock_zt_pool_em(date=date_formatted)
        
        if df.empty:
            return []
        
        # 直接使用列的索引获取股票代码，第1列（索引为1）是股票代码
        limit_up_stocks = df.iloc[:, 1].tolist()  # '代\u3000码\u3000' 列
        return limit_up_stocks
    except Exception as e:
        print(f"获取涨停股票数据失败 {date_str}: {e}")
        return []

def get_strong_stocks(date_str):
    """
    获取强势股数据
    """
    try:
        # 将日期格式从 YYYY-MM-DD 转换为 YYYYMMDD
        date_formatted = date_str.replace('-', '')
        
        # 获取强势股池数据
        df = ak.stock_zt_pool_strong_em(date=date_formatted)
        
        if df.empty:
            return []
        
        # 直接使用列的索引获取股票代码，第1列（索引为1）是股票代码
        strong_stocks = df.iloc[:, 1].tolist()  # '代\u3000码\u3000' 列
        return strong_stocks
    except Exception as e:
        print(f"获取强势股票数据失败 {date_str}: {e}")
        return []

def get_stock_name(stock_code):
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

def calculate_technical_indicators(df):
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

@app.route('/')
def index():
    """渲染主页"""
    return render_template('index.html')

def screen_stocks_by_date(target_date_str, strategy='mixed', max_stocks=200):
    """按指定日期执行股票筛选，输出日志"""
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
        
        # 获取指定日期的涨停股票数据
        limit_up_stocks_today = get_limit_up_stocks(target_date_str)
        limit_up_stocks_yesterday = get_limit_up_stocks(prev_date_str)
        limit_up_stocks_2_days_ago = get_limit_up_stocks(prev_2_date_str)
        
        # 定义数据路径 - 修正为相对于项目根目录的路径
        DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'full_stock_data', 'daily_data')
        
        # 从股票池加载首板股票，只对这些股票进行筛选以提高性能
        try:
            pool_data_dir = os.path.join(os.path.dirname(__file__), 'full_stock_data', 'pool_data')
            pool_file = os.path.join(pool_data_dir, f"pool_{target_date_str}.json")
            
            if os.path.exists(pool_file):
                with open(pool_file, 'r', encoding='utf-8') as f:
                    pool_data = json.load(f)
                first_board_stocks = pool_data.get('first_board_stocks', [])
            else:
                # 如果没有对应的pool文件，使用传统的扫描方式，但限制数量
                csv_files = glob.glob(os.path.join(DATA_PATH, "*.csv"))
                first_board_stocks = []
                for file_path in csv_files[:max_stocks]:
                    stock_code = os.path.basename(file_path).replace('.csv', '')
                    first_board_stocks.append(stock_code)
        except Exception as e:
            logger.error(f'加载股票池数据失败: {e}')
            # 如果加载失败，使用传统的扫描方式
            csv_files = glob.glob(os.path.join(DATA_PATH, "*.csv"))
            first_board_stocks = []
            for file_path in csv_files[:max_stocks]:
                stock_code = os.path.basename(file_path).replace('.csv', '')
                first_board_stocks.append(stock_code)
        
        # 按策略分类的股票列表
        sbgk_stocks = []  # 首板高开
        sbdk_stocks = []  # 首板低开
        rzq_stocks = []  # 弱转强
        
        # 只检查股票池中的股票以提高性能
        for stock_code in first_board_stocks[:max_stocks]:
            try:
                file = os.path.join(DATA_PATH, f"{stock_code}.csv")
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
                
                df = calculate_technical_indicators(df)
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
                stock_name = get_stock_name(stock_code)
                
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
                
                # 使用竞价数据而不是开盘价
                call_auction_data = get_call_auction_data(stock_code, target_date_str)
                if call_auction_data is not None:
                    auction_price = call_auction_data['price']
                    open_ratio = (auction_price - prev_close) / prev_close  # 使用竞价价格vs前收盘价
                    current_ratio = auction_price / prev_close  # 使用竞价价格vs前收盘价
                else:
                    # Fallback到开盘价
                    current_open = float(day_data.get('open', 0)) if pd.notna(day_data.get('open', 0)) else 0
                    open_ratio = (current_open - prev_close) / prev_close
                    current_ratio = current_open / prev_close if prev_close != 0 else 0
                
                # 使用涨停股票列表来判断是否为涨停板（更准确）
                is_limit_up_today = stock_code in limit_up_stocks_today
                was_limit_up_yesterday = stock_code in limit_up_stocks_yesterday
                was_limit_up_2_days_ago = stock_code in limit_up_stocks_2_days_ago
                
                # 首板高开条件 - 更接近aa.py的完整逻辑
                # 1. 昨日涨停，前日未涨停
                if was_limit_up_yesterday and not was_limit_up_2_days_ago:
                    # aa.py的首板高开条件：
                    # 条件一：均价，金额，市值，换手率
                    # avg_price_increase_value = prev_day_data['money'][0] / prev_day_data['volume'][0] / prev_day_data['close'][0] *1.1 - 1
                    # 过滤：收盘获利比例低于7%，成交额小于5.5亿或者大于20亿
                    if prev_volume != 0 and prev_amount != 0:
                        avg_price = prev_amount / prev_volume
                        avg_price_increase_value = avg_price / prev_close * 1.1 - 1
                        condition1_part1 = avg_price_increase_value >= 0.07  # 收盘获利比例 >= 7%
                    else:
                        condition1_part1 = True  # 如果金额或成交量为0，跳过此检查
                    
                    condition1_part2 = 5.5e8 <= prev_amount <= 20e8  # 成交额在5.5亿-20亿之间
                    condition1 = condition1_part1 and condition1_part2
                    
                    # 条件二：高开,开比
                    if prev_close != 0:
                        volume_ratio = current_volume / prev_volume if prev_volume > 0 else 0
                        condition2_part1 = volume_ratio >= 0.03  # 成交量占比
                        condition2_part2 = 1 < current_ratio < 1.06  # 高开但未涨停
                        condition2 = condition2_part1 and condition2_part2
                    else:
                        condition2 = False
                    
                    # 条件三：左压
                    # 获取历史数据计算左压条件
                    # 获取前一日及之前的数据来检查左压条件
                    hst_data = df[df['date'] <= pd.to_datetime(prev_date_str)]
                    if len(hst_data) >= 2:
                        hst = hst_data.tail(101) if len(hst_data) >= 101 else hst_data
                        if len(hst) >= 2:
                            # 获取前一日的高点
                            prev_high = float(hst.iloc[-1]['high']) if pd.notna(hst.iloc[-1]['high']) else 0
                            
                            # 计算zyts_0：从倒数第2个开始往前，找到高点大于等于前一日高点的天数
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
                                # 注意：这里应该是前一日的成交量，而不是当日成交量
                                current_vol = float(hst.iloc[-1]['volume']) if pd.notna(hst.iloc[-1]['volume']) else 0
                                
                                if max_prev_vol > 0:
                                    condition3 = current_vol > max_prev_vol * 0.9  # 检查前一日的成交量是否放大
                                else:
                                    condition3 = True  # 如果前期成交量为0，条件通过
                            else:
                                condition3 = True  # 如果数据不足，条件通过
                        else:
                            condition3 = True  # 如果数据不足，条件通过
                    else:
                        condition3 = True  # 如果数据不足，条件通过
                    
                    # 所有条件都满足
                    if condition1 and condition2 and condition3:
                        # 检查是否非ST股票等其他条件
                        if 'ST' not in stock_name and '退' not in stock_name:  # 类似于aa.py的过滤条件
                            sbgk_stocks.append(stock_code)
                
                # 首板低开条件 - 昨日涨停，前日未涨停，今日低开（-9.5%到-3%）
                elif was_limit_up_yesterday and not was_limit_up_2_days_ago and -0.095 <= open_ratio <= -0.03:
                    # 类似地应用其他条件，如成交量等
                    prev_day_volume = float(prev_day_data.iloc[0]['volume']) if pd.notna(prev_day_data.iloc[0]['volume']) else 1
                    volume_ratio = current_volume / prev_day_volume if prev_day_volume > 0 else 0
                    if volume_ratio >= 0.03 and 'ST' not in stock_name and '退' not in stock_name:
                        sbdk_stocks.append(stock_code)
                
                # 弱转强条件 - 模拟aa.py中的弱转强逻辑
                elif len(df) >= 5:
                    recent_df = df[df['date'] <= pd.to_datetime(target_date_str)].tail(5)
                    if len(recent_df) >= 5:
                        past_4_close = recent_df['close'].tail(5).tolist()[:-1]  # 前4天收盘价
                        current_close_val = float(day_data.get('close', 0))
                        
                        if len(set(past_4_close)) > 1:  # 确保不是相同价格
                            # 检查是否前期涨幅不超过28%
                            if len(past_4_close) > 1:
                                increase_ratio = (past_4_close[-1] - past_4_close[0]) / past_4_close[0]
                                if increase_ratio <= 0.28:  # 前期涨幅不超过28%
                                    # 检查前一日收盘是否比开盘价高不超过5%
                                    prev_day_open = float(prev_day_data.iloc[0]['open']) if pd.notna(prev_day_data.iloc[0]['open']) else 0
                                    if prev_day_open != 0:
                                        open_close_ratio = (prev_close - prev_day_open) / prev_day_open
                                        if open_close_ratio >= -0.05:  # 前一日收盘价不低于开盘价5%以上
                                            # 检查今日是否高开且价格超过前期高点
                                            if open_ratio > 0.02 and current_close_val > max(past_4_close):
                                                # 检查成交量是否放大（可选条件）
                                                prev_day_volume = float(prev_day_data.iloc[0]['volume']) if pd.notna(prev_day_data.iloc[0]['volume']) else 1
                                                if prev_day_volume > 0 and current_volume / prev_day_volume >= 0.03:  # 成交量占比
                                                    rzq_stocks.append(stock_code)
                                
            except Exception as e:
                continue  # 跳过有问题的股票文件
        
        # 合并所有选中的股票
        all_qualified = sbgk_stocks + sbdk_stocks + rzq_stocks
        
        # 记录日志，模仿aa.py的格式
        logger.info(f'今日选股：{all_qualified}')
        logger.info(f'首板高开：{sbgk_stocks}')
        logger.info(f'首板低开：{sbdk_stocks}')
        logger.info(f'弱转强：{rzq_stocks}')
        
        # 返回结果
        result = []
        for stock_code in all_qualified:
            stock_name = get_stock_name(stock_code)
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
        logger.error(f'选股过程中出错: {e}')
        return []

@app.route('/api/screen_stocks', methods=['POST'])
def screen_stocks():
    """执行股票筛选"""
    try:
        criteria = request.json
        target_date = criteria.get('date', datetime.now().strftime('%Y-%m-%d'))  # 使用当前日期作为默认值
        strategy = criteria.get('strategy', 'mixed')
        
        screened_stocks = screen_stocks_by_date(target_date, strategy)
        
        return jsonify(screened_stocks)
    except Exception as e:
        logger.error(f'选股API出错: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/fast_screen_stocks', methods=['POST'])
def fast_screen_stocks():
    """执行快速股票筛选，使用与Web应用一致的算法"""
    try:
        criteria = request.json
        target_date = criteria.get('date', datetime.now().strftime('%Y-%m-%d'))  # 使用前端传递的日期
        
        # 动态导入select_2026_01_12模块
        try:
            import sys
            import os
            # 添加selection目录到Python路径
            selection_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'selection')
            if selection_path not in sys.path:
                sys.path.insert(0, selection_path)
            
            selection_module = __import__('select_2026_01_12', fromlist=['TodayStockSelector'])
            TodayStockSelector = getattr(selection_module, 'TodayStockSelector')
            
            selector = TodayStockSelector()
            
            # 读取指定日期的pool数据
            pool_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'full_stock_data', 'pool_data', f'pool_{target_date}.json')
            
            if not os.path.exists(pool_file_path):
                return jsonify({
                    'error': f'未找到{target_date}的pool数据文件: {pool_file_path}'
                }), 404
            
            with open(pool_file_path, 'r', encoding='utf-8') as f:
                pool_data = json.load(f)
            
            start_time = time.time()
            results = selector.select_stocks_from_pool(target_date, pool_data)
            execution_time = time.time() - start_time
            
            # 计算各策略数量
            sbgk_count = sum(1 for r in results if r['strategy'] == 'First Board High Open')
            sbdk_count = sum(1 for r in results if r['strategy'] == 'First Board Low Open')
            rzq_count = sum(1 for r in results if r['strategy'] == 'Weak to Strong')
            
            return jsonify({
                'stocks': results,
                'summary': {
                    'total_selected': len(results),
                    'sbgk_count': sbgk_count,
                    'sbdk_count': sbdk_count,
                    'rzq_count': rzq_count,
                    'execution_time': execution_time
                },
                'source': 'select_2026_01_12'
            })
        except ImportError as e:
            logger.error(f'无法导入select_2026_01_12模块: {e}')
            return jsonify({'error': '无法导入select_2026_01_12模块，请确保select_2026_01_12.py文件存在'}), 500
        except Exception as e:
            logger.error(f'使用select_2026_01_12选股过程中出错: {e}')
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'使用select_2026_01_12选股执行出错: {str(e)}'}), 500
    except Exception as e:
        logger.error(f'快速选股API出错: {e}')
        return jsonify({'error': str(e)}), 500

def load_and_filter_stock_pool(date_str):
    """从pool_data加载股票池并应用筛选条件"""
    import pandas as pd
    
    # 获取前一个交易日
    target_date = datetime.strptime(date_str, '%Y-%m-%d')
    prev_date = target_date - timedelta(days=1)
    while prev_date.weekday() >= 5:  # 跳过周末
        prev_date -= timedelta(days=1)
    prev_date_str = prev_date.strftime('%Y-%m-%d')
    
    DATA_PATH = os.path.join(os.path.dirname(__file__), 'full_stock_data', 'daily_data')
    
    # 加载pool_data中的首板股票
    pool_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'full_stock_data', 'pool_data')
    pool_file = os.path.join(pool_data_dir, f"pool_{date_str}.json")
    
    if not os.path.exists(pool_file):
        return []
    
    with open(pool_file, 'r', encoding='utf-8') as f:
        pool_data = json.load(f)
    
    first_board_stocks = pool_data.get('first_board_stocks', [])
    
    # 从本地数据中获取这些股票的详细信息并应用筛选条件
    qualified_stocks = []
    
    for stock_code in first_board_stocks:
        try:
            file_path = os.path.join(DATA_PATH, f"{stock_code}.csv")
            if not os.path.exists(file_path):
                # 尝试带交易所后缀的文件名
                for suffix in ['.XSHG', '.XSHE']:
                    alt_file_path = os.path.join(DATA_PATH, f"{stock_code}{suffix}.csv")
                    if os.path.exists(alt_file_path):
                        file_path = alt_file_path
                        break
            
            if not os.path.exists(file_path):
                continue
            
            df = pd.read_csv(file_path)
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
            
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            # 查找指定日期的数据
            day_data = df[df['date'] == pd.to_datetime(date_str)]
            if day_data.empty:
                continue
            
            day_data = day_data.iloc[0]
            current_close = float(day_data.get('close', 0)) if pd.notna(day_data.get('close', 0)) else 0
            
            if current_close <= 0:  # 无效价格，跳过
                continue
            
            # 获取前一个交易日的数据
            prev_day_data = df[df['date'] < pd.to_datetime(date_str)].tail(1)
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
            
            # 计算开盘比例
            open_ratio = (current_open - prev_close) / prev_close
            current_ratio = current_open / prev_close if prev_close != 0 else 0
            
            # 只有在初步筛选通过后，才尝试获取更精确的竞价数据
            # 以避免对所有股票都进行昂贵的API调用
            
            # 首先使用开盘价进行初步筛选
            # 条件二：高开,开比 - 初步筛选
            if prev_close != 0:
                volume_ratio = current_volume / prev_volume if prev_volume > 0 else 0
                condition2_part1 = volume_ratio >= 0.03  # 成交量占比
                condition2_part2 = 1 < current_ratio < 1.06  # 高开但未涨停
                condition2_initial = condition2_part1 and condition2_part2
            else:
                condition2_initial = False
            
            # 应用首板高开的筛选条件
            # 条件一：均价，金额，市值，换手率
            if prev_volume != 0 and prev_amount != 0:
                avg_price = prev_amount / prev_volume
                avg_price_increase_value = avg_price / prev_close * 1.1 - 1
                condition1_part1 = avg_price_increase_value >= 0.07  # 收盘获利比例 >= 7%
            else:
                condition1_part1 = True  # 如果金额或成交量为0，跳过此检查
            
            condition1_part2 = 5.5e8 <= prev_amount <= 20e8  # 成交额在5.5亿-20亿之间
            condition1 = condition1_part1 and condition1_part2
            
            # 如果初步筛选通过，再尝试获取竞价数据进行精确判断
            if condition1 and condition2_initial:
                # 获取竞价数据以进行更精确的判断（使用pytdx如果可用）
                call_auction_data = None
                try:
                    call_auction_data = get_call_auction_data(stock_code, date_str)
                except:
                    # 如果获取竞价数据失败，继续使用开盘价数据
                    call_auction_data = None
                
                if call_auction_data is not None:
                    auction_price = call_auction_data['price']
                    open_ratio = (auction_price - prev_close) / prev_close  # 使用竞价价格vs前收盘价
                    current_ratio = auction_price / prev_close  # 使用竞价价格vs前收盘价
                    
                    # 使用竞价数据重新计算条件二
                    volume_ratio = current_volume / prev_volume if prev_volume > 0 else 0
                    condition2_part1 = volume_ratio >= 0.03  # 成交量占比
                    condition2_part2 = 1 < current_ratio < 1.06  # 高开但未涨停
                    condition2 = condition2_part1 and condition2_part2
                else:
                    # 如果无法获取竞价数据，使用开盘价数据
                    condition2 = condition2_initial
            else:
                # 如果初步筛选未通过，直接使用计算结果
                condition2 = condition2_initial
            
            # 条件三：左压
            hst_data = df[df['date'] <= pd.to_datetime(prev_date_str)]
            if len(hst_data) >= 2:
                hst = hst_data.tail(101) if len(hst_data) >= 101 else hst_data
                if len(hst) >= 2:
                    # 获取前一日的高点
                    prev_high = float(hst.iloc[-1]['high']) if pd.notna(hst.iloc[-1]['high']) else 0
                    
                    # 计算zyts_0：从倒数第2个开始往前，找到高点大于等于前一日高点的天数
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
                        # 注意：这里应该是前一日的成交量，而不是当日成交量
                        current_vol = float(hst.iloc[-1]['volume']) if pd.notna(hst.iloc[-1]['volume']) else 0
                        
                        if max_prev_vol > 0:
                            condition3 = current_vol > max_prev_vol * 0.9  # 检查前一日的成交量是否放大
                        else:
                            condition3 = True  # 如果前期成交量为0，条件通过
                    else:
                        condition3 = True  # 如果数据不足，条件通过
                else:
                    condition3 = True  # 如果数据不足，条件通过
            else:
                condition3 = True  # 如果数据不足，条件通过
            
            # 所有条件都满足
            if condition1 and condition2 and condition3:
                # 检查是否非ST股票等其他条件
                stock_name = get_stock_name(stock_code)
                if 'ST' not in stock_name and '退' not in stock_name:  # 类似于Web应用的过滤条件
                    qualified_stocks.append({
                        'code': stock_code,
                        'name': stock_name,
                        'date': date_str,
                        'strategy': 'First Board High Open'
                    })
        
        except Exception as e:
            # 如果处理某只股票出错，继续处理下一只
            continue
    
    return qualified_stocks

@app.route('/api/data_status')
def get_data_status():
    """获取本地数据状态"""
    try:
        import glob
        from datetime import datetime
        import pandas as pd
        
        daily_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'full_stock_data', 'daily_data')
        
        if not os.path.exists(daily_data_dir):
            return jsonify({
                'status': 'error',
                'message': '数据目录不存在',
                'latest_date': 'N/A',
                'total_stocks': 0
            })
        
        # 获取所有CSV文件
        csv_files = glob.glob(os.path.join(daily_data_dir, "*.csv"))
        
        # 获取最新数据日期
        latest_date = None
        for file in csv_files[:100]:  # 只检查前100个文件以提高性能
            try:
                df = pd.read_csv(file)
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    file_latest = df['date'].max()
                    if latest_date is None or file_latest > latest_date:
                        latest_date = file_latest
            except:
                continue
        
        latest_date_str = latest_date.strftime('%Y-%m-%d') if latest_date else 'N/A'
        
        return jsonify({
            'status': 'success',
            'latest_date': latest_date_str,
            'total_stocks': len(csv_files),
            'message': f'数据最新到 {latest_date_str}，共 {len(csv_files)} 只股票'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'latest_date': 'N/A',
            'total_stocks': 0
        })


@app.route('/api/trade')
def get_trade_data():
    """获取股票竞价数据，支持实时和历史数据
    对应接口: http://localhost:8080/api/trade?code=000001&date=20260108
    如果不带日期参数，则获取实时竞价数据
    """
    try:
        code = request.args.get('code')
        date = request.args.get('date', '')  # 默认为空字符串，表示获取实时数据
        
        if not code:
            return jsonify({'error': '股票代码不能为空'}), 400
        
        # 动态导入FastWebStrategySelector
        sys.path.append(os.path.dirname(__file__))  # 添加当前目录到路径
        fast_selector_module = importlib.import_module('fast_web_strategy')
        FastWebStrategySelector = getattr(fast_selector_module, 'FastWebStrategySelector')
        
        selector = FastWebStrategySelector()
        
        # 获取竞价数据
        auction_data = selector.get_trade_data(code, date)
        
        if auction_data is not None:
            return jsonify({
                'code': code,
                'auction_data': auction_data,
                'success': True
            })
        else:
            return jsonify({
                'code': code,
                'error': '无法获取竞价数据',
                'success': False
            }), 404
            
    except Exception as e:
        logger.error(f'获取竞价数据时出错: {e}')
        return jsonify({
            'error': str(e),
            'success': False
        }), 500


@app.route('/api/update_today_data', methods=['POST'])
def update_today_data():
    """增量更新今日数据"""
    try:
        import sys
        import os
        # 添加data_processing目录到Python路径
        data_proc_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_processing')
        if data_proc_path not in sys.path:
            sys.path.insert(0, data_proc_path)
        
        from incremental_download import IncrementalDataDownloader
        
        # 创建下载器实例
        downloader = IncrementalDataDownloader()
        
        # 获取所有股票列表
        all_stocks = downloader.get_all_a_stocks()
        
        success_count = 0
        fail_count = 0
        
        # 只更新已有数据的股票（增量更新）
        for i, stock in enumerate(all_stocks):
            # 限制更新数量以避免超时
            if i >= 100:  # 只更新前100只股票作为示例
                break
                
            ak_code = stock['code'].replace('.XSHG', '').replace('.XSHE', '')
            csv_path = os.path.join(downloader.data_dir, "daily_data", f"{ak_code}.csv")
            
            if os.path.exists(csv_path):
                # 增量更新这只股票
                try:
                    # 使用更安全的方式调用更新函数
                    result = downloader.update_single_stock_data(stock, days=1)  # 只更新1天
                    if result[0]:  # 检查返回结果的第一个元素是否为True
                        success_count += 1
                    else:
                        fail_count += 1
                except TypeError as te:
                    # 如果遇到日期类型错误，尝试另一种方式
                    print(f"{stock['code']} 更新失败 (类型错误): {te}")
                    fail_count += 1
                except Exception as e:
                    print(f"{stock['code']} 更新失败: {e}")
                    fail_count += 1
        
        return jsonify({
            'status': 'success',
            'message': f'今日数据更新完成，成功: {success_count}, 失败: {fail_count}',
            'success_count': success_count,
            'fail_count': fail_count
        })
    except Exception as e:
        logger.error(f'更新今日数据失败: {e}')
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/generate_stock_pool', methods=['POST'])
def generate_stock_pool():
    """生成股票池"""
    try:
        import sys
        import os
        # 添加data_processing目录到Python路径
        data_proc_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_processing')
        if data_proc_path not in sys.path:
            sys.path.insert(0, data_proc_path)
        
        from stock_pool_generator import StockPoolGenerator
        
        data = request.json
        target_date = data.get('date', datetime.now().strftime('%Y-%m-%d'))
        
        generator = StockPoolGenerator()
        pool_data = generator.generate_stock_pool(target_date)
        
        return jsonify({
            'status': 'success',
            'message': '股票池生成成功',
            'pool_data': pool_data
        })
    except Exception as e:
        logger.error(f'生成股票池失败: {e}')
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/load_stock_pool', methods=['GET'])
def load_stock_pool():
    """加载股票池"""
    try:
        import sys
        import os
        # 添加data_processing目录到Python路径
        data_proc_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_processing')
        if data_proc_path not in sys.path:
            sys.path.insert(0, data_proc_path)
        
        from stock_pool_generator import StockPoolGenerator
        
        date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        
        generator = StockPoolGenerator()
        pool_data = generator.load_stock_pool(date_str)
        
        if not pool_data:
            return jsonify({
                'status': 'error',
                'message': '未找到指定日期的股票池数据'
            }), 404
        
        return jsonify({
            'status': 'success',
            'pool_data': pool_data
        })
    except Exception as e:
        logger.error(f'加载股票池失败: {e}')
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/repair_missing_data', methods=['POST'])
def repair_missing_data():
    """修复缺失的数据"""
    try:
        from repair_missing_data import DataRepairer
        
        data = request.json
        stock_code = data.get('stock_code')  # 可选：指定股票代码，如果不指定则修复所有股票
        
        repairer = DataRepairer()
        
        if stock_code:
            # 修复单个股票
            success = repairer.repair_stock_data(stock_code)
            if success:
                return jsonify({
                    'status': 'success',
                    'message': f'股票 {stock_code} 数据修复完成'
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': f'股票 {stock_code} 数据修复失败'
                })
        else:
            # 修复所有股票（异步执行）
            import threading
            def run_repair():
                repairer.repair_all_stocks()
            
            thread = threading.Thread(target=run_repair)
            thread.start()
            
            return jsonify({
                'status': 'success',
                'message': '开始修复所有股票数据，请查看服务器日志了解进度'
            })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/backtest', methods=['POST'])
def run_backtest():
    """执行回测"""
    try:
        params = request.json
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        strategy = params.get('strategy', 'mixed')
        initial_capital = float(params.get('initial_capital', 100000))
        
        if not start_date or not end_date:
            return jsonify({'error': '请提供开始日期和结束日期'}), 400
        
        result = backtest_strategy(start_date, end_date, strategy, initial_capital)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5007, threaded=True)
