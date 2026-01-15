#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
优化的TDX处理器，支持批量和并发处理
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import threading
import akshare as ak
import pandas as pd

# 已禁用pytdx库，只使用外部API接口
# 已禁用pytdx库，只使用外部API接口
PYTDX_AVAILABLE = False
# 移除pytdx库加载提示



class OptimizedTdxHandler:
    def __init__(self, max_connections=5):
        self.max_connections = max_connections
        self.connection_pool = []
        self.lock = threading.Lock()
        self.auction_data_cache = {}
    
    def get_connection(self):
        """获取一个TDX连接"""
        with self.lock:
            # 检查现有连接是否仍然有效
            for conn in self.connection_pool:
                try:
                    # 简单测试连接
                    conn.get_security_count(0)  # 测试连接是否有效
                    return conn
                except:
                    # 连接失效，移除它
                    try:
                        self.connection_pool.remove(conn)
                    except ValueError:
                        pass  # 连接可能已经被移除
            
            # 如果没有有效连接，创建新连接
            if len(self.connection_pool) < self.max_connections:
                connection = self._create_new_connection()
                if connection:
                    self.connection_pool.append(connection)
                    return connection
            else:
                # 如果达到最大连接数，返回第一个连接（即使可能失效）
                if self.connection_pool:
                    return self.connection_pool[0]
        
        return None
    
    def _create_new_connection(self):
        """创建新的TDX连接"""
        servers = [
            ('124.71.187.122', 7709),  # 上海(华为)
            ('122.51.120.217', 7709),  # 上海(腾讯)
            ('121.36.54.217', 7709),  # 北京(华为)
            ('124.71.85.110', 7709),   # 广州(华为)
            ('115.238.56.198', 7709),  # 已验证可用的服务器
            ('106.120.74.11', 7709),   # 可能的服务器
            ('119.147.92.222', 7709),  # 另一个可能的服务器
            ('113.105.95.25', 7709),   # 华泰证券服务器
            ('218.108.98.244', 7709),  # 电信服务器
            ('59.173.127.15', 7709),   # 广发证券服务器
        ]
        
        for ip, port in servers:
            try:
                api = TdxHq_API()
                if api.connect(ip, port, time_out=2):  # 2秒超时
                    print(f"成功连接到pytdx服务器 {ip}:{port}")
                    return api
            except Exception as e:
                print(f"连接到 {ip}:{port} 失败: {e}")
                continue
        
        print("无法连接到任何pytdx服务器")
        return None
    
    def close_all_connections(self):
        """关闭所有连接"""
        with self.lock:
            for conn in self.connection_pool:
                try:
                    conn.disconnect()
                except:
                    pass
            self.connection_pool.clear()
    
    def get_single_auction_data(self, stock_code, date_str):
        """
        获取单只股票的竞价数据（用于批量处理）
        """
        # 检查缓存
        cache_key = f"{stock_code}_{date_str}"
        if cache_key in self.auction_data_cache:
            return stock_code, self.auction_data_cache[cache_key]
        
        if not PYTDX_AVAILABLE:
            result = self.get_call_auction_data_fallback(stock_code, date_str)
            if result:
                self.auction_data_cache[cache_key] = result
            return stock_code, result
        
        try:
            # 从连接池获取连接
            api = self.get_connection()
            if api is None:
                result = self.get_call_auction_data_fallback(stock_code, date_str)
                if result:
                    self.auction_data_cache[cache_key] = result
                return stock_code, result
            
            # 根据股票代码确定市场
            if stock_code.startswith(('5', '6')):  # 上海市场
                market = 1
            else:  # 深圳市场
                market = 0
            
            # 将日期字符串转换为整数格式 (YYYYMMDD)
            date_int = int(date_str.replace("-", ""))
            
            # 获取历史逐笔交易数据
            # 竞价数据通常在9:25至9:30之间
            transactions = api.get_history_transaction_data(market, stock_code, 0, 200, date_int)
            
            if transactions:
                # 转换为DataFrame
                df = api.to_df(transactions)
                
                if not df.empty:
                    # 将time列转换为字符串
                    df['time'] = df['time'].astype(str)

                    # 筛选竞价时段的数据 (09:25:00 到 09:30:00)
                    auction_data = df[
                        (df['time'] >= '09:25:00') & (df['time'] <= '09:30:00')
                    ].copy()
                    
                    if not auction_data.empty:
                        # 按时间排序，获取最后一笔竞价数据
                        auction_data = auction_data.sort_values(by='time')
                        latest_auction = auction_data.iloc[-1]
                        
                        # 安全地提取数据，处理可能缺失的字段
                        try:
                            result = {
                                'time': f"{date_str} {latest_auction['time']}",  # 成交时间
                                'price': float(latest_auction.get('price', 0)),  # 成交价格
                                'volume': int(latest_auction.get('vol', latest_auction.get('volume', 0))),  # 成交量
                                'direction': latest_auction.get('direction', 'N/A'),  # 买卖方向
                                'order': latest_auction.get('order', 0)  # 委托单号
                            }
                        except (KeyError, ValueError, TypeError) as e:
                            print(f"解析竞价数据时出错: {e}, 数据: {latest_auction}")
                            # 如果解析失败，尝试获取开盘后数据
                            result = None
                        else:
                            # 缓存结果
                            self.auction_data_cache[cache_key] = result
                            return stock_code, result
                    
                    # 如果没有找到竞价时段的数据，尝试获取9:30之后的开盘几分钟数据（作为竞价后开盘价的近似）
                    opening_data = df[
                        (df['time'] >= '09:30:00') & (df['time'] <= '09:35:00')
                    ].copy()
                    
                    if not opening_data.empty:
                        opening_data = opening_data.sort_values(by='time')
                        first_opening = opening_data.iloc[0]  # 获取开盘后的第一笔交易
                        
                        try:
                            result = {
                                'time': f"{date_str} {first_opening['time']}",  # 成交时间
                                'price': float(first_opening.get('price', 0)),  # 成交价格
                                'volume': int(first_opening.get('vol', first_opening.get('volume', 0))),  # 成交量
                                'direction': first_opening.get('direction', 'N/A'),  # 买卖方向
                                'order': first_opening.get('order', 0)  # 委托单号
                            }
                        except (KeyError, ValueError, TypeError) as e:
                            print(f"解析开盘数据时出错: {e}, 数据: {first_opening}")
                            result = None
                        else:
                            # 缓存结果
                            self.auction_data_cache[cache_key] = result
                            return stock_code, result
            
            # 如果没有历史交易数据，尝试获取当日数据（如果是今天）
            today = datetime.now().strftime('%Y-%m-%d')
            if date_str == today:
                transactions_today = api.get_transaction_data(market, stock_code, 0, 200)
                if transactions_today:
                    df_today = api.to_df(transactions_today)
                    if not df_today.empty:
                        df_today['time'] = df_today['time'].astype(str)
                        
                        # 筛选竞价时段的数据
                        auction_data = df_today[
                            (df_today['time'] >= '09:25:00') & (df_today['time'] <= '09:30:00')
                        ].copy()
                        
                        if not auction_data.empty:
                            auction_data = auction_data.sort_values(by='time')
                            latest_auction = auction_data.iloc[-1]
                            
                            try:
                                result = {
                                    'time': f"{date_str} {latest_auction['time']}",  # 成交时间
                                    'price': float(latest_auction.get('price', 0)),  # 成交价格
                                    'volume': int(latest_auction.get('vol', latest_auction.get('volume', 0))),  # 成交量
                                    'direction': latest_auction.get('direction', 'N/A'),  # 买卖方向
                                    'order': latest_auction.get('order', 0)  # 委托单号
                                }
                            except (KeyError, ValueError, TypeError) as e:
                                print(f"解析当日竞价数据时出错: {e}, 数据: {latest_auction}")
                                result = None
                            else:
                                # 缓存结果
                                self.auction_data_cache[cache_key] = result
                                return stock_code, result
                                
                            # 如果没有竞价时段数据，尝试开盘后数据
                            opening_data = df_today[
                                (df_today['time'] >= '09:30:00') & (df_today['time'] <= '09:35:00')
                            ].copy()
                            
                            if not opening_data.empty:
                                opening_data = opening_data.sort_values(by='time')
                                first_opening = opening_data.iloc[0]
                                
                                try:
                                    result = {
                                        'time': f"{date_str} {first_opening['time']}",  # 成交时间
                                        'price': float(first_opening.get('price', 0)),  # 成交价格
                                        'volume': int(first_opening.get('vol', first_opening.get('volume', 0))),  # 成交量
                                        'direction': first_opening.get('direction', 'N/A'),  # 买卖方向
                                        'order': first_opening.get('order', 0)  # 委托单号
                                    }
                                except (KeyError, ValueError, TypeError) as e:
                                    print(f"解析当日开盘数据时出错: {e}, 数据: {first_opening}")
                                    result = None
                                else:
                                    # 缓存结果
                                    self.auction_data_cache[cache_key] = result
                                    return stock_code, result
                                    
        except Exception as e:
            print(f"获取股票 {stock_code} 竞价数据时出错: {e}")
            import traceback
            traceback.print_exc()
        
        # 如果TDX获取失败，使用fallback方法
        result = self.get_call_auction_data_fallback(stock_code, date_str)
        if result:
            self.auction_data_cache[cache_key] = result
        return stock_code, result

    def get_call_auction_data_fallback(self, stock_code, date_str):
        """
        获取指定股票和日期的开盘竞价数据（9:26之前的数据）- 回落方案
        模拟JoinQuant的get_call_auction()功能
        注意：这个方法仅在TDX完全不可用时使用
        """
        # 由于用户要求禁用akshare回退，这里直接返回None
        print(f"❌ 已禁用akshare数据源，无法获取股票 {stock_code} 在 {date_str} 的竞价数据")
        return None
        
        # 以下是被注释掉的原始akshare代码
        '''
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
                    self.auction_data_cache[cache_key] = result
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
        '''
    def get_call_auction_batch_concurrent(self, stock_codes, date_str, max_workers=5):
        """
        并发批量获取多只股票的开盘竞价数据
        """
        results = {}
        
        # 先处理已缓存的数据
        uncached_stocks = []
        for code in stock_codes:
            cache_key = f"{code}_{date_str}"
            if cache_key in self.auction_data_cache:
                results[code] = self.auction_data_cache[cache_key]
            else:
                uncached_stocks.append(code)
        
        if not uncached_stocks:
            return results
        
        # 并发处理未缓存的股票
        def fetch_single_stock(code):
            return self.get_single_auction_data(code, date_str)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_stock = {
                executor.submit(fetch_single_stock, code): code 
                for code in uncached_stocks
            }
            
            # 收集结果
            for future in as_completed(future_to_stock):
                stock_code, auction_data = future.result()
                results[stock_code] = auction_data
        
        return results

    def get_call_auction_data(self, stock_code, date_str):
        """
        获取指定股票和日期的开盘竞价数据
        """
        _, result = self.get_single_auction_data(stock_code, date_str)
        return result
    
    def get_cached_auction_data(self, stock_code, date_str):
        """
        从缓存获取竞价数据
        """
        cache_key = f"{stock_code}_{date_str}"
        return self.auction_data_cache.get(cache_key)
    
    def clear_cache(self):
        """
        清空竞价数据缓存
        """
        self.auction_data_cache.clear()


# 创建全局实例
tdx_handler = OptimizedTdxHandler(max_connections=5)


# 便捷函数接口
def get_call_auction_data(stock_code, date_str):
    """获取单只股票的竞价数据"""
    return tdx_handler.get_call_auction_data(stock_code, date_str)

def get_call_auction_batch_concurrent(stock_codes, date_str, max_workers=5):
    """并发批量获取多只股票的竞价数据"""
    return tdx_handler.get_call_auction_batch_concurrent(stock_codes, date_str, max_workers)

def clear_cache():
    """清空缓存"""
    tdx_handler.clear_cache()

def close_connections():
    """关闭所有连接"""
    tdx_handler.close_all_connections()


def test_performance():
    """
    测试批量并发处理性能
    """
    print("=" * 80)
    print("批量和并发处理性能测试")
    print("=" * 80)
    
    # 测试股票列表
    test_stocks = ['000001', '000002', '600000', '600036', '000063']
    
    # 找到最近的工作日
    test_date = datetime.now()
    while test_date.weekday() >= 5:  # 5是周六，6是周日
        test_date -= timedelta(days=1)
    date_str = test_date.strftime('%Y-%m-%d')
    
    print(f"测试日期: {date_str}")
    print(f"测试股票: {test_stocks}")
    
    # 清空缓存
    clear_cache()
    
    # 测试顺序获取
    print(f"\n顺序获取 {len(test_stocks)} 只股票的竞价数据:")
    start_time = time.time()
    seq_results = {}
    for stock in test_stocks:
        seq_results[stock] = get_call_auction_data(stock, date_str)
    seq_time = time.time() - start_time
    
    print(f"   顺序获取耗时: {seq_time:.4f}秒")
    print(f"   成功获取: {sum(1 for v in seq_results.values() if v is not None)}/{len(test_stocks)} 只股票")
    
    # 清空缓存
    clear_cache()
    
    # 测试批量并发获取
    print(f"\n批量并发获取 {len(test_stocks)} 只股票的竞价数据:")
    start_time = time.time()
    batch_results = get_call_auction_batch_concurrent(test_stocks, date_str, max_workers=3)
    batch_time = time.time() - start_time
    
    print(f"   批量并发耗时: {batch_time:.4f}秒")
    print(f"   成功获取: {sum(1 for v in batch_results.values() if v is not None)}/{len(test_stocks)} 只股票")
    
    # 显示结果
    for stock, data in batch_results.items():
        if data:
            print(f"   {stock}: 价格={data['price']}, 时间={data['time']}")
        else:
            print(f"   {stock}: 无数据")
    
    if seq_time > 0:
        speedup = seq_time / batch_time if batch_time > 0 else float('inf')
        print(f"\n   性能提升: {speedup:.2f}倍")
    
    # 关闭连接
    close_connections()


if __name__ == "__main__":
    test_performance()