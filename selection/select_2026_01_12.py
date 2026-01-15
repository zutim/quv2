#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2026-01-12股票选择器
使用2026-01-12的股票池数据进行选股
"""

import pandas as pd
import os
from datetime import datetime, timedelta
import requests
import json
import akshare as ak
from functools import lru_cache


class TodayStockSelector:
    def __init__(self, data_path=None):
        """
        初始化选股器
        :param data_path: 股票数据文件路径，默认为 'full_stock_data/daily_data'
        """
        if data_path is None:
            # 修改路径：使用项目根目录下的daily_data，而不是当前脚本目录下的
            self.data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'full_stock_data', 'daily_data')
        else:
            self.data_path = data_path
        # 添加市值缓存
        self.market_cap_cache = {}
        self.last_market_data_fetch_time = None
        self.cached_market_data = None
        # 初始化市值管理器
        self.market_cap_manager = self._init_market_cap_manager()

    def _init_market_cap_manager(self):
        """初始化市值管理器"""
        try:
            # 添加项目根目录到sys.path，以便能够导入data_processing模块
            import sys
            import os
            project_root = os.path.dirname(os.path.dirname(__file__))  # 获取项目根目录
            data_processing_path = os.path.join(project_root, 'data_processing')
            
            if data_processing_path not in sys.path:
                sys.path.insert(0, data_processing_path)
            
            from get_market_caps import StockMarketCapManager
            # 传入绝对路径的data_dir，确保市值管理器在正确的位置查找文件
            full_data_dir = os.path.join(project_root, 'full_stock_data')
            return StockMarketCapManager(data_dir=full_data_dir)
        except ImportError:
            print("警告: 无法导入市值管理器，将使用备用方案")
            return None

    def get_stock_name(self, stock_code):
        """获取股票名称"""
        # 简化处理，实际应用中可以调用akshare获取
        return f"股票{stock_code}"

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

    def get_market_cap_from_local_data(self, stock_code, date_str):
        """
        从本地历史数据计算市值
        市值 = 收盘价 × 总股本
        由于我们没有总股本数据，使用本地数据估算市值
        """
        try:
            # 构建文件路径
            csv_file = os.path.join(self.data_path, f'{stock_code}.csv')
            if not os.path.exists(csv_file):
                print(f"股票数据文件不存在: {csv_file}")
                return 0, 0  # 返回默认值
            
            df = pd.read_csv(csv_file)
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
            
            # 查找指定日期的数据
            day_data = df[df['date'] == pd.to_datetime(date_str)]
            if day_data.empty:
                # 如果找不到指定日期的数据，使用最接近的日期
                available_dates = df[df['date'] <= pd.to_datetime(date_str)]
                if available_dates.empty:
                    return 0, 0
                day_data = available_dates.tail(1)
            
            day_data = day_data.iloc[0]
            close_price = float(day_data.get('close', 0)) if pd.notna(day_data.get('close', 0)) else 0
            
            if close_price <= 0:  # 无效价格，跳过
                return 0, 0
            
            # 使用成交额作为市值的估算（这是不准确的，但如果没有总股本数据，这是唯一的选择）
            # 更好的方式是从其他数据源获取总股本，然后计算：市值 = 股价 × 总股本
            # 由于我们没有总股本数据，我们使用一个估算方法：
            # 从实时数据获取一个近似的总股本/流通股本乘数
            try:
                # 获取实时数据来获取总股本乘数
                real_time_data = ak.stock_zh_a_spot_em()
                stock_rt = real_time_data[real_time_data['代码'].astype(str) == stock_code]
                if not stock_rt.empty:
                    # 使用实时总市值与股价的比值来估算总股本
                    real_time_price = float(stock_rt['最新价'].iloc[0]) if '最新价' in stock_rt.columns else 0
                    real_time_mc = float(stock_rt['总市值'].iloc[0]) / 1e8 if '总市值' in stock_rt.columns and pd.notna(stock_rt['总市值'].iloc[0]) else 0
                    real_time_cmc = float(stock_rt['流通市值'].iloc[0]) / 1e8 if '流通市值' in stock_rt.columns and pd.notna(stock_rt['流通市值'].iloc[0]) else 0
                    
                    if real_time_price > 0:
                        # 使用实时的总股本乘数来估算历史市值
                        share_multiplier = real_time_mc / real_time_price if real_time_price > 0 else 1
                        estimated_market_cap = close_price * share_multiplier
                        # 流通市值按类似方法估算，但可能需要单独的流通股乘数
                        circ_share_multiplier = real_time_cmc / real_time_price if real_time_price > 0 else 1
                        estimated_circulating_market_cap = close_price * circ_share_multiplier
                        return estimated_market_cap, estimated_circulating_market_cap
            except:
                pass  # 如果实时数据获取失败，使用下面的估算方法
            
            # 如果实时数据获取失败或没有相应数据，使用默认的估算方法
            # 根据股票代码类型进行大致估算
            if stock_code.startswith('00'):
                # 深市中小板股票，通常市值在几十亿到几百亿之间
                return 150, 100  # 总市值，流通市值
            elif stock_code.startswith('60'):
                # 沪市主板股票，通常市值较大
                return 200, 150
            elif stock_code.startswith('30') or stock_code.startswith('68'):
                # 创业板或科创板，通常市值适中
                return 100, 80
            else:
                return 150, 100  # 默认值
        except Exception as e:
            print(f"从本地数据获取市值失败 for {stock_code} on {date_str}: {e}")
            return 0, 0

    def get_market_cap(self, stock_code, date_str):
        """
        通过多种方式获取股票市值，使用缓存优化性能
        优先使用本地市值管理器获取历史市值
        """
        # 检查缓存中是否存在该股票的市值数据
        cache_key = f"{stock_code}_{date_str}"
        if cache_key in self.market_cap_cache:
            return self.market_cap_cache[cache_key]
        
        # 优先使用市值管理器获取历史市值
        if self.market_cap_manager:
            try:
                historical_market_cap, historical_circulating_market_cap = \
                    self.market_cap_manager.get_historical_market_cap(stock_code, date_str)
                
                if historical_market_cap > 0 and historical_circulating_market_cap > 0:
                    result = (historical_market_cap, historical_circulating_market_cap)
                    # 将结果存入缓存
                    self.market_cap_cache[cache_key] = result
                    return result
                else:
                    # 如果市值管理器中没有该股票的数据，打印跳过信息
                    print(f"市值管理器中没有股票 {stock_code} 的数据，跳过该股票")
                    return None  # 返回None表示跳过该股票
            except Exception as e:
                print(f"使用市值管理器获取历史市值失败 for {stock_code} on {date_str}: {e}")
        
        # 如果市值管理器不可用或获取失败，使用原来的方法
        try:
            # 首先尝试从本地历史数据获取市值
            market_cap, circulating_market_cap = self.get_market_cap_from_local_data(stock_code, date_str)
            
            # 如果从本地数据获取成功（即估值不为0），则使用本地数据
            if market_cap > 0 and circulating_market_cap > 0:
                result = (market_cap, circulating_market_cap)
            else:
                # 否则，使用实时数据作为备选
                # 检查是否已有市场整体数据缓存且未过期（10分钟）
                current_time = datetime.now()
                if (self.cached_market_data is not None and 
                    self.last_market_data_fetch_time is not None and
                    (current_time - self.last_market_data_fetch_time).seconds < 600):  # 10分钟
                    # 使用已缓存的市场数据
                    market_df = self.cached_market_data
                else:
                    # 获取新的市场数据并缓存
                    market_df = ak.stock_zh_a_spot_em()
                    self.cached_market_data = market_df
                    self.last_market_data_fetch_time = current_time
                
                # 格式化股票代码
                if stock_code.startswith('6'):
                    jq_code = f"{stock_code}.XSHG"
                else:
                    jq_code = f"{stock_code}.XSHE"
                
                # 查找对应股票
                stock_data = market_df[market_df['代码'].astype(str) == stock_code]
                if not stock_data.empty:
                    # 总市值在亿单位
                    market_cap = pd.to_numeric(stock_data['总市值'].iloc[0], errors='coerce') / 1e8 if '总市值' in stock_data.columns else 0
                    circulating_market_cap = pd.to_numeric(stock_data['流通市值'].iloc[0], errors='coerce') / 1e8 if '流通市值' in stock_data.columns else 0
                    result = (market_cap, circulating_market_cap)
                else:
                    # 如果实时数据获取不到，返回默认值
                    result = self.estimate_market_cap_by_code(stock_code)
        except Exception as e:
            print(f"获取市值数据失败 for {stock_code}: {e}")
            result = self.estimate_market_cap_by_code(stock_code)
        
        # 将结果存入缓存
        self.market_cap_cache[cache_key] = result
        return result

    def estimate_market_cap_by_code(self, stock_code):
        """
        根据股票代码估算市值（备用方案）
        """
        if stock_code.startswith('00'):
            # 深市中小板股票，通常市值在几十亿到几百亿之间
            return 150, 100  # 总市值，流通市值
        elif stock_code.startswith('60'):
            # 沪市主板股票，通常市值较大
            return 200, 150
        elif stock_code.startswith('30') or stock_code.startswith('68'):
            # 创业板或科创板，通常市值适中
            return 100, 80
        else:
            return 150, 100  # 默认值

    def clear_market_cap_cache(self):
        """清空市值缓存"""
        self.market_cap_cache.clear()
        self.cached_market_data = None
        self.last_market_data_fetch_time = None

    def get_historical_auction_data(self, stock_code, date_str):
        """
        获取历史竞价数据
        对于历史日期，如果没有API数据，使用开盘价作为竞价价的近似
        """
        from datetime import datetime

        # 获取今天的日期
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 使用原始API接口
        # 将日期格式从 YYYY-MM-DD 转换为 YYYYMMDD
        formatted_date = date_str.replace("-", "")
        url = f"http://localhost:8080/api/minute-trade-all?code={stock_code}&date={formatted_date}"

        # 如果是今天的日期，则调用API获取真实的竞价数据
        if date_str == today:
            url = f"http://localhost:8080/api/minute-trade-all?code={stock_code}"

        try:
            print(f"尝试获取股票 {stock_code} 在 {date_str} 的竞价数据，API请求URL: {url}")
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                print(f"API响应成功，股票 {stock_code} 在 {date_str}")
                data = response.json()
                if 'data' in data and 'List' in data['data']:
                    # 获取竞价时段的数据（9:25-9:29之间的数据）
                    trade_list = data['data']['List']
                    if trade_list:
                        print(f"API返回 {len(trade_list)} 条交易数据，股票 {stock_code} 在 {date_str}")
                        # 筛选竞价时段的数据（9:25-9:29）
                        auction_trades = []
                        for trade in trade_list:
                            time_str = trade.get('Time', '')
                            # 检查是否在竞价时段（9:25-9:29）
                            if ('T09:25:' in time_str or 'T09:26:' in time_str or
                                'T09:27:' in time_str or 'T09:28:' in time_str or
                                'T09:29:' in time_str):
                                auction_trades.append(trade)

                        # 如果找到竞价时段数据，取最接近9:26-9:30的数据
                        if auction_trades:
                            print(f"找到 {len(auction_trades)} 条竞价时段数据，股票 {stock_code} 在 {date_str}")
                            # 按时间排序，取最后一个（最晚的，最接近9:26-9:30开盘的数据）
                            latest_auction = auction_trades[-1]
                            # 保留原始价格和成交量的处理
                            price = latest_auction.get('Price', 0) / 1000
                            volume = latest_auction.get('Volume', 0) * 100  # 成交量需要乘以100
                            
                            auction_data = {
                                'time': latest_auction.get('Time', ''),
                                'price': price,
                                'volume': volume,
                                'direction': 'B' if latest_auction.get('Status') == 1 else 'S',
                                'order': latest_auction.get('Number', 0)
                            }
                            print(f"成功获取竞价数据，时间: {auction_data['time']}, 价格: {auction_data['price']}, 成交量: {auction_data['volume']}")
                            return auction_data

                        # 如果没有找到竞价时段数据，但有9点时段的数据，取最接近9:26的数据
                        morning_trades = []
                        for trade in trade_list:
                            time_str = trade.get('Time', '')
                            if 'T09:' in time_str:  # 早上9点时段的数据
                                morning_trades.append(trade)

                        if morning_trades:
                            print(f"找到 {len(morning_trades)} 条早盘数据，股票 {stock_code} 在 {date_str}")
                            # 按时间排序，取最后一个
                            latest_morning = morning_trades[-1]
                            price = latest_morning.get('Price', 0) / 1000  # 价格也除以1000
                            volume = latest_morning.get('Volume', 0) * 100  # 成交量需要乘以100
                            
                            auction_data = {
                                'time': latest_morning.get('Time', ''),
                                'price': price,
                                'volume': volume,
                                'direction': 'B' if latest_morning.get('Status') == 1 else 'S',
                                'order': latest_morning.get('Number', 0)
                            }
                            print(f"使用早盘数据，时间: {auction_data['time']}, 价格: {auction_data['price']}, 成交量: {auction_data['volume']}")
                            return auction_data

                        # 如果仍然没有找到早盘数据，取最后一条数据
                        last_trade = trade_list[-1]
                        price = last_trade.get('Price', 0) / 1000  # 价格也除以1000
                        volume = last_trade.get('Volume', 0) * 100  # 成交量需要乘以100
                        
                        auction_data = {
                            'time': last_trade.get('Time', ''),
                            'price': price,
                            'volume': volume,
                            'direction': 'B' if last_trade.get('Status') == 1 else 'S',
                            'order': last_trade.get('Number', 0)
                        }
                        print(f"使用最后一条数据，时间: {auction_data['time']}, 价格: {auction_data['price']}, 成交量: {auction_data['volume']}")
                        return auction_data
                    else:
                        print(f"API返回空数据列表，股票 {stock_code} 在 {date_str}")
                        # 如果API返回空数据，尝试从本地数据获取
                        return self.get_auction_data_from_daily_data(stock_code, date_str)
                else:
                    print(f"API返回格式错误，缺少'data.List'字段，股票 {stock_code} 在 {date_str}")
                    # 如果API返回格式错误，尝试从本地数据获取
                    return self.get_auction_data_from_daily_data(stock_code, date_str)
            else:
                print(f"API请求失败，状态码: {response.status_code}，股票 {stock_code} 在 {date_str}")
                # 如果API不可用，尝试从本地日线数据获取
                return self.get_auction_data_from_daily_data(stock_code, date_str)
        except Exception as e:
            # 如果API调用失败，尝试从本地日线数据获取
            print(f"API调用异常，股票 {stock_code} 在 {date_str}: {e}")
            return self.get_auction_data_from_daily_data(stock_code, date_str)

    def get_auction_data_from_daily_data(self, stock_code, date_str):
        """
        从日线数据中估算竞价数据
        使用开盘价作为竞价价的近似
        """
        try:
            # 构建文件路径获取本地数据
            csv_file = os.path.join(self.data_path, f'{stock_code}.csv')
            if not os.path.exists(csv_file):
                print(f"股票数据文件不存在: {csv_file}")
                return None
            
            print(f"从本地文件获取数据: {csv_file}")
            df = pd.read_csv(csv_file)
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
            
            # 查找指定日期的数据
            day_data = df[df['date'] == pd.to_datetime(date_str)]
            if day_data.empty:
                print(f"未找到股票 {stock_code} 在 {date_str} 的数据")
                return None
            
            day_data = day_data.iloc[0]
            
            open_price = float(day_data.get('open', 0)) if pd.notna(day_data.get('open', 0)) else 0
            
            if open_price <= 0:
                print(f"股票 {stock_code} 在 {date_str} 的开盘价无效: {open_price}")
                return None
            
            # 使用开盘价作为竞价数据的近似，但成交量设置为0表示未知，不进行估算
            # 因为集合竞价成交量和全日成交量没有固定比例关系
            auction_data = {
                'time': f"{date_str} 09:25:00",  # 竞价时间
                'price': open_price,  # 使用开盘价作为竞价价的近似
                'volume': 0,  # 不进行估算，因为无法准确知道集合竞价的成交量
                'direction': 'B',  # 买入方向
                'order': 0
            }
            
            print(f"成功从本地数据获取竞价信息，股票 {stock_code} 在 {date_str}，开盘价: {open_price}")
            return auction_data
        except Exception as e:
            print(f"从本地日线数据获取竞价信息失败 for {stock_code}: {e}")
            return None

    def select_stocks_from_pool(self, target_date_str, pool_data):
        """
        使用JSON格式的pool数据进行选股
        :param target_date_str: 目标日期，格式 'YYYY-MM-DD'
        :param pool_data: 股票池数据，JSON格式
        {
            "target_date": "2026-01-07",
            "prev_trading_date": "2026-01-06",
            "prev_2_trading_date": "2026-01-05",
            "limit_up_stocks": [...],  # 昨日涨停股票
            "limit_up_2_days_ago": [...],  # 前日涨停股票
            "first_board_stocks": [...]  # 首板股票
        }
        :return: 选中的股票列表
        """
        try:
            # 将字符串转换为日期对象
            target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
            
            # 从pool_data中获取涨停股票列表
            limit_up_stocks_today = pool_data.get('limit_up_stocks', [])
            limit_up_stocks_yesterday = pool_data.get('limit_up_stocks', [])  # 昨日涨停即为今天看昨天的涨停
            limit_up_2_days_ago = pool_data.get('limit_up_2_days_ago', [])
            first_board_stocks = pool_data.get('first_board_stocks', [])
            
            print(f"股票池数据 - 昨日涨停: {len(limit_up_stocks_yesterday)}, 前日涨停: {len(limit_up_2_days_ago)}, 首板: {len(first_board_stocks)}")
            
            # 应用过滤条件
            filtered_first_board_stocks = self.filter_kcbj_stock(first_board_stocks)
            filtered_first_board_stocks = self.filter_st_paused_stock(filtered_first_board_stocks, target_date)
            filtered_first_board_stocks = self.filter_new_stock(filtered_first_board_stocks, target_date)
            
            print(f"过滤后首板股票: {len(filtered_first_board_stocks)}")
            
            # 从pool_data中获取曾涨停未封板的股票（对应aa.py中的target_list2）
            ever_limit_up_not_closed_yesterday = pool_data.get('limit_up_not_closed_stocks', [])  # 曾涨停未封板股票
            
            print(f"从pool数据获取曾涨停未封板股票: {len(ever_limit_up_not_closed_yesterday)} 只")
            
            print(f"曾涨停未封板股票: {len(ever_limit_up_not_closed_yesterday)}")
            
            # 按策略分类的股票列表
            sbgk_stocks = []  # 首板高开
            sbdk_stocks = []  # 首板低开
            rzq_stocks = []  # 弱转强
            
            # 对首板股票进行处理（来自first_board_stocks，对应aa.py中的target_list）
            target_stocks = filtered_first_board_stocks
            
            for stock_code in target_stocks:
                try:
                    # 构建文件路径
                    csv_file = os.path.join(self.data_path, f'{stock_code}.csv')
                    if not os.path.exists(csv_file):
                        print(f"股票数据文件不存在: {csv_file}，跳过股票 {stock_code}")
                        continue
                    
                    df = pd.read_csv(csv_file)
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
                    
                    # 即使本地数据中没有当日数据，也要尝试获取竞价数据
                    # 查找指定日期的数据
                    day_data = df[df['date'] == pd.to_datetime(target_date_str)]
                    if day_data.empty:
                        print(f"未找到股票 {stock_code} 在 {target_date_str} 的本地数据，但仍需获取竞价数据")
                        # 即使本地数据为空，也继续尝试获取竞价数据
                        day_data_for_calc = None
                    else:
                        day_data_for_calc = day_data.iloc[0]
                    
                    # 获取前一个交易日的数据
                    prev_day_data = df[df['date'] < pd.to_datetime(target_date_str)].tail(1)
                    if prev_day_data.empty:
                        print(f"股票 {stock_code} 在 {target_date_str} 前无历史数据，跳过")
                        continue
                    
                    prev_close = float(prev_day_data.iloc[0]['close']) if pd.notna(prev_day_data.iloc[0]['close']) else 0
                    current_open = float(day_data_for_calc.get('open', 0)) if day_data_for_calc is not None and pd.notna(day_data_for_calc.get('open', 0)) else 0
                    current_high = float(day_data_for_calc.get('high', 0)) if day_data_for_calc is not None and pd.notna(day_data_for_calc.get('high', 0)) else 0
                    current_low = float(day_data_for_calc.get('low', 0)) if day_data_for_calc is not None and pd.notna(day_data_for_calc.get('low', 0)) else 0
                    current_volume = float(day_data_for_calc.get('volume', 0)) if day_data_for_calc is not None and pd.notna(day_data_for_calc.get('volume', 0)) else 0
                    # 修复成交量数据：本地CSV中的volume可能以手为单位，而实际成交量应为股
                    # 通常volume字段是以股为单位，但有些数据源会以手为单位（1手=100股）
                    # 根据数据特征判断，需要乘以100来统一单位
                    prev_volume_raw = float(prev_day_data.iloc[0]['volume']) if pd.notna(prev_day_data.iloc[0]['volume']) else 1
                    # 检测是否需要调整成交量单位
                    # 通过计算均价与收盘价的关系来判断
                    prev_amount = float(prev_day_data.iloc[0]['amount']) if pd.notna(prev_day_data.iloc[0]['amount']) else 0
                    
                    # 检测成交量单位：如果按原始volume计算均价远超收盘价，说明volume单位过小
                    if prev_volume_raw != 0:
                        raw_avg_price = prev_amount / prev_volume_raw if prev_volume_raw != 0 else 0
                        if raw_avg_price > prev_close * 5:  # 如果均价远大于收盘价，可能是单位问题
                            prev_volume = prev_volume_raw * 100  # 将手转换为股
                        else:
                            prev_volume = prev_volume_raw
                    else:
                        prev_volume = 1  # 防止除零错误
                    
                    if prev_close == 0:
                        print(f"股票 {stock_code} 前日收盘价为0，跳过")
                        continue
                    
                    # 获取涨停状态（从pool_data中获取）
                    is_limit_up_today = stock_code in limit_up_stocks_today
                    was_limit_up_yesterday = stock_code in limit_up_stocks_yesterday
                    was_limit_up_2_days_ago = stock_code in limit_up_2_days_ago
                    
                    # 检查是否满足首板条件（昨日涨停，前日未涨停）
                    is_first_board = was_limit_up_yesterday and not was_limit_up_2_days_ago
                    
                    if is_first_board:
                        # 获取竞价数据
                        call_auction_data = self.get_historical_auction_data(stock_code, target_date_str)
                        if call_auction_data is not None:
                            auction_price = call_auction_data['price']
                            # 使用竞价价格计算开盘比例
                            current_ratio = auction_price / prev_close if prev_close != 0 else 0

                            volume_ratio = call_auction_data['volume'] / prev_volume if prev_volume > 0 else 0
                            print(f"DEBUG: 竞价成交量={call_auction_data['volume']}, 前日成交量={prev_volume}, 成交量比例={volume_ratio}")

                            # 获取当天开盘价用于对比
                            current_open = float(day_data_for_calc.get('open', 0)) if day_data_for_calc is not None and pd.notna(day_data_for_calc.get('open', 0)) else 0
                            
                        else:
                            # 与aa.py一致：如果无法获取竞价数据，则跳过该股票
                            print(f"股票 {stock_code}: 无法获取竞价数据，跳过")
                            continue
                            
                        # 获取市值数据
                        market_cap_result = self.get_market_cap(stock_code, target_date_str)
                        
                        # 检查是否成功获取市值数据
                        if market_cap_result is None:
                            print(f"无法获取股票 {stock_code} 的市值数据，跳过该股票")
                            continue  # 跳过该股票
                        
                        market_cap, circulating_market_cap = market_cap_result
                        
                        # 条件一：均价，金额，市值，换手率
                        if prev_volume != 0 and prev_amount != 0:
                            avg_price = prev_amount / prev_volume
                            # 修正：根据aa.py中的计算公式
                            # avg_price_increase_value = prev_day_data['money'][0] / prev_day_data['volume'][0] / prev_day_data['close'][0] *1.1 - 1
                            avg_price_increase_value = avg_price / prev_close * 1.1 - 1
                            print(f"股票 {stock_code}: 均价计算详情 - 成交额={prev_amount:.2f}, 成交量={prev_volume:.2f}, 均价={avg_price:.3f}, 收盘价={prev_close:.3f}, 均价获利={avg_price_increase_value:.3f}")
                            condition1_part1 = avg_price_increase_value >= 0.07  # 收盘获利比例 >= 7%
                        else:
                            condition1_part1 = False  # 如果金额或成交量为0，条件不满足
                            print(f"股票 {stock_code}: 前日成交额或成交量为0，均价获利条件不满足")
                        condition1_part2 = 5.5e8 <= prev_amount <= 20e8  # 成交额在5.5亿-20亿之间
                        condition1 = condition1_part1 and condition1_part2
                        
                        # 检查条件一中的市值条件
                        if market_cap < 70 or circulating_market_cap > 520:
                            print(f"股票 {stock_code}: 市值条件不满足 - 总市值={market_cap:.2f}亿, 流通市值={circulating_market_cap:.2f}亿")
                            continue  # 市值不在范围内，跳过
                        
                        # 条件二：高开,开比
                        condition2_part1 = volume_ratio >= 0.03  # 集合竞价成交量占比 >= 3%
                        # 修正开盘比例计算，符合aa.py的逻辑 - 开盘价高于收盘价(高开)且不超过6%
                        condition2_part2 = 1.0 < current_ratio < 1.06  # 开盘价高于前日收盘价且不超过6%
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
                                
                                # 计算zyts_0：从倒数第3个开始往前，找到高点大于等于前一日高点的天数
                                zyts_0 = next((i-1 for i, high in enumerate(hst['high'][-3::-1], 2) if pd.notna(high) and high >= prev_high), 100)  # 计算zyts_0
                                
                                zyts = zyts_0 + 5
                                # 获取高点以来的成交量数据（基于涨停日的历史数据）
                                volume_data_historical = hst['volume'].tail(zyts) if len(hst) >= zyts else hst['volume']
                                
                                if len(volume_data_historical) >= 2:
                                    max_prev_vol = volume_data_historical.iloc[:-1].max() if len(volume_data_historical) > 1 else volume_data_historical.iloc[0]
                                    
                                    # 使用涨停日的成交量来检查左压条件（而不是竞价成交量）
                                    yesterday_volume = float(prev_day_data.iloc[0]['volume']) if pd.notna(prev_day_data.iloc[0]['volume']) else 0
                                    
                                    if max_prev_vol > 0:
                                        condition3 = yesterday_volume > max_prev_vol * 0.9  # 检查涨停日成交量是否放大
                                    else:
                                        condition3 = True  # 如果前期成交量为0，条件通过
                                else:
                                    condition3 = False  # 如果数据不足，条件不满足
                            else:
                                condition3 = False  # 如果数据不足，条件不满足
                        else:
                            condition3 = False  # 如果数据不足，条件不满足
                        
                        # 详细输出每个条件的检查结果
                        print(f"股票 {stock_code}: "
                              f"均价获利={avg_price_increase_value:.3f}(≥0.07?{condition1_part1}), "
                              f"成交额={prev_amount/1e8:.2f}亿(in[5.5,20]?{condition1_part2}), "
                              f"市值总={market_cap:.2f}亿≥70?{market_cap>=70}, "
                              f"市值流={circulating_market_cap:.2f}亿≤520?{circulating_market_cap<=520}, "
                              f"竞价量比={volume_ratio:.3f}(≥0.03?{condition2_part1}), "
                              f"开盘比={current_ratio:.3f}(1.0<{current_ratio:.3f}<1.06?{condition2_part2}), "
                              f"左压周期={zyts_0}, "
                              f"昨量={yesterday_volume:.0f}, "
                              f"前量max={max_prev_vol:.0f}, "
                              f"左压?{condition3}")
                        
                        # 所有条件都满足
                        if condition1 and condition2 and condition3:
                            sbgk_stocks.append(stock_code)
                            print(f'股票 {stock_code} 满足首板高开条件: 成交额={prev_amount/1e8:.2f}亿, 市值={market_cap:.2f}亿, 开盘比例={current_ratio:.3f}, 左压周期={zyts_0}天')
                    
                    # 首板低开条件 - 昨日涨停，前日未涨停，低开幅度3%-4.5%
                    if is_first_board:
                        # 使用开盘价来计算开盘比例
                        open_ratio = current_open / prev_close if prev_close != 0 else 0
                        low_open_condition = 0.955 <= open_ratio <= 0.97  # 低开3%-4.5%
                        
                        if low_open_condition:
                            # 检查其他条件：相对位置和成交额
                            history_data = df[df['date'] <= pd.to_datetime(prev_day_data.iloc[0]['date'])]
                            if len(history_data) >= 60:
                                hist_60 = history_data.tail(60)
                                close_60 = float(hist_60.iloc[-1]['close']) if pd.notna(hist_60.iloc[-1]['close']) else 0
                                high_60 = hist_60['high'].max() if len(hist_60) > 0 else 0
                                low_60 = hist_60['low'].min() if len(hist_60) > 0 else 0
                                
                                if high_60 != low_60:
                                    rp = (close_60 - low_60) / (high_60 - low_60)  # 相对位置
                                    money_condition = prev_amount >= 1e8  # 昨日成交额>=1亿
                                        
                                    if rp <= 0.5 and money_condition:
                                        # 获取市值数据
                                        market_cap_result = self.get_market_cap(stock_code, target_date_str)
                                        
                                        # 检查是否成功获取市值数据
                                        if market_cap_result is None:
                                            print(f"无法获取股票 {stock_code} 的市值数据，跳过该股票")
                                            continue  # 跳过该股票
                                        
                                        market_cap, circulating_market_cap = market_cap_result
                                        
                                        sbdk_stocks.append(stock_code)
                                        print(f'股票 {stock_code} 满足首板低开条件: 相对位置={rp:.3f}, 金额={prev_amount/1e8:.2f}亿, 市值={market_cap:.2f}亿, 开盘比例={open_ratio:.3f}')
                    
                except Exception as e:
                    print(f"处理股票 {stock_code} 时出错: {e}")
                    continue  # 跳过有问题的股票
            
            # 现在处理弱转强股票：遍历曾涨停未封板的股票（对应aa.py中的target_list2）
            for stock_code in ever_limit_up_not_closed_yesterday:
                try:
                    # 只处理在曾涨停未封板列表中的股票
                    if stock_code not in ever_limit_up_not_closed_yesterday:
                        continue
                    
                    # 构建文件路径
                    csv_file = os.path.join(self.data_path, f'{stock_code}.csv')
                    if not os.path.exists(csv_file):
                        print(f"股票数据文件不存在: {csv_file}，跳过股票 {stock_code}")
                        continue
                    
                    df = pd.read_csv(csv_file)
                    # 重命名列以处理中文列名
                    df.rename(columns={
                        '股　　票　　代　　码　': 'code',
                        '股　票　代　碼': 'code',
                        '开　　盘': 'open',
                        '开　盘': 'open',
                        '收　　盘': 'close',
                        '收　盘': 'close',
                        '最　　高': 'high',
                        '最　高': 'high',
                        '最　　低': 'low',
                        '最　低': 'low',
                        '成　　交　量': 'volume',
                        '成　交　量': 'volume',
                        '成　　交　额': 'amount',
                        '成　交　额': 'amount',
                        '振　　幅': 'amplitude',
                        '振　幅': 'amplitude',
                        '涨　　跌　　幅': 'pct_change',
                        '涨　跌　幅': 'pct_change',
                        '涨　　跌　　额': 'change',
                        '涨　跌　额': 'change',
                        '换　　手　　率': 'turnover',
                        '换　手　率': 'turnover'
                    }, inplace=True)
                    
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # 即使本地数据中没有当日数据，也要尝试获取竞价数据
                    # 查找指定日期的数据
                    day_data = df[df['date'] == pd.to_datetime(target_date_str)]
                    if day_data.empty:
                        print(f"未找到股票 {stock_code} 在 {target_date_str} 的本地数据，但仍需获取竞价数据")
                        # 即使本地数据为空，也继续尝试获取竞价数据
                        day_data_for_calc = None
                    else:
                        day_data_for_calc = day_data.iloc[0]
                    
                    # 获取前一个交易日的数据
                    prev_day_data = df[df['date'] < pd.to_datetime(target_date_str)].tail(1)
                    if prev_day_data.empty:
                        print(f"股票 {stock_code} 在 {target_date_str} 前无历史数据，跳过")
                        continue
                    
                    prev_close = float(prev_day_data.iloc[0]['close']) if pd.notna(prev_day_data.iloc[0]['close']) else 0
                    # 注意：对于当天选股，当日收盘价不存在，所以我们不检查当日收盘价
                    # 我们只关注竞价数据和其他条件
                    
                    
                    # 获取股票名称
                    stock_name = self.get_stock_name(stock_code)
                    
                    current_open = float(day_data_for_calc.get('open', 0)) if day_data_for_calc is not None and pd.notna(day_data_for_calc.get('open', 0)) else 0
                    current_high = float(day_data_for_calc.get('high', 0)) if day_data_for_calc is not None and pd.notna(day_data_for_calc.get('high', 0)) else 0
                    current_low = float(day_data_for_calc.get('low', 0)) if day_data_for_calc is not None and pd.notna(day_data_for_calc.get('low', 0)) else 0
                    current_volume = float(day_data_for_calc.get('volume', 0)) if day_data_for_calc is not None and pd.notna(day_data_for_calc.get('volume', 0)) else 0
                    # 修复成交量数据：本地CSV中的volume可能以手为单位，而实际成交量应为股
                    # 通常volume字段是以股为单位，但有些数据源会以手为单位（1手=100股）
                    # 根据数据特征判断，需要乘以100来统一单位
                    prev_volume_raw = float(prev_day_data.iloc[0]['volume']) if pd.notna(prev_day_data.iloc[0]['volume']) else 1
                    # 检测是否需要调整成交量单位
                    # 通过计算均价与收盘价的关系来判断
                    prev_amount = float(prev_day_data.iloc[0]['amount']) if pd.notna(prev_day_data.iloc[0]['amount']) else 0
                    
                    # 检测成交量单位：如果按原始volume计算均价远超收盘价，说明volume单位过小
                    if prev_volume_raw != 0:
                        raw_avg_price = prev_amount / prev_volume_raw if prev_volume_raw != 0 else 0
                        if raw_avg_price > prev_close * 5:  # 如果均价远大于收盘价，可能是单位问题
                            prev_volume = prev_volume_raw * 100  # 将手转换为股
                        else:
                            prev_volume = prev_volume_raw
                    else:
                        prev_volume = 1  # 防止除零错误
                    
                    if prev_close == 0:
                        print(f"股票 {stock_code} 前日收盘价为0，跳过")
                        continue
                    
                    # 获取竞价数据
                    rzq_call_auction_data = self.get_historical_auction_data(stock_code, target_date_str)
                    if rzq_call_auction_data is None:
                        print(f"股票 {stock_code}: 无法获取竞价数据，跳过弱转强判断")
                        continue
                    
                    # 获取最近4天的数据用于弱转强判断
                    recent_df = df[df['date'] <= pd.to_datetime(prev_day_data.iloc[0]['date'])].tail(4)
                    if len(recent_df) >= 4:
                        past_4_close = [float(row['close']) if pd.notna(row['close']) else 0 for _, row in recent_df.iterrows()]
                        prev_open_val = float(prev_day_data.iloc[0]['open']) if pd.notna(prev_day_data.iloc[0]['open']) else 0
                        prev_close_val = float(prev_day_data.iloc[0]['close']) if pd.notna(prev_day_data.iloc[0]['close']) else 0
                        
                        if len(past_4_close) >= 4 and all(c > 0 for c in past_4_close):
                            # 检查前3日涨幅 ≤ 28%
                            increase_ratio = (past_4_close[-1] - past_4_close[0]) / past_4_close[0]
                            if increase_ratio <= 0.28:  # 前3日涨幅 ≤ 28%
                                # 检查前一日跌幅 ≥ -5%
                                open_close_ratio = (prev_close_val - prev_open_val) / prev_open_val if prev_open_val != 0 else 0
                                if open_close_ratio >= -0.05:  # 前一日跌幅 ≥ -5%
                                    # 计算开盘价相对于涨停价/1.1的比例（开收比例）
                                    # 根据聚宽代码修改：使用涨停价/1.1作为基准
                                    # 需要根据股票类型确定涨停比例
                                    is_st = 'ST' in stock_code or 'st' in stock_code
                                    if stock_code.startswith('30'):  # 创业板股票
                                        limit_ratio = 0.2  # 20%涨停板
                                    elif is_st:  # ST股票
                                        limit_ratio = 0.05  # 5%涨停板
                                    else:
                                        limit_ratio = 0.1  # 10%涨停板
                                    limit_price = round(prev_close * (1 + limit_ratio), 2)
                                    current_ratio_to_close = rzq_call_auction_data['price'] / (limit_price / 1.1) if (limit_price / 1.1) != 0 else 0
                                    
                                    # 检查竞价是否在合理范围（相对于昨日收盘价）
                                    if 0.98 <= current_ratio_to_close <= 1.09:  # -2% 到 +9%
                                        # 检查成交量占比
                                        volume_condition = rzq_call_auction_data['volume'] / prev_volume >= 0.03 if prev_volume > 0 else False
                                        if volume_condition:
                                            # 检查均价涨幅
                                            avg_price_increase_value = prev_amount / prev_volume / prev_close - 1 if prev_volume != 0 and prev_close != 0 else 0
                                            if avg_price_increase_value >= -0.04:  # 均价涨幅 ≥ -4%
                                                # 检查成交额
                                                if 3e8 <= prev_amount <= 19e8:  # 成交额在3亿-19亿之间
                                                    # 获取市值数据
                                                    market_cap_result = self.get_market_cap(stock_code, target_date_str)
                                                    
                                                    # 检查是否成功获取市值数据
                                                    if market_cap_result is None:
                                                        print(f"无法获取股票 {stock_code} 的市值数据，跳过该股票")
                                                        continue  # 跳过该股票
                                                    
                                                    market_cap, circulating_market_cap = market_cap_result
                                                    
                                                    if market_cap >= 70 and circulating_market_cap <= 520:
                                                        # 检查左压条件
                                                        hst_data = df[df['date'] <= pd.to_datetime(prev_day_data.iloc[0]['date'])]
                                                        if len(hst_data) >= 2:
                                                            hst = hst_data.tail(101) if len(hst_data) >= 101 else hst_data
                                                            if len(hst) >= 2:
                                                                # 获取前一日的高点
                                                                prev_high = float(hst.iloc[-1]['high']) if pd.notna(hst.iloc[-1]['high']) else 0
                                                                
                                                                # 计算zyts_0
                                                                zyts_0 = next((i-1 for i, high in enumerate(hst['high'][-3::-1], 2) if pd.notna(high) and high >= prev_high), 100)  # 计算zyts_0
                                                                
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
                                                            print(f'股票 {stock_code} 满足弱转强条件: 前期涨幅={increase_ratio:.3f}, 开收比例={open_close_ratio:.3f}, 成交额={prev_amount/1e8:.2f}亿, 市值={market_cap:.2f}亿, 开盘比例={current_ratio_to_close:.3f}, 左压周期={zyts_0}天')
                    
                except Exception as e:
                    print(f"处理弱转强股票 {stock_code} 时出错: {e}")
                    continue  # 跳过有问题的股票
            
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
            import traceback
            traceback.print_exc()
            return []


def select_stocks_by_date(target_date):
    """选择指定日期的股票"""
    selector = TodayStockSelector()
    
    print(f"开始选择{target_date}股票: {target_date}")
    
    # 读取指定日期的pool数据
    # 修改路径：pool数据位于项目根目录下的full_stock_data/pool_data，而不是当前脚本目录
    pool_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'full_stock_data', 'pool_data', f'pool_{target_date}.json')
    
    if os.path.exists(pool_file_path):
        with open(pool_file_path, 'r', encoding='utf-8') as f:
            pool_data = json.load(f)
        print(f"已加载pool数据: {pool_file_path}")
    else:
        print(f"未找到{target_date}的pool数据文件: {pool_file_path}")
        return []
    
    print(f"使用{target_date}选股模块...")
    print(f"Pool数据: 昨日涨停{len(pool_data['limit_up_stocks'])}只, 首板{len(pool_data['first_board_stocks'])}只")
    
    results = selector.select_stocks_from_pool(target_date, pool_data)
    
    print(f"总共选出 {len(results)} 只股票")
    
    sbgk_stocks = [r for r in results if r['strategy'] == 'First Board High Open']
    sbdk_stocks = [r for r in results if r['strategy'] == 'First Board Low Open']
    rzq_stocks = [r for r in results if r['strategy'] == 'Weak to Strong']
    
    print(f"首板高开: {len(sbgk_stocks)} 只")
    print(f"首板低开: {len(sbdk_stocks)} 只")
    print(f"弱转强: {len(rzq_stocks)} 只")
    
    # 显示所有结果
    if results:
        print(f"\n{target_date}选中股票:")
        for i, stock in enumerate(results, 1):
            print(f"  {i}. {stock['code']}: {stock['name']} ({stock['strategy']})")
    else:
        print(f"\n{target_date}未选出任何股票")
    
    return results


def select_2026_01_12_stocks():
    """选择2026-01-12的股票"""
    return select_stocks_by_date("2026-01-12")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        select_stocks_by_date(target_date)
    else:
        select_2026_01_12_stocks()