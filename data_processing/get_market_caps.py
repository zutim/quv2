#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票市值数据获取和管理程序
用于获取并存储A股市场所有股票的市值数据到本地
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import time
import warnings
warnings.filterwarnings('ignore')


class StockMarketCapManager:
    def __init__(self, data_dir='full_stock_data'):
        self.data_dir = data_dir
        self.market_cap_file = os.path.join(data_dir, 'market_caps.json')
        self.daily_data_dir = os.path.join(data_dir, 'daily_data')  # 本地日线数据目录
        self.ensure_directory_exists()
    
    def ensure_directory_exists(self):
        """确保数据目录存在"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        if not os.path.exists(self.daily_data_dir):
            os.makedirs(self.daily_data_dir)
    
    def get_current_market_caps(self):
        """获取当前所有A股的市值数据"""
        try:
            print("正在获取A股实时行情数据...")
            # 获取当前A股实时数据
            stock_data = ak.stock_zh_a_spot_em()
            
            # 选择需要的列
            required_cols = ['代码', '名称', '最新价', '总市值', '流通市值', '涨跌幅']
            available_cols = [col for col in required_cols if col in stock_data.columns]
            stock_data = stock_data[available_cols].copy()
            
            # 创建市值字典
            market_caps = {}
            for index, row in stock_data.iterrows():
                stock_code = str(row['代码'])
                
                # 检查市值数据是否有效
                total_mv = pd.to_numeric(row['总市值'], errors='coerce') if pd.notna(row['总市值']) else 0
                circ_mv = pd.to_numeric(row['流通市值'], errors='coerce') if pd.notna(row['流通市值']) else 0
                
                # 转换为亿元单位
                total_mv = total_mv / 1e8 if total_mv > 0 else 0
                circ_mv = circ_mv / 1e8 if circ_mv > 0 else 0
                
                stock_name = row['名称'] if '名称' in row else f"股票{stock_code}"
                current_price = pd.to_numeric(row['最新价'], errors='coerce') if pd.notna(row['最新价']) else 0
                
                market_caps[stock_code] = {
                    'name': stock_name,
                    'current_price': float(current_price),
                    'total_market_cap': float(total_mv),
                    'circulating_market_cap': float(circ_mv),
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # 每1000条记录显示一次进度
                if (index + 1) % 1000 == 0:
                    print(f"已处理 {index + 1}/{len(stock_data)} 条股票数据")
            
            print(f"成功获取 {len(market_caps)} 只股票的市值数据")
            return market_caps
            
        except Exception as e:
            print(f"获取市值数据失败: {e}")
            return {}
    
    def save_market_caps(self, market_caps):
        """保存市值数据到本地文件"""
        try:
            # 添加获取时间戳
            data_to_save = {
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'market_caps': market_caps
            }
            
            with open(self.market_cap_file, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
            
            print(f"市值数据已保存到: {self.market_cap_file}")
            return True
        except Exception as e:
            print(f"保存市值数据失败: {e}")
            return False
    
    def load_market_caps(self):
        """从本地文件加载市值数据"""
        try:
            if not os.path.exists(self.market_cap_file):
                print(f"市值数据文件不存在: {self.market_cap_file}")
                return {}
            
            with open(self.market_cap_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 返回市值数据
            return data.get('market_caps', {})
        except Exception as e:
            print(f"加载市值数据失败: {e}")
            return {}
    
    def get_historical_price_from_local(self, stock_code, date_str):
        """
        从本地历史数据获取指定日期的股价
        :param stock_code: 股票代码
        :param date_str: 日期 (格式: 'YYYY-MM-DD')
        :return: 收盘价，如果找不到则返回None
        """
        try:
            # 构建CSV文件路径
            csv_file = os.path.join(self.daily_data_dir, f'{stock_code}.csv')
            if not os.path.exists(csv_file):
                print(f"股票 {stock_code} 的本地历史数据文件不存在: {csv_file}")
                return None
            
            # 读取CSV文件
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
                # 如果找不到指定日期的数据，返回None
                return None
            
            day_data = day_data.iloc[0]
            close_price = float(day_data.get('close', 0)) if pd.notna(day_data.get('close', 0)) else 0
            
            if close_price <= 0:  # 无效价格，返回None
                return None
            
            return close_price
            
        except Exception as e:
            print(f"从本地数据获取股票 {stock_code} 在 {date_str} 的价格失败: {e}")
            return None
    
    def get_historical_market_cap(self, stock_code, historical_date, current_date=None):
        """
        根据历史日期的股价比例估算历史市值
        :param stock_code: 股票代码
        :param historical_date: 历史日期 (格式: 'YYYY-MM-DD')
        :param current_date: 当前日期 (格式: 'YYYY-MM-DD')，如果为None则使用今天
        :return: (总市值, 流通市值) in 亿元
        """
        if current_date is None:
            current_date = datetime.now().strftime('%Y-%m-%d')
        
        # 特殊情况：如果查询的日期是当前日期或未来日期，直接返回当前市值
        if pd.to_datetime(historical_date) >= pd.to_datetime(current_date):
            print(f"查询日期 {historical_date} 是今日或未来日期，返回当前市值")
            current_market_caps = self.load_market_caps()
            
            if stock_code not in current_market_caps:
                print(f"股票 {stock_code} 的当前市值数据不存在")
                return 0, 0
            
            # 获取当前市值和价格
            current_stock_data = current_market_caps[stock_code]
            current_market_cap = current_stock_data['total_market_cap']
            current_circulating_market_cap = current_stock_data['circulating_market_cap']
            
            return current_market_cap, current_circulating_market_cap
        
        # 加载当前市值数据
        current_market_caps = self.load_market_caps()
        
        if stock_code not in current_market_caps:
            print(f"股票 {stock_code} 的当前市值数据不存在")
            return 0, 0
        
        # 获取当前市值和价格
        current_stock_data = current_market_caps[stock_code]
        current_market_cap = current_stock_data['total_market_cap']
        current_circulating_market_cap = current_stock_data['circulating_market_cap']
        current_price = current_stock_data['current_price']
        
        if current_price <= 0:
            print(f"股票 {stock_code} 当前价格无效: {current_price}")
            return 0, 0
        
        # 优先从本地历史数据获取历史股价
        historical_close = self.get_historical_price_from_local(stock_code, historical_date)
        
        if historical_close is None:
            print(f"无法从本地数据获取股票 {stock_code} 在 {historical_date} 的历史价格，尝试使用最近可用价格")
            # 如果指定日期的数据不存在，尝试使用最近的可用数据
            historical_close = self._get_most_recent_price(stock_code, historical_date)
            
            if historical_close is None:
                print(f"无法获取股票 {stock_code} 在 {historical_date} 或附近的可用价格")
                return 0, 0
        
        # 计算价格比例
        price_ratio = historical_close / current_price
        
        # 估算历史市值
        historical_market_cap = current_market_cap * price_ratio
        historical_circulating_market_cap = current_circulating_market_cap * price_ratio
        
        return historical_market_cap, historical_circulating_market_cap
    
    def update_market_caps(self):
        """更新市值数据"""
        print("开始更新市值数据...")
        
        # 获取最新的市值数据
        market_caps = self.get_current_market_caps()
        
        if not market_caps:
            print("未能获取到市值数据，更新失败")
            return False
        
        # 保存数据
        success = self.save_market_caps(market_caps)
        
        if success:
            print("市值数据更新完成")
        else:
            print("市值数据更新失败")
        
        return success


def main():
    """主函数 - 命令行运行"""
    manager = StockMarketCapManager()
    
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "update":
            manager.update_market_caps()
        elif sys.argv[1] == "get" and len(sys.argv) >= 4:
            stock_code = sys.argv[2]
            date = sys.argv[3]
            total_mc, circ_mc = manager.get_historical_market_cap(stock_code, date)
            print(f"股票 {stock_code} 在 {date} 的市值:")
            print(f"  总市值: {total_mc:.2f} 亿")
            print(f"  流通市值: {circ_mc:.2f} 亿")
        else:
            print("用法:")
            print("  python get_market_caps.py update          # 更新市值数据")
            print("  python get_market_caps.py get <股票代码> <日期>  # 获取历史市值")
    else:
        # 默认执行更新
        print("执行市值数据更新...")
        manager.update_market_caps()


if __name__ == "__main__":
    main()