"""
基于本地数据的选股验证模块
用于验证选股逻辑并对比不同时间段的结果
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sqlite3
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

from local_data_manager import LocalDataManager


class LocalStockSelector:
    def __init__(self, data_manager: LocalDataManager):
        self.data_manager = data_manager
        self.stock_pool = []
        
    def get_stock_list(self):
        """从本地获取股票列表"""
        return self.data_manager.get_stock_list()
    
    def get_daily_data(self, stock_code):
        """从本地获取日线数据"""
        return self.data_manager.get_daily_data(stock_code)
    
    def get_stock_info(self, stock_code):
        """从本地获取股票信息"""
        return self.data_manager.get_stock_info(stock_code)
    
    def calculate_limit_price(self, stock_code, date_str):
        """计算涨停价 - 从本地数据计算"""
        df = self.get_daily_data(stock_code)
        if df.empty:
            return 0
        
        # 找到指定日期的前一个交易日
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
        available_dates = df.index[df.index <= target_date]
        
        if len(available_dates) < 2:
            return 0
        
        prev_date = available_dates[-2]  # 前一个交易日
        prev_close = df.loc[prev_date, 'close']
        
        # 获取股票信息判断是否为ST
        stock_info = self.get_stock_info(stock_code)
        is_st = False
        if stock_info is not None and pd.notna(stock_info.get('name')):
            is_st = 'ST' in str(stock_info['name']) or 'st' in str(stock_info['name']).lower()
        
        # 计算涨停价
        limit_ratio = 0.05 if is_st else 0.1
        return round(prev_close * (1 + limit_ratio), 2)
    
    def is_limit_up(self, stock_code, date_str):
        """判断是否涨停 - 从本地数据判断"""
        df = self.get_daily_data(stock_code)
        if df.empty:
            return False
        
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
        if target_date not in df.index:
            return False
        
        current_data = df.loc[target_date]
        
        # 直接使用涨跌幅列判断涨幅是否 >= 9.75%
        # 在local_data_manager中，列名为'涨　跌　幅　'而不是'pct_change'
        pct_change_col = '涨\u3000跌\u3000幅\u3000'
        if pct_change_col in current_data and pd.notna(current_data[pct_change_col]):
            pct_change = float(current_data[pct_change_col])
            return pct_change >= 9.75
        else:
            # 如果涨跌幅列为空，使用容差方法作为备用
            limit_price = self.calculate_limit_price(stock_code, date_str)
            return abs(current_data['close'] - limit_price) < 0.02
    
    def calculate_relative_position(self, df, window=60):
        """计算相对位置 (close-low)/(high-low)"""
        if len(df) < window:
            return 0
        recent = df.tail(window)
        low_val = recent['low'].min()
        high_val = recent['high'].max()
        if high_val == low_val:
            return 0.5  # 避免除零
        current_close = df['close'].iloc[-1]
        return (current_close - low_val) / (high_val - low_val)
    
    def get_limit_up_stocks(self, stock_list, date_str):
        """获取指定日期的涨停股票"""
        print(f"正在查找 {date_str} 的涨停股票...")
        limit_up_stocks = []
        
        for i, stock_code in enumerate(stock_list):
            if isinstance(stock_code, dict):
                # 如果是字典格式，提取code
                stock_code = stock_code['code']
            
            if self.is_limit_up(stock_code, date_str):
                limit_up_stocks.append(stock_code)
                
            if (i + 1) % 100 == 0:
                print(f"已检查 {i+1}/{len(stock_list)} 只股票")
        
        print(f"找到 {len(limit_up_stocks)} 只涨停股票")
        return limit_up_stocks
    
    def select_first_limit_up_open_high(self, limit_up_stocks, date_str):
        """选股1: 首板高开"""
        print("开始筛选首板高开股票...")
        selected = []
        
        for i, stock in enumerate(limit_up_stocks):
            if i % 10 == 0:
                print(f"已处理首板高开筛选: {i}/{len(limit_up_stocks)}")
                
            # 获取历史数据
            df = self.get_daily_data(stock)
            if df.empty or len(df) < 3:
                continue
            
            # 找到目标日期的数据
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
            if target_date not in df.index:
                continue
            
            # 获取前一个交易日的数据
            available_dates = df.index[df.index < target_date]
            if len(available_dates) < 2:
                continue
                
            prev_idx = available_dates[-1]  # 前一天
            prev2_idx = available_dates[-2]  # 前两天
            
            # 条件一：均价，金额，市值，换手率过滤
            prev_close = df.loc[prev2_idx, 'close']
            prev_volume = df.loc[prev2_idx, 'volume'] if 'volume' in df.columns else 0
            prev_amount = df.loc[prev2_idx, 'amount'] if 'amount' in df.columns else 0
            
            if prev_amount == 0 or prev_volume == 0 or prev_close == 0:
                continue
                
            # 计算均价增益值
            avg_price = prev_amount / prev_volume if prev_volume != 0 else 0
            avg_price_increase_value = (avg_price / prev_close) * 1.1 - 1
            
            if avg_price_increase_value < 0.07 or prev_amount < 5.5e8 or prev_amount > 20e8:
                continue
            
            # 条件二：高开比例
            current_open = df.loc[prev_idx, 'open']  # 当天开盘价
            current_ratio = current_open / prev_close
            if current_ratio <= 1.0 or current_ratio >= 1.06:
                continue
            
            # 条件三：左压 - 检查成交量是否放大
            if len(df) >= 102:
                hst = df[df.index <= prev2_idx].tail(101)  # 最近101天数据
                if len(hst) >= 2:
                    prev_high = hst['high'].iloc[-2]  # 前一天的高点
                    
                    # 计算前高位置
                    zyts_0 = 100
                    for j in range(len(hst)-3, -1, -1):
                        if hst['high'].iloc[j] >= prev_high:
                            zyts_0 = len(hst) - 1 - j
                            break
                    
                    zyts = zyts_0 + 5
                    if zyts <= len(hst):
                        volume_data = hst['volume'].tail(zyts)
                        if len(volume_data) >= 2:
                            current_vol = volume_data.iloc[-1]
                            max_prev_vol = volume_data.iloc[:-1].max()
                            if current_vol > max_prev_vol * 0.9:
                                selected.append(stock)
        
        return selected
    
    def select_first_limit_up_open_low(self, limit_up_stocks, date_str):
        """选股2: 首板低开"""
        print("开始筛选首板低开股票...")
        selected = []
        
        for i, stock in enumerate(limit_up_stocks):
            if i % 10 == 0:
                print(f"已处理首板低开筛选: {i}/{len(limit_up_stocks)}")
                
            # 获取历史数据
            df = self.get_daily_data(stock)
            if df.empty or len(df) < 2:
                continue
            
            # 找到目标日期的数据
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
            available_dates = df.index[df.index < target_date]
            if len(available_dates) < 2 or target_date not in df.index:
                continue
            
            prev_idx = available_dates[-1]  # 前一天
            current_data = df.loc[target_date]
            prev_data = df.loc[prev_idx]
            
            # 获取60日数据计算相对位置
            hist_60 = df[df.index <= prev_idx].tail(60)
            if len(hist_60) < 60:
                continue
            
            rp = self.calculate_relative_position(hist_60, 60)
            money = prev_data['amount'] if 'amount' in df.columns else 0
            
            if rp <= 0.5 and money >= 1e8:
                # 检查开盘比例
                open_price = current_data['open']
                prev_close = prev_data['close']
                current_ratio = open_price / prev_close
                
                if current_ratio <= 0.97 and current_ratio >= 0.955:
                    selected.append(stock)
        
        return selected
    
    def select_stocks(self, date_str=None):
        """选股主函数"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        print(f"开始选股: {date_str}")
        
        # 获取股票池
        stock_list = self.get_stock_list()
        if len(stock_list) == 0:
            print("获取股票列表失败")
            return []
        
        # 获取前一日日期
        yesterday = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 获取前一日涨停股票
        prev_day_limit_up = self.get_limit_up_stocks(stock_list, yesterday)
        
        # 执行两种选股策略（简化版，先做首板高开和低开）
        sbgk_stocks = self.select_first_limit_up_open_high(prev_day_limit_up, date_str)
        sbdk_stocks = self.select_first_limit_up_open_low(prev_day_limit_up, date_str)
        
        # 合并结果
        all_qualified = sbgk_stocks + sbdk_stocks
        
        print(f'\n=== 选股结果 ===')
        print(f'今日选股：{all_qualified}')
        print(f'首板高开：{sbgk_stocks}')
        print(f'首板低开：{sbdk_stocks}')
        print(f'总计选出 {len(all_qualified)} 只股票')
        
        return {
            'all': all_qualified,
            'sbgk': sbgk_stocks,
            'sbdk': sbdk_stocks
        }


class BacktestValidator:
    """回测验证器，用于验证选股逻辑"""
    
    def __init__(self, selector: LocalStockSelector):
        self.selector = selector
        
    def backtest_period(self, start_date: str, end_date: str) -> Dict:
        """回测指定时间段"""
        print(f"开始回测 {start_date} 到 {end_date}")
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        results = {
            'dates': [],
            'selected_counts': [],
            'selected_stocks': []
        }
        
        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # 检查是否为交易日（简单判断，实际应该查询交易日历）
            if current_date.weekday() < 5:  # 假设工作日为交易日
                print(f"\n回测日期: {date_str}")
                
                try:
                    selection_result = self.selector.select_stocks(date_str)
                    selected_count = len(selection_result['all'])
                    
                    results['dates'].append(date_str)
                    results['selected_counts'].append(selected_count)
                    results['selected_stocks'].append(selection_result)
                    
                    print(f"选出 {selected_count} 只股票")
                except Exception as e:
                    print(f"回测日期 {date_str} 失败: {e}")
            
            current_date += timedelta(days=1)
        
        return results
    
    def analyze_results(self, backtest_results: Dict):
        """分析回测结果"""
        print("\n=== 回测结果分析 ===")
        
        if not backtest_results['dates']:
            print("没有回测结果")
            return
        
        avg_daily_selections = np.mean(backtest_results['selected_counts'])
        max_daily_selections = np.max(backtest_results['selected_counts'])
        total_selections = np.sum(backtest_results['selected_counts'])
        
        print(f"回测天数: {len(backtest_results['dates'])}")
        print(f"平均每日选股数: {avg_daily_selections:.2f}")
        print(f"最大单日选股数: {max_daily_selections}")
        print(f"总计选股次数: {total_selections}")
        
        # 统计选股分布
        selection_counts = np.array(backtest_results['selected_counts'])
        non_zero_selections = selection_counts[selection_counts > 0]
        print(f"有选股的交易日: {len(non_zero_selections)}")
        print(f"选股成功率: {len(non_zero_selections)/len(backtest_results['dates'])*100:.2f}%")
        
        return {
            'avg_daily_selections': avg_daily_selections,
            'max_daily_selections': max_daily_selections,
            'total_selections': total_selections,
            'success_rate': len(non_zero_selections)/len(backtest_results['dates'])
        }


def main():
    """主函数 - 演示本地数据选股验证"""
    print("基于本地数据的选股验证模块")
    print("="*50)
    
    # 创建数据管理器
    data_manager = LocalDataManager()
    
    # 创建选股器
    selector = LocalStockSelector(data_manager)
    
    # 获取股票列表（需要确保有数据）
    stock_list = data_manager.get_stock_list()
    if len(stock_list) == 0:
        print("本地没有股票列表数据，请先运行数据获取模块")
        return
    
    print(f"本地股票数量: {len(stock_list)}")
    
    # 演示选股功能
    try:
        # 选择一个最近的日期进行演示
        recent_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"\n演示选股 - 日期: {recent_date}")
        
        results = selector.select_stocks(recent_date)
        print(f"演示完成，选出 {len(results['all'])} 只股票")
        
        # 进行短期回测验证
        print(f"\n进行短期回测验证...")
        validator = BacktestValidator(selector)
        
        # 使用最近5个交易日进行回测
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        backtest_results = validator.backtest_period(start_date, end_date)
        analysis = validator.analyze_results(backtest_results)
        
        print(f"\n验证完成！")
        print(f"平均每日选股数: {analysis['avg_daily_selections']:.2f}")
        print(f"选股成功率: {analysis['success_rate']*100:.2f}%")
        
    except Exception as e:
        print(f"执行过程中出现错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()