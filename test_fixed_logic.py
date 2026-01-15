"""
测试修复后的弱转强开盘比例计算逻辑
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
import glob
import warnings
warnings.filterwarnings('ignore')


class FixedLogicTester:
    def __init__(self, data_dir=None):
        # 如果没有指定data_dir，则使用项目根目录下的full_stock_data
        if data_dir is None:
            # 获取当前文件所在目录
            current_file_dir = os.path.dirname(os.path.abspath(__file__))
            self.data_dir = os.path.join(current_file_dir, "full_stock_data")
        else:
            self.data_dir = data_dir
        self.daily_data_dir = os.path.join(self.data_dir, "daily_data")
        self.pool_data_dir = os.path.join(self.data_dir, "pool_data")
    
    def get_daily_data(self, stock_code):
        """从本地获取日线数据"""
        csv_file = os.path.join(self.daily_data_dir, f"{stock_code}.csv")
        if os.path.exists(csv_file):
            try:
                df = pd.read_csv(csv_file)
                
                # 处理列名
                original_columns = df.columns.tolist()
                column_mapping = {}
                for orig_col in original_columns:
                    if '涨\u3000跌\u3000幅' in orig_col or orig_col == '涨\u3000跌\u3000幅':
                        column_mapping[orig_col] = 'pct_change'
                    elif '涨\u3000跌\u3000额' in orig_col or orig_col == '涨\u3000跌\u3000额':
                        column_mapping[orig_col] = 'change'
                    elif '股\u3000票\u3000代\u3000码' in orig_col or orig_col == '股\u3000票\u3000代\u3000码':
                        column_mapping[orig_col] = 'code'
                    elif '开\u3000盘' in orig_col or orig_col == '开\u3000盘':
                        column_mapping[orig_col] = 'open'
                    elif '收\u3000盘' in orig_col or orig_col == '收\u3000盘':
                        column_mapping[orig_col] = 'close'
                    elif '最\u3000高' in orig_col or orig_col == '最\u3000高':
                        column_mapping[orig_col] = 'high'
                    elif '最\u3000低' in orig_col or orig_col == '最\u3000低':
                        column_mapping[orig_col] = 'low'
                    elif '成\u3000交\u3000量' in orig_col or orig_col == '成\u3000交\u3000量':
                        column_mapping[orig_col] = 'volume'
                    elif '成\u3000交\u3000额' in orig_col or orig_col == '成\u3000交\u3000额':
                        column_mapping[orig_col] = 'amount'
                    elif '振\u3000幅' in orig_col or orig_col == '振\u3000幅':
                        column_mapping[orig_col] = 'amplitude'
                    elif '换\u3000手\u3000率' in orig_col or orig_col == '换\u3000手\u3000率':
                        column_mapping[orig_col] = 'turnover'
                
                df.rename(columns=column_mapping, inplace=True)
                
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                
                return df
            except Exception as e:
                print(f"读取文件 {csv_file} 出错: {e}")
                return pd.DataFrame()
        return pd.DataFrame()
    
    def calculate_limit_price(self, prev_close, stock_code):
        """计算涨停价"""
        is_st = 'ST' in stock_code or 'st' in stock_code
        if stock_code.startswith('30'):  # 创业板股票
            limit_ratio = 0.2  # 20%涨停板
        elif is_st:  # ST股票
            limit_ratio = 0.05  # 5%涨停板
        else:
            limit_ratio = 0.1  # 10%涨停板
        return round(prev_close * (1 + limit_ratio), 2)
    
    def test_fixed_logic(self, stock_code, target_date_str):
        """
        测试修复后的弱转强逻辑
        target_date_str: 目标日期（选股日期），例如 '2025-12-18'
        """
        print(f"正在测试股票 {stock_code} 在 {target_date_str} 的修复后逻辑...")
        
        df = self.get_daily_data(stock_code)
        if df.empty:
            print(f"股票 {stock_code} 无数据")
            return
        
        # 将目标日期转换为datetime
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
        
        # 检查目标日期是否存在数据
        if target_date not in df.index:
            print(f"目标日期 {target_date_str} 无数据")
            return
        
        # 获取可用日期
        available_dates = df.index[df.index <= target_date]
        if len(available_dates) < 2:
            print("数据不足，至少需要2个交易日的数据")
            return
        
        # 获取关键日期的数据
        prev_idx = available_dates[-1]  # 选股当天
        prev1_idx = available_dates[-2]  # 前一日（炸板日）
        
        print(f"分析日期:")
        print(f"- 选股当天 ({target_date_str}): {prev_idx.date()}")
        print(f"- 炸板日: {prev1_idx.date()}")
        
        # 获取各日数据
        current_data = df.loc[prev_idx]  # 选股当天数据
        prev1_data = df.loc[prev1_idx]  # 炸板日数据
        
        print(f"\n各日数据:")
        print(f"炸板日 - 开盘: {prev1_data['open']:.2f}, 收盘: {prev1_data['close']:.2f}, 最高: {prev1_data['high']:.2f}")
        print(f"选股当天 - 开盘: {current_data['open']:.2f}, 昨收: {prev1_data['close']:.2f}")
        
        # 修复后的逻辑：使用前日收盘价作为基准
        fixed_open_ratio = current_data['open'] / prev1_data['close']
        print(f"\n修复后的弱转强开盘比例计算:")
        print(f"开盘价 / 前日收盘价 = {current_data['open']:.2f} / {prev1_data['close']:.2f} = {fixed_open_ratio:.3f}")
        
        # 检查是否满足条件 (0.98 <= ratio <= 1.09)
        condition_met = 0.98 <= fixed_open_ratio <= 1.09
        print(f"是否满足条件 (0.98 <= {fixed_open_ratio:.3f} <= 1.09): {condition_met}")
        
        if condition_met:
            print(f"✅ 修复后，股票 {stock_code} 在弱转强策略中满足开盘比例条件！")
        else:
            print(f"❌ 即使修复后，股票 {stock_code} 在弱转强策略中仍不满足开盘比例条件。")
        
        # 对比修复前的逻辑
        limit_price = self.calculate_limit_price(prev1_data['close'], stock_code)
        old_method_ratio = current_data['open'] / (limit_price / 1.1)
        print(f"\n修复前的弱转强开盘比例计算:")
        print(f"开盘价 / (涨停价/1.1) = {current_data['open']:.2f} / ({limit_price:.2f}/1.1) = {current_data['open']:.2f} / {limit_price/1.1:.2f} = {old_method_ratio:.3f}")
        old_condition_met = 0.98 <= old_method_ratio <= 1.09
        print(f"修复前是否满足条件: {old_condition_met}")
        
        print(f"\n对比结果:")
        print(f"修复前开盘比例: {old_method_ratio:.3f}, 满足条件: {old_condition_met}")
        print(f"修复后开盘比例: {fixed_open_ratio:.3f}, 满足条件: {condition_met}")
        
        if not old_condition_met and condition_met:
            print(f"✅ 修复成功！股票现在可以被选中了")
        elif old_condition_met and condition_met:
            print(f"ℹ️  修复前后都满足条件")
        elif not old_condition_met and not condition_met:
            print(f"ℹ️  修复前后都不满足条件")
        else:
            print(f"⚠️  修复后反而不满足条件了")
        
        return {
            'stock_code': stock_code,
            'target_date': target_date_str,
            'fixed_ratio': fixed_open_ratio,
            'fixed_condition_met': condition_met,
            'old_ratio': old_method_ratio,
            'old_condition_met': old_condition_met,
            'fix_improved': not old_condition_met and condition_met
        }


def main():
    """主函数 - 测试修复后的逻辑"""
    print("弱转强开盘比例计算逻辑修复测试")
    print("="*60)
    
    tester = FixedLogicTester()
    
    # 测试用户关注的股票 301408，在 2025-12-18 选股
    stock_code = "301408"
    target_date = "2025-12-18"
    
    print(f"测试股票: {stock_code}")
    print(f"选股日期: {target_date}")
    print()
    
    result = tester.test_fixed_logic(stock_code, target_date)
    
    print(f"\n测试结果总结:")
    print(f"- 股票代码: {result['stock_code']}")
    print(f"- 修复前比例: {result['old_ratio']:.3f}, 满足条件: {result['old_condition_met']}")
    print(f"- 修复后比例: {result['fixed_ratio']:.3f}, 满足条件: {result['fixed_condition_met']}")
    print(f"- 修复是否改善: {result['fix_improved']}")


if __name__ == "__main__":
    main()