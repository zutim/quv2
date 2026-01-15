"""
开盘比例计算方式分析器
分析弱转强和首板高开中开盘比例的不同计算方式
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
import glob
import warnings
warnings.filterwarnings('ignore')


class OpenRatioAnalyzer:
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
    
    def analyze_open_ratio_calculation(self, stock_code, target_date_str):
        """
        分析开盘比例的不同计算方式
        target_date_str: 目标日期（选股日期），例如 '2025-12-18'
        """
        print(f"正在分析股票 {stock_code} 在 {target_date_str} 的开盘比例计算方式...")
        
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
        
        # 计算涨停价
        limit_price = self.calculate_limit_price(prev1_data['close'], stock_code)
        print(f"昨收: {prev1_data['close']:.2f}, 涨停价: {limit_price:.2f}")
        
        # 方式1：首板高开的计算方式（使用前一日收盘价作为基准）
        open_ratio_method1 = current_data['open'] / prev1_data['close']
        print(f"\n方法1 - 首板高开计算方式: 开盘价/前日收盘价 = {current_data['open']:.2f}/{prev1_data['close']:.2f} = {open_ratio_method1:.3f}")
        
        # 方式2：弱转强的计算方式（使用涨停价/1.1作为基准）
        # 这里的逻辑是 high_limit/1.1 应该等于前日收盘价
        # 因为 high_limit = 前日收盘价 * (1 + 涨停比例)，所以 high_limit/1.1 ≈ 前日收盘价（对普通股而言）
        high_limit_div_11 = limit_price / 1.1
        open_ratio_method2 = current_data['open'] / high_limit_div_11
        print(f"方法2 - 弱转强计算方式: 开盘价/(涨停价/1.1) = {current_data['open']:.2f}/{high_limit_div_11:.2f} = {open_ratio_method2:.3f}")
        
        # 验证理论计算
        theoretical_divisor = prev1_data['close'] * (1 + 0.1) / 1.1  # 对于10%涨停板，理论上应该是prev_close
        if stock_code.startswith('30'):  # 创业板
            theoretical_divisor = prev1_data['close'] * (1 + 0.2) / 1.1  # 20%涨停板除以1.1
            print(f"注意: {stock_code} 是创业板股票，使用20%涨停板计算")
        else:
            print(f"注意: {stock_code} 是普通股票，使用10%涨停板计算")
        
        print(f"理论验证 - 涨停价/1.1: {limit_price:.2f}/1.1 = {high_limit_div_11:.2f}")
        print(f"理论上，对于非创业板股票，涨停价/1.1 应该等于前日收盘价 {prev1_data['close']:.2f}")
        
        # 检查两种方法的差异
        diff = abs(open_ratio_method1 - open_ratio_method2)
        print(f"\n两种计算方式的差异: |{open_ratio_method1:.3f} - {open_ratio_method2:.3f}| = {diff:.6f}")
        
        if diff > 0.001:  # 如果差异较大
            print("警告: 两种计算方式存在显著差异！")
            print("- 首板高开方式使用前日收盘价作为基准")
            print("- 弱转强方式使用涨停价/1.1作为基准")
            print("- 对于10%涨停板的股票，两种方式应该相同，因为涨停价/1.1 = 前日收盘价")
            print("- 对于20%涨停板的创业板股票，两种方式会不同")
        else:
            print("两种计算方式基本一致")
        
        # 分析301408的情况
        if stock_code == "301408":
            print(f"\n=== 301408股票具体情况分析 ===")
            print(f"股票代码: {stock_code} (创业板股票)")
            print(f"前日收盘价: {prev1_data['close']:.2f}")
            print(f"涨停价(20%): {limit_price:.2f}")
            print(f"涨停价/1.1: {high_limit_div_11:.2f}")
            print(f"选股日开盘价: {current_data['open']:.2f}")
            print(f"按首板高开方式计算的开盘比例: {open_ratio_method1:.3f}")
            print(f"按弱转强方式计算的开盘比例: {open_ratio_method2:.3f}")
            print(f"策略要求开盘比例在0.98-1.09之间")
            print(f"首板高开方式下是否满足条件: {0.98 <= open_ratio_method1 <= 1.09}")
            print(f"弱转强方式下是否满足条件: {0.98 <= open_ratio_method2 <= 1.09}")
            print(f"这解释了为什么这只股票在弱转强策略中未被选中")
        
        return {
            'stock_code': stock_code,
            'target_date': target_date_str,
            'method1_ratio': open_ratio_method1,
            'method2_ratio': open_ratio_method2,
            'method1_valid': 0.98 <= open_ratio_method1 <= 1.09,
            'method2_valid': 0.98 <= open_ratio_method2 <= 1.09,
            'difference': diff
        }


def main():
    """主函数 - 分析开盘比例计算方式"""
    print("开盘比例计算方式分析器")
    print("="*60)
    
    analyzer = OpenRatioAnalyzer()
    
    # 分析用户关注的股票 301408，在 2025-12-18 选股
    stock_code = "301408"
    target_date = "2025-12-18"
    
    print(f"分析股票: {stock_code}")
    print(f"选股日期: {target_date}")
    print()
    
    result = analyzer.analyze_open_ratio_calculation(stock_code, target_date)
    
    print(f"\n分析结果总结:")
    print(f"- 股票代码: {result['stock_code']}")
    print(f"- 首板高开计算方式比例: {result['method1_ratio']:.3f}, 是否满足条件: {result['method1_valid']}")
    print(f"- 弱转强计算方式比例: {result['method2_ratio']:.3f}, 是否满足条件: {result['method2_valid']}")
    print(f"- 两种方式差异: {result['difference']:.6f}")


if __name__ == "__main__":
    main()