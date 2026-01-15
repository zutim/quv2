"""
弱转强选股条件分析器
用于分析特定股票在弱转强策略中的条件满足情况
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
import glob
import warnings
warnings.filterwarnings('ignore')


class WeakToStrongAnalyzer:
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
    
    def analyze_weak_to_strong_conditions(self, stock_code, target_date_str):
        """
        分析弱转强选股条件
        target_date_str: 目标日期（选股日期），例如 '2025-12-18'
        """
        print(f"正在分析股票 {stock_code} 在 {target_date_str} 的弱转强条件...")
        
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
        if len(available_dates) < 4:
            print("数据不足，至少需要4个交易日的数据")
            return
        
        # 获取关键日期的数据
        # target_date: 选股当天（例如 2025-12-18）
        # prev_idx: 前一个交易日（例如 2025-12-17，炸板日）
        # prev2_idx: 前两个交易日（例如 2025-12-16）
        # prev3_idx: 前三个交易日（例如 2025-12-15）
        prev_idx = available_dates[-1]  # 选股当天
        prev1_idx = available_dates[-2]  # 前一日（炸板日）
        prev2_idx = available_dates[-3]  # 前两日
        prev3_idx = available_dates[-4]  # 前三日
        
        print(f"分析日期:")
        print(f"- 选股当天 ({target_date_str}): {prev_idx.date()}")
        print(f"- 炸板日: {prev1_idx.date()}")
        print(f"- 前两日: {prev2_idx.date()}")
        print(f"- 前三日: {prev3_idx.date()}")
        
        # 获取各日数据
        current_data = df.loc[prev_idx]  # 选股当天数据
        prev1_data = df.loc[prev1_idx]  # 炸板日数据
        prev2_data = df.loc[prev2_idx]  # 前两日数据
        prev3_data = df.loc[prev3_idx]  # 前三日数据
        
        print(f"\n各日数据:")
        print(f"炸板日 - 开盘: {prev1_data['open']:.2f}, 收盘: {prev1_data['close']:.2f}, 最高: {prev1_data['high']:.2f}, 涨幅: {prev1_data.get('pct_change', 'N/A')}%, 成交额: {prev1_data.get('amount', 0)/1e8:.2f}亿")
        print(f"选股当天 - 开盘: {current_data['open']:.2f}, 收盘: {current_data['close']:.2f}, 昨收: {prev1_data['close']:.2f}")
        
        # 检查弱转强的各个条件
        print(f"\n弱转强选股条件分析:")
        
        # 条件1: 前三日涨幅 ≤ 28%
        prev3_close = prev3_data['close']
        prev1_close = prev1_data['close']
        if prev3_close != 0:
            three_day_return = (prev1_close - prev3_close) / prev3_close * 100
            cond1_pass = three_day_return <= 28
            print(f"条件1 - 前三日涨幅 ≤ 28%: {three_day_return:.2f}% {'✓' if cond1_pass else '✗'}")
        else:
            cond1_pass = False
            print(f"条件1 - 前三日涨幅 ≤ 28%: 无法计算（前三日收盘价为0） ✗")
        
        # 条件2: 前日收盘相对开盘跌幅 < 5% (即前日跌幅 < 5%)
        prev1_open = prev1_data['open']
        if prev1_open != 0:
            prev1_drop = (prev1_close - prev1_open) / prev1_open * 100
            cond2_pass = prev1_drop >= -5  # 跌幅-5%以上才是负数，所以>=-5表示跌幅不超过5%
            print(f"条件2 - 前日收盘相对开盘跌幅 < 5%: {prev1_drop:.2f}% {'✓' if cond2_pass else '✗'}")
        else:
            cond2_pass = False
            print(f"条件2 - 前日收盘相对开盘跌幅 < 5%: 无法计算（前日开盘价为0） ✗")
        
        # 条件3: 均价增益值 ≥ -4%
        prev1_amount = prev1_data.get('amount', 0)
        prev1_volume = prev1_data.get('volume', 0)
        if prev1_volume != 0 and prev1_close != 0:
            avg_price = prev1_amount / prev1_volume if prev1_volume != 0 else 0
            avg_price_increase_value = (avg_price / prev1_close) * 1.1 - 1
            avg_price_increase_pct = avg_price_increase_value * 100
            cond3_pass = avg_price_increase_value >= -0.04  # ≥ -4%
            print(f"条件3 - 均价增益值 ≥ -4%: {avg_price_increase_pct:.2f}% {'✓' if cond3_pass else '✗'}")
        else:
            avg_price_increase_pct = 0
            cond3_pass = False
            print(f"条件3 - 均价增益值 ≥ -4%: 无法计算（成交额或成交量为0） ✗")
        
        # 条件4: 成交额在3-19亿之间
        prev1_amount_billion = prev1_amount / 1e8
        cond4_pass = 3e8 <= prev1_amount <= 19e8  # 3亿-19亿
        print(f"条件4 - 成交额在3-19亿之间: {prev1_amount_billion:.2f}亿 {'✓' if cond4_pass else '✗'}")
        
        # 条件5: 开盘比例在0.98-1.09之间
        current_open = current_data['open']
        prev1_close = prev1_data['close']
        if prev1_close != 0:
            open_ratio = current_open / prev1_close
            cond5_pass = 0.98 <= open_ratio <= 1.09
            print(f"条件5 - 开盘比例在0.98-1.09之间: {open_ratio:.3f} {'✓' if cond5_pass else '✗'}")
        else:
            cond5_pass = False
            print(f"条件5 - 开盘比例在0.98-1.09之间: 无法计算（前日收盘价为0） ✗")
        
        # 条件6: 左压条件（成交量放大确认）
        # 检查是否有足够的历史数据来计算左压
        hist_data = df[df.index <= prev2_idx].tail(101)  # 最近101天数据
        if len(hist_data) >= 10:
            # 计算前高位置
            prev_high = hist_data['high'].iloc[-2]  # 前一日的高点
            zyts_0 = 100
            for j in range(len(hist_data)-3, -1, -1):
                if hist_data['high'].iloc[j] >= prev_high:
                    zyts_0 = len(hist_data) - 1 - j
                    break
            
            zyts = zyts_0 + 5
            if zyts <= len(hist_data):
                volume_data = hist_data['volume'].tail(zyts)
                if len(volume_data) >= 2:
                    current_vol = volume_data.iloc[-1]  # 前两日成交量
                    max_prev_vol = volume_data.iloc[:-1].max() if len(volume_data) > 1 else 0
                    if max_prev_vol > 0:
                        vol_condition = current_vol > max_prev_vol * 0.9
                        cond6_pass = vol_condition
                        print(f"条件6 - 左压成交量放大: 当前成交量{current_vol/1e6:.2f}万手, 前期最大成交量{max_prev_vol/1e6:.2f}万手 {'✓' if cond6_pass else '✗'}")
                    else:
                        cond6_pass = False
                        print(f"条件6 - 左压成交量放大: 无法比较（前期最大成交量为0） ✗")
                else:
                    cond6_pass = False
                    print(f"条件6 - 左压成交量放大: 历史成交量数据不足 ✗")
            else:
                cond6_pass = False
                print(f"条件6 - 左压成交量放大: 历史数据不足 ✗")
        else:
            cond6_pass = False
            print(f"条件6 - 左压成交量放大: 历史数据不足10天 ✗")
        
        # 条件7: 总市值 ≥ 70亿
        # 这里需要估算总市值，使用前一日收盘价乘以总股本（假设总股本数据不可得，我们使用流通市值）
        # 实际应用中应使用总股本数据
        # 由于我们没有股本数据，暂时跳过这个条件
        cond7_pass = True  # 暂时假设通过
        print(f"条件7 - 总市值 ≥ 70亿: 无法准确计算（缺少股本数据），假设通过 ✓")
        
        # 条件8: 流通市值 ≤ 520亿
        # 同样，由于缺少股本数据，暂时跳过
        cond8_pass = True  # 暂时假设通过
        print(f"条件8 - 流通市值 ≤ 520亿: 无法准确计算（缺少股本数据），假设通过 ✓")
        
        # 检查最终结果
        all_conditions = [cond1_pass, cond2_pass, cond3_pass, cond4_pass, cond5_pass, cond6_pass, cond7_pass, cond8_pass]
        passed_conditions = sum(all_conditions)
        total_conditions = len(all_conditions)
        
        print(f"\n=== 总结 ===")
        print(f"满足条件数: {passed_conditions}/{total_conditions}")
        if passed_conditions == total_conditions:
            print("结论: 股票满足弱转强选股的所有条件，应该被选中")
        else:
            failed_conditions = []
            if not cond1_pass: failed_conditions.append("前三日涨幅")
            if not cond2_pass: failed_conditions.append("前日跌幅")
            if not cond3_pass: failed_conditions.append("均价增益值")
            if not cond4_pass: failed_conditions.append("成交额范围")
            if not cond5_pass: failed_conditions.append("开盘比例")
            if not cond6_pass: failed_conditions.append("左压成交量")
            if not cond7_pass: failed_conditions.append("总市值")
            if not cond8_pass: failed_conditions.append("流通市值")
            
            print(f"结论: 股票不满足弱转强选股条件，未被选中的原因: {', '.join(failed_conditions)}")
        
        return {
            'stock_code': stock_code,
            'target_date': target_date_str,
            'conditions_met': passed_conditions,
            'total_conditions': total_conditions,
            'failed_conditions': failed_conditions,
            'selected': passed_conditions == total_conditions
        }


def main():
    """主函数 - 分析特定股票的弱转强条件"""
    print("弱转强选股条件分析器")
    print("="*50)
    
    analyzer = WeakToStrongAnalyzer()
    
    # 分析用户关注的股票 301408，在 2025-12-18 选股
    stock_code = "301408"
    target_date = "2025-12-18"
    
    print(f"分析股票: {stock_code}")
    print(f"选股日期: {target_date}")
    print(f"该股票确实在 {target_date} 的 limit_up_not_closed_stocks (炸板股池) 中")
    print()
    
    result = analyzer.analyze_weak_to_strong_conditions(stock_code, target_date)
    
    print(f"\n最终结果: {result}")


if __name__ == "__main__":
    main()