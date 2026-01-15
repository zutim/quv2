#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def debug_weak_to_strong_conditions():
    # 加载股票池数据
    pool_file = 'full_stock_data/pool_data/pool_2025-12-19.json'
    with open(pool_file, 'r', encoding='utf-8') as f:
        pool_data = json.load(f)

    print('Pool data loaded.')
    print(f'000917 in limit_up_not_closed_stocks:', '000917' in pool_data['limit_up_not_closed_stocks'])
    
    if '000917' not in pool_data['limit_up_not_closed_stocks']:
        print('000917 is not in the limit_up_not_closed_stocks list')
        return

    target_date_str = '2025-12-19'
    stock_code = '000917'
    
    print(f'\\nAnalyzing {stock_code} for weak-to-strong strategy on {target_date_str}')
    
    # 读取股票数据
    data_path = os.path.join('full_stock_data', 'daily_data')
    csv_file = os.path.join(data_path, f'{stock_code}.csv')
    
    if not os.path.exists(csv_file):
        print(f'Stock file {csv_file} does not exist')
        return
    
    df = pd.read_csv(csv_file)
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
    
    # 查找目标日期的数据
    day_data = df[df['date'] == pd.to_datetime(target_date_str)]
    if day_data.empty:
        print(f'No data found for {stock_code} on {target_date_str}')
        # 找到最接近的日期
        closest_date = df[df['date'] <= pd.to_datetime(target_date_str)]
        if not closest_date.empty:
            closest_date = closest_date.tail(1)
            print(f'Closest date data: {closest_date.iloc[0]["date"].strftime("%Y-%m-%d")}')
            day_data = closest_date
        else:
            print('No historical data found')
            return
    
    day_data = day_data.iloc[0]
    current_close = float(day_data.get('close', 0)) if pd.notna(day_data.get('close', 0)) else 0
    current_open = float(day_data.get('open', 0)) if pd.notna(day_data.get('open', 0)) else 0
    current_high = float(day_data.get('high', 0)) if pd.notna(day_data.get('high', 0)) else 0
    current_low = float(day_data.get('low', 0)) if pd.notna(day_data.get('low', 0)) else 0
    current_volume = float(day_data.get('volume', 0)) if pd.notna(day_data.get('volume', 0)) else 0
    current_amount = float(day_data.get('amount', 0)) if pd.notna(day_data.get('amount', 0)) else 0
    
    print(f'Current day ({target_date_str}) data:')
    print(f'  Open: {current_open}, Close: {current_close}, High: {current_high}, Low: {current_low}')
    print(f'  Volume: {current_volume}, Amount: {current_amount}')
    
    # 获取前一个交易日的数据（2025-12-18，涨停日）
    prev_day_data = df[df['date'] < pd.to_datetime(target_date_str)].tail(1)
    if prev_day_data.empty:
        print('No previous day data found')
        return
    
    prev_day_data = prev_day_data.iloc[0]
    prev_close = float(prev_day_data['close']) if pd.notna(prev_day_data['close']) else 0
    prev_open = float(prev_day_data['open']) if pd.notna(prev_day_data['open']) else 0
    prev_high = float(prev_day_data['high']) if pd.notna(prev_day_data['high']) else 0
    prev_low = float(prev_day_data['low']) if pd.notna(prev_day_data['low']) else 0
    prev_volume = float(prev_day_data['volume']) if pd.notna(prev_day_data['volume']) else 1  # 避免除零
    prev_amount = float(prev_day_data['amount']) if pd.notna(prev_day_data['amount']) else 0
    
    print(f'Previous day data ({prev_day_data["date"].strftime("%Y-%m-%d")}):')
    print(f'  Open: {prev_open}, Close: {prev_close}, High: {prev_high}, Low: {prev_low}')
    print(f'  Volume: {prev_volume}, Amount: {prev_amount}')
    
    # 检查是否曾涨停未封板（即最高价触及涨停价但收盘未封住）
    is_st = 'ST' in stock_code or 'st' in stock_code
    limit_ratio = 0.05 if is_st else 0.1
    expected_limit_price = round(prev_close * (1 + limit_ratio), 2)
    print(f'Expected limit price based on prev close {prev_close}: {expected_limit_price}')
    print(f'Prev day high: {prev_high}, close: {prev_close}')
    print(f'Was ever limit up (high reached limit): {abs(prev_high - expected_limit_price) <= 0.02}')
    print(f'Was closed limit up (close reached limit): {abs(prev_close - expected_limit_price) <= 0.02}')
    
    # 计算均价
    if prev_volume != 0 and prev_amount != 0:
        avg_price = prev_amount / prev_volume
        avg_price_increase_value = avg_price / prev_close * 1.1 - 1
        print(f'Average price calculation: {prev_amount} / {prev_volume} = {avg_price}')
        print(f'Average price increase value: {avg_price} / {prev_close} * 1.1 - 1 = {avg_price_increase_value}')
    else:
        avg_price_increase_value = 0
        print('Could not calculate average price (volume or amount is 0)')
    
    # 弱转强策略条件检查
    print(f'\\nWeak-to-Strong Strategy Conditions Check for {stock_code}:')
    
    # 条件1: 均价获利 >= -4%, 成交额在3-19亿之间
    condition1_part1 = avg_price_increase_value >= -0.04  # 均价涨幅 >= -4%
    condition1_part2 = 3e8 <= prev_amount <= 19e8  # 成交额在3亿-19亿之间
    condition1 = condition1_part1 and condition1_part2
    print(f'  Condition 1: Avg price increase >= -4% AND 3e8 <= amount <= 19e8')
    print(f'    Avg price increase >= -4%: {condition1_part1} ({avg_price_increase_value:.3f} >= -0.04)')
    print(f'    3e8 <= amount <= 19e8: {condition1_part2} ({prev_amount/1e8:.2f}亿 in [3, 19])')
    print(f'    Condition 1 overall: {condition1}')
    
    # 条件2: 前3日涨幅 <= 28%
    recent_df = df[df['date'] <= pd.to_datetime(prev_day_data['date'])].tail(4)
    if len(recent_df) >= 4:
        past_4_close = [float(row['close']) if pd.notna(row['close']) else 0 for _, row in recent_df.iterrows()]
        if len(past_4_close) >= 4 and all(c > 0 for c in past_4_close):
            increase_ratio = (past_4_close[-1] - past_4_close[0]) / past_4_close[0]
            condition2 = increase_ratio <= 0.28  # 前3日涨幅 <= 28%
            print(f'  Condition 2: 3-day increase <= 28%')
            print(f'    Past 4 closes: {past_4_close}')
            print(f'    3-day increase: {(past_4_close[-1] - past_4_close[0]) / past_4_close[0]:.3f} <= 0.28: {condition2}')
        else:
            condition2 = False
            print(f'  Condition 2: Cannot calculate (insufficient or invalid close data)')
    else:
        condition2 = False
        print(f'  Condition 2: Cannot calculate (insufficient historical data)')
    
    # 条件3: 前一日跌幅 >= -5%
    if prev_open != 0:
        open_close_ratio = (prev_close - prev_open) / prev_open if prev_open != 0 else 0
        condition3 = open_close_ratio >= -0.05  # 前一日跌幅 >= -5%
        print(f'  Condition 3: Prev day drop >= -5%')
        print(f'    Prev open: {prev_open}, close: {prev_close}')
        print(f'    Open-close ratio: {open_close_ratio:.3f} >= -0.05: {condition3}')
    else:
        condition3 = False
        print(f'  Condition 3: Cannot calculate (prev open is 0)')
    
    # 条件4: 竞价在合理范围（相对于涨停价）
    # 模拟竞价数据
    # 这里我们模拟一个竞价价格，通常竞价价格接近开盘价
    simulated_auction_price = current_open if current_open > 0 else prev_close * 1.005  # 假设竞价略高于前收盘
    expected_limit_up = prev_close * 1.1
    current_ratio_to_limit = simulated_auction_price / expected_limit_up if expected_limit_up != 0 else 0
    condition4 = 0.98 <= current_ratio_to_limit <= 1.09  # -2% 到 +9%
    print(f'  Condition 4: Auction price relative to limit price in [0.98, 1.09]')
    print(f'    Simulated auction price: {simulated_auction_price:.3f}')
    print(f'    Expected limit up: {expected_limit_up:.3f}')
    print(f'    Ratio: {current_ratio_to_limit:.3f} in [0.98, 1.09]: {condition4}')
    
    # 条件5: 成交量占比 >= 3%
    # 模拟竞价成交量，假设是前一天成交量的某个比例
    simulated_auction_volume = prev_volume * 0.05  # 假设竞价成交量是前一天的5%
    volume_condition = simulated_auction_volume / prev_volume >= 0.03 if prev_volume > 0 else False
    condition5 = volume_condition
    print(f'  Condition 5: Auction volume ratio >= 3%')
    print(f'    Simulated auction volume: {simulated_auction_volume:.0f}')
    print(f'    Prev volume: {prev_volume:.0f}')
    print(f'    Volume ratio: {simulated_auction_volume / prev_volume:.3f} >= 0.03: {condition5}')
    
    # 条件6: 市值条件（总市值 >= 70亿，流通市值 <= 520亿）
    # 这里简单模拟市值，实际需要从市值管理器获取
    # 假设总市值和流通市值
    total_market_cap = 100  # 100亿
    circulating_market_cap = 300  # 300亿
    condition6 = total_market_cap >= 70 and circulating_market_cap <= 520
    print(f'  Condition 6: Market cap >= 70 AND Circulating <= 520')
    print(f'    Total market cap: {total_market_cap} >= 70: {total_market_cap >= 70}')
    print(f'    Circulating market cap: {circulating_market_cap} <= 520: {circulating_market_cap <= 520}')
    print(f'    Condition 6 overall: {condition6}')
    
    # 条件7: 左压条件
    hst_data = df[df['date'] <= pd.to_datetime(prev_day_data['date'])]
    if len(hst_data) >= 2:
        hst = hst_data.tail(101) if len(hst_data) >= 101 else hst_data
        if len(hst) >= 2:
            # 获取前一日的高点
            actual_prev_high = float(hst.iloc[-1]['high']) if pd.notna(hst.iloc[-1]['high']) else 0
            
            # 计算zyts_0
            zyts_0 = 100  # 默认值
            for i, (_, row) in enumerate(hst['high'][-3::-1].items(), 2):
                high = float(row) if pd.notna(row) else 0
                if high >= actual_prev_high:
                    zyts_0 = i - 1
                    break
            
            zyts = zyts_0 + 5
            # 获取高点以来的成交量数据
            volume_data = hst['volume'].tail(zyts) if len(hst) >= zyts else hst['volume']
            
            if len(volume_data) >= 2:
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
    
    condition7 = left_pressure_condition
    print(f'  Condition 7: Left pressure condition')
    print(f'    ZYTS_0: {zyts_0}, ZYTS: {zyts}')
    print(f'    Prev volume: {current_vol:.0f}, Max previous volume: {max_prev_vol:.0f}')
    print(f'    Left pressure condition: {left_pressure_condition}')
    
    # 总体评估
    all_conditions = condition1 and condition2 and condition3 and condition4 and condition5 and condition6 and condition7
    print(f'\\nOverall Weak-to-Strong Result: {all_conditions}')
    print(f'All conditions: Cond1({condition1}) AND Cond2({condition2}) AND Cond3({condition3}) AND Cond4({condition4}) AND Cond5({condition5}) AND Cond6({condition6}) AND Cond7({condition7})')
    
    failed_conditions = []
    if not condition1: failed_conditions.append("Condition 1 (Avg price/Amount)")
    if not condition2: failed_conditions.append("Condition 2 (3-day increase)")
    if not condition3: failed_conditions.append("Condition 3 (Prev day drop)")
    if not condition4: failed_conditions.append("Condition 4 (Auction ratio)")
    if not condition5: failed_conditions.append("Condition 5 (Volume ratio)")
    if not condition6: failed_conditions.append("Condition 6 (Market cap)")
    if not condition7: failed_conditions.append("Condition 7 (Left pressure)")
    
    if failed_conditions:
        print(f'Failed conditions: {", ".join(failed_conditions)}')
    else:
        print('All conditions passed!')


if __name__ == "__main__":
    debug_weak_to_strong_conditions()