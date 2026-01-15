"""
量化策略回测模块
用于验证选股策略的历史表现
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sqlite3
import warnings
warnings.filterwarnings('ignore')

class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, initial_capital=1000000, commission_rate=0.001):
        """
        初始化回测引擎
        :param initial_capital: 初始资金
        :param commission_rate: 手续费率
        """
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {}  # 持仓 {stock_code: {'shares': int, 'avg_cost': float}}
        self.commission_rate = commission_rate
        self.trade_log = []  # 交易记录
        self.portfolio_history = []  # 组合历史
        self.current_date = None
        
    def buy(self, stock_code, shares, price):
        """买入股票"""
        cost = shares * price
        commission = cost * self.commission_rate
        total_cost = cost + commission
        
        if self.cash >= total_cost:
            self.cash -= total_cost
            
            if stock_code in self.positions:
                # 加仓：计算平均成本
                old_shares = self.positions[stock_code]['shares']
                old_cost = self.positions[stock_code]['avg_cost']
                new_shares = old_shares + shares
                new_avg_cost = (old_shares * old_cost + shares * price) / new_shares
                self.positions[stock_code] = {'shares': new_shares, 'avg_cost': new_avg_cost}
            else:
                # 新建仓位
                self.positions[stock_code] = {'shares': shares, 'avg_cost': price}
            
            # 记录交易
            self.trade_log.append({
                'date': self.current_date,
                'stock': stock_code,
                'action': 'BUY',
                'shares': shares,
                'price': price,
                'commission': commission,
                'total_cost': total_cost
            })
            
            return True
        return False
    
    def sell(self, stock_code, shares, price):
        """卖出股票"""
        if stock_code in self.positions and self.positions[stock_code]['shares'] >= shares:
            revenue = shares * price
            commission = revenue * self.commission_rate
            net_revenue = revenue - commission
            
            self.cash += net_revenue
            
            # 更新持仓
            old_shares = self.positions[stock_code]['shares']
            old_avg_cost = self.positions[stock_code]['avg_cost']
            
            if old_shares == shares:
                # 全部卖出，清空仓位
                del self.positions[stock_code]
            else:
                # 部分卖出，更新份额
                remaining_shares = old_shares - shares
                self.positions[stock_code]['shares'] = remaining_shares
            
            # 记录交易
            self.trade_log.append({
                'date': self.current_date,
                'stock': stock_code,
                'action': 'SELL',
                'shares': shares,
                'price': price,
                'commission': commission,
                'net_revenue': net_revenue,
                'profit': (price - old_avg_cost) * shares if 'old_avg_cost' in locals() else 0
            })
            
            return True
        return False
    
    def get_portfolio_value(self, current_prices):
        """获取当前组合总价值"""
        value = self.cash
        for stock_code, pos in self.positions.items():
            if stock_code in current_prices:
                value += pos['shares'] * current_prices[stock_code]
        return value
    
    def get_position_size(self):
        """获取当前持仓数量"""
        return len(self.positions)


class StrategySimulator:
    """策略模拟器"""
    
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.backtest_engine = BacktestEngine()
        
    def get_daily_data_for_date(self, date_str):
        """获取指定日期的股票数据"""
        stock_list = self.data_manager.get_stock_list()
        daily_data = {}
        
        for _, stock in stock_list.iterrows():
            stock_code = stock['code']
            df = self.data_manager.get_daily_data(stock_code)
            if not df.empty and pd.to_datetime(date_str) in df.index:
                daily_data[stock_code] = df.loc[pd.to_datetime(date_str)]
        
        return daily_data
    
    def select_stocks_for_date(self, date_str):
        """为指定日期选股"""
        # 这里实现您的选股逻辑
        # 简化版：选择当日开盘价低于前一日收盘价的股票（模拟首板低开）
        selected_stocks = []
        
        # 获取当日数据
        daily_data = self.get_daily_data_for_date(date_str)
        
        for stock_code, data in daily_data.items():
            # 获取前一日数据
            df = self.data_manager.get_daily_data(stock_code)
            if df.empty:
                continue
                
            # 找到前一日数据
            current_date = pd.to_datetime(date_str)
            prev_dates = df.index[df.index < current_date]
            if len(prev_dates) < 1:
                continue
                
            prev_data = df.loc[prev_dates[-1]]
            
            # 模拟首板低开条件：开盘价相对于前日收盘价的比率
            if 'close' in prev_data and 'open' in data and prev_data['close'] > 0:
                open_ratio = data['open'] / prev_data['close']
                
                # 简化的首板低开条件：开盘价比前日收盘价低1-3%
                if 0.97 <= open_ratio <= 0.99:
                    selected_stocks.append(stock_code)
        
        return selected_stocks
    
    def run_backtest(self, start_date, end_date):
        """运行回测"""
        print(f"开始回测: {start_date} 到 {end_date}")
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # 生成交易日列表（简化：使用连续日期）
        current_date = start_dt
        trade_days = []
        while current_date <= end_dt:
            if current_date.weekday() < 5:  # 假设周一到周五为交易日
                trade_days.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        results = {
            'dates': [],
            'portfolio_values': [],
            'cash_values': [],
            'selected_stocks': [],
            'trade_counts': []
        }
        
        for date_str in trade_days:
            print(f"回测日期: {date_str}")
            
            self.backtest_engine.current_date = date_str
            
            # 选股
            selected_stocks = self.select_stocks_for_date(date_str)
            print(f"  选股结果: {len(selected_stocks)} 只股票")
            
            # 买入信号股票（资金分配）
            if selected_stocks and self.backtest_engine.cash > 0:
                # 按资金平均分配
                available_cash = self.backtest_engine.cash
                cash_per_stock = available_cash / max(len(selected_stocks), 1) * 0.9  # 保留10%现金
                
                daily_data = self.get_daily_data_for_date(date_str)
                
                for stock_code in selected_stocks:
                    if stock_code in daily_data:
                        current_price = daily_data[stock_code]['open']  # 使用开盘价买入
                        
                        # 计算可买入股数（至少1手100股）
                        max_shares = int(cash_per_stock / current_price)
                        shares_to_buy = (max_shares // 100) * 100  # 取整到100的倍数
                        
                        if shares_to_buy >= 100:
                            success = self.backtest_engine.buy(stock_code, shares_to_buy, current_price)
                            if success:
                                print(f"    买入 {stock_code} {shares_to_buy}股 @ {current_price:.2f}")
            
            # 模拟卖出（简单策略：持有超过3天且盈利则卖出）
            stocks_to_sell = []
            for stock_code, pos in self.backtest_engine.positions.items():
                # 检查当前价格
                if stock_code in daily_data:
                    current_price = daily_data[stock_code]['close']  # 使用收盘价计算
                    avg_cost = pos['avg_cost']
                    
                    # 如果盈利超过3%或持有超过3天，考虑卖出
                    profit_ratio = (current_price - avg_cost) / avg_cost
                    if profit_ratio > 0.03:  # 盈利超过3%则卖出
                        stocks_to_sell.append((stock_code, pos['shares'], current_price))
            
            # 执行卖出
            for stock_code, shares, price in stocks_to_sell:
                success = self.backtest_engine.sell(stock_code, shares, price)
                if success:
                    print(f"    卖出 {stock_code} {shares}股 @ {price:.2f}")
            
            # 记录当前组合价值
            current_prices = {}
            for stock_code in self.backtest_engine.positions.keys():
                if stock_code in daily_data:
                    current_prices[stock_code] = daily_data[stock_code]['close']
            
            portfolio_value = self.backtest_engine.get_portfolio_value(current_prices)
            
            results['dates'].append(date_str)
            results['portfolio_values'].append(portfolio_value)
            results['cash_values'].append(self.backtest_engine.cash)
            results['selected_stocks'].append(len(selected_stocks))
            results['trade_counts'].append(len([t for t in self.backtest_engine.trade_log if t['date'] == date_str]))
            
            print(f"  组合价值: {portfolio_value:.2f}, 现金: {self.backtest_engine.cash:.2f}")
        
        return results
    
    def analyze_results(self, backtest_results):
        """分析回测结果"""
        print("\n=== 回测结果分析 ===")
        
        if not backtest_results['dates']:
            print("没有回测结果")
            return
        
        initial_value = self.backtest_engine.initial_capital
        final_value = backtest_results['portfolio_values'][-1]
        
        total_return = (final_value - initial_value) / initial_value * 100
        total_trades = len(self.backtest_engine.trade_log)
        
        print(f"初始资金: {initial_value:,.2f}")
        print(f"最终价值: {final_value:,.2f}")
        print(f"总收益率: {total_return:.2f}%")
        print(f"总交易次数: {total_trades}")
        print(f"最终持仓数: {self.backtest_engine.get_position_size()}")
        
        # 计算年化收益率（假设252个交易日）
        if len(backtest_results['dates']) > 0:
            days = len(backtest_results['dates'])
            annual_return = ((final_value / initial_value) ** (252 / days) - 1) * 100
            print(f"年化收益率: {annual_return:.2f}%")
        
        # 计算最大回撤
        portfolio_values = backtest_results['portfolio_values']
        if len(portfolio_values) > 1:
            max_value = portfolio_values[0]
            max_drawdown = 0
            for value in portfolio_values:
                if value > max_value:
                    max_value = value
                drawdown = (max_value - value) / max_value * 100
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
            print(f"最大回撤: {max_drawdown:.2f}%")
        
        return {
            'total_return': total_return,
            'annual_return': annual_return if 'annual_return' in locals() else 0,
            'max_drawdown': max_drawdown if 'max_drawdown' in locals() else 0,
            'total_trades': total_trades,
            'final_value': final_value
        }


def main():
    """主函数 - 演示回测功能"""
    print("量化策略回测模块演示")
    print("="*50)
    
    # 首先确保有数据
    from local_data_manager import LocalStockDataManager
    
    data_manager = LocalStockDataManager()
    stock_list = data_manager.get_stock_list()
    
    if stock_list.empty:
        print("本地没有股票数据，请先运行数据获取模块")
        return
    
    print(f"本地股票数量: {len(stock_list)}")
    
    # 创建策略模拟器
    simulator = StrategySimulator(data_manager)
    
    # 运行回测（使用最近的一个月数据作为示例）
    start_date = "2025-12-01"
    end_date = "2025-12-31"
    
    print(f"回测期间: {start_date} 到 {end_date}")
    
    try:
        results = simulator.run_backtest(start_date, end_date)
        analysis = simulator.analyze_results(results)
        
        print(f"\n回测完成！")
        print(f"总收益率: {analysis['total_return']:.2f}%")
        print(f"年化收益率: {analysis['annual_return']:.2f}%")
        print(f"最大回撤: {analysis['max_drawdown']:.2f}%")
        
    except Exception as e:
        print(f"回测过程中出现错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()