"""
股票池生成器
用于生成每日涨停股票池，避免在选股时实时获取
"""

import akshare as ak
import pandas as pd
import numpy as np
import os
import sqlite3
from datetime import datetime, timedelta
import time
import warnings
import glob
from typing import List, Dict
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')

class StockPoolGenerator:
    def __init__(self, data_dir=None):
        # 如果没有指定data_dir，则使用项目根目录下的full_stock_data
        if data_dir is None:
            # 获取当前文件所在目录（data_processing）的父目录（项目根目录）
            current_file_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_file_dir)
            self.data_dir = os.path.join(project_root, "full_stock_data")
        else:
            self.data_dir = data_dir
        self.daily_data_dir = os.path.join(self.data_dir, "daily_data")
        self.db_path = os.path.join(self.data_dir, "stock_data.db")
        self.pool_data_dir = os.path.join(self.data_dir, "pool_data")  # 新增：股票池数据目录
        
        # 确保目录存在
        if not os.path.exists(self.daily_data_dir):
            os.makedirs(self.daily_data_dir)
        if not os.path.exists(self.pool_data_dir):
            os.makedirs(self.pool_data_dir)
        
        # 缓存数据，避免重复读取
        self._cached_data = {}
        self._all_stocks_data = None
        self._trading_dates_cache = None
    
    def _load_all_stocks_data(self):
        """一次性加载所有股票数据到内存"""
        if self._all_stocks_data is not None:
            return self._all_stocks_data
        
        print("正在一次性加载所有股票数据到内存...")
        start_time = time.time()
        
        csv_files = glob.glob(os.path.join(self.daily_data_dir, "*.csv"))
        all_data = {}
        
        # 预先定义列映射
        column_mappings = {
            '涨\u3000跌\u3000幅': 'pct_change',
            '涨\u3000跌\u3000额': 'change',
            '股\u3000票\u3000代\u3000码': 'code',
            '开\u3000盘': 'open',
            '收\u3000盘': 'close',
            '最\u3000高': 'high',
            '最\u3000低': 'low',
            '成\u3000交\u3000量': 'volume',
            '成\u3000交\u3000额': 'amount',
            '振\u3000幅': 'amplitude',
            '换\u3000手\u3000率': 'turnover'
        }
        
        for file_path in csv_files:
            try:
                df = pd.read_csv(file_path)
                
                # 重命名列
                original_columns = df.columns.tolist()
                rename_dict = {}
                for orig_col in original_columns:
                    for key, val in column_mappings.items():
                        if key in orig_col or orig_col == key:
                            rename_dict[orig_col] = val
                            break
                
                df.rename(columns=rename_dict, inplace=True)
                
                # 转换日期并创建日期索引
                df['date'] = pd.to_datetime(df['date'])
                df.set_index(['date'], inplace=True)
                
                # 获取股票代码
                stock_code = os.path.basename(file_path).replace('.csv', '')
                
                # 存储处理后的数据
                all_data[stock_code] = df
                
            except Exception as e:
                print(f"加载文件 {file_path} 时出错: {e}")
                continue
        
        self._all_stocks_data = all_data
        end_time = time.time()
        print(f"所有股票数据加载完成，耗时: {end_time - start_time:.2f}秒，共加载 {len(all_data)} 只股票")
        return all_data
    
    def _get_trading_dates(self):
        """获取所有交易日期"""
        if self._trading_dates_cache is not None:
            return self._trading_dates_cache
        
        print("正在获取所有交易日期...")
        all_data = self._load_all_stocks_data()
        
        all_dates = set()
        for df in all_data.values():
            # 获取所有日期
            dates = pd.Series(df.index.date).unique()
            all_dates.update(dates)
        
        # 过滤掉NaT值
        all_dates = {d for d in all_dates if pd.notna(d)}
        trading_dates = sorted(list(all_dates), reverse=True)
        self._trading_dates_cache = trading_dates
        return trading_dates
    
    def get_last_trading_date(self, target_date: str) -> str:
        """获取指定日期的前一个交易日"""
        target_dt = datetime.strptime(target_date, '%Y-%m-%d').date()
        trading_dates = self._get_trading_dates()
        
        for date in trading_dates:
            if date < target_dt:
                return date.strftime('%Y-%m-%d')
        return None
    
    def get_limit_up_stocks_from_daily_data(self, date_str: str) -> List[str]:
        """从内存中的数据获取指定日期的涨停股票"""
        date_obj = pd.to_datetime(date_str).date()
        all_data = self._load_all_stocks_data()
        
        limit_up_stocks = []
        
        # 直接访问日期索引，提高查询效率
        target_date = pd.to_datetime(date_str)
        prev_date = None
        
        # 找到前一个交易日
        trading_dates = self._get_trading_dates()
        for i, dt in enumerate(trading_dates):
            if dt == date_obj:
                if i + 1 < len(trading_dates):
                    prev_date = pd.to_datetime(trading_dates[i + 1])
                break
        
        if prev_date is None:
            # 如果没找到，就手动查找前一个日期
            for dt in trading_dates:
                if dt < date_obj:
                    prev_date = pd.to_datetime(dt)
                    break
        
        start_time = time.time()
        processed_count = 0
        
        for stock_code, df in all_data.items():
            try:
                processed_count += 1
                
                # 直接通过索引获取数据
                if target_date not in df.index:
                    continue
                
                day_data = df.loc[target_date]
                
                # 检查是否有前一个交易日数据
                if prev_date is not None and prev_date not in df.index:
                    # 寻找最接近的前一个交易日
                    prev_rows = df[df.index < target_date]
                    if prev_rows.empty:
                        continue
                    prev_row = prev_rows.iloc[-1]  # 最后一行就是最近的前一个交易日
                else:
                    prev_row = df.loc[prev_date]
                
                prev_close = float(prev_row['close'])
                if prev_close == 0:
                    continue
                
                # 获取当日的最高价和收盘价
                current_high = float(day_data['high'])
                current_close = float(day_data['close'])
                
                # 判断是否真正涨停（当日涨幅大于等于9.75%）
                current_pct_change = day_data['pct_change'] if 'pct_change' in day_data else None
                if pd.isna(current_pct_change) or current_pct_change == 0:
                    # 如果pct_change是NaN或0，使用计算公式
                    calculated_change = ((current_close - prev_close) / prev_close) * 100
                    current_pct_change = calculated_change
                else:
                    current_pct_change = float(current_pct_change)
                
                # 根据股票代码判断涨停板比例
                if stock_code.startswith('30'):  # 创业板股票
                    limit_threshold = 19.5
                else:
                    limit_threshold = 9.75
                
                if current_pct_change >= limit_threshold:  # 当日涨幅大于等于阈值
                    limit_up_stocks.append(stock_code)
                else:
                    # 备用判断：收盘价接近涨停价
                    is_st = 'ST' in stock_code or 'st' in stock_code
                    if stock_code.startswith('30'):  # 创业板股票
                        limit_ratio = 0.2
                    elif is_st:  # ST股票
                        limit_ratio = 0.05
                    else:
                        limit_ratio = 0.1
                        
                    limit_price = round(prev_close * (1 + limit_ratio), 2)
                    
                    # 检查收盘价是否接近涨停价（允许0.02元的误差）
                    if abs(current_close - limit_price) <= 0.02:
                        limit_up_stocks.append(stock_code)
                        
            except Exception:
                continue  # 如果处理出错，跳过这只股票
        
        end_time = time.time()
        print(f"涨停股票识别完成，处理了 {processed_count} 只股票，耗时: {end_time - start_time:.2f}秒")
        
        return limit_up_stocks
    
    def get_ever_limit_up_not_closed_stocks_from_daily_data(self, date_str: str) -> List[str]:
        """从内存中的数据获取指定日期的曾涨停未封板股票"""
        date_obj = pd.to_datetime(date_str).date()
        all_data = self._load_all_stocks_data()
        
        limit_up_not_closed_stocks = []
        
        # 直接访问日期索引，提高查询效率
        target_date = pd.to_datetime(date_str)
        prev_date = None
        
        # 找到前一个交易日
        trading_dates = self._get_trading_dates()
        for i, dt in enumerate(trading_dates):
            if dt == date_obj:
                if i + 1 < len(trading_dates):
                    prev_date = pd.to_datetime(trading_dates[i + 1])
                break
        
        if prev_date is None:
            # 如果没找到，就手动查找前一个日期
            for dt in trading_dates:
                if dt < date_obj:
                    prev_date = pd.to_datetime(dt)
                    break
        
        start_time = time.time()
        processed_count = 0
        
        for stock_code, df in all_data.items():
            try:
                processed_count += 1
                
                # 直接通过索引获取数据
                if target_date not in df.index:
                    continue
                
                day_data = df.loc[target_date]
                
                # 检查是否有前一个交易日数据
                if prev_date is not None and prev_date not in df.index:
                    # 寻找最接近的前一个交易日
                    prev_rows = df[df.index < target_date]
                    if prev_rows.empty:
                        continue
                    prev_row = prev_rows.iloc[-1]  # 最后一行就是最近的前一个交易日
                else:
                    prev_row = df.loc[prev_date]
                
                prev_close = float(prev_row['close'])
                if prev_close == 0:
                    continue
                
                # 获取当日的最高价和收盘价
                current_high = float(day_data['high'])
                current_close = float(day_data['close'])
                
                # 判断是否触及涨停价但收盘未封住（曾涨停未封板）
                current_pct_change = day_data['pct_change'] if 'pct_change' in day_data else None
                if pd.isna(current_pct_change) or current_pct_change == 0:
                    # 如果pct_change是NaN或0，使用计算公式
                    calculated_change = ((current_close - prev_close) / prev_close) * 100
                    current_pct_change = calculated_change
                else:
                    current_pct_change = float(current_pct_change)
                
                # 根据股票代码判断涨停板比例
                if stock_code.startswith('30'):  # 创业板股票
                    limit_threshold = 19.5
                else:
                    limit_threshold = 9.75
                
                # 计算涨停价
                is_st = 'ST' in stock_code or 'st' in stock_code
                if stock_code.startswith('30'):  # 创业板股票
                    limit_ratio = 0.2
                elif is_st:  # ST股票
                    limit_ratio = 0.05
                else:
                    limit_ratio = 0.1
                    
                limit_price = round(prev_close * (1 + limit_ratio), 2)
                
                # 判断是否曾涨停未封板
                if (current_pct_change >= limit_threshold and current_close < limit_price) or \
                   (abs(current_high - limit_price) <= 0.02 and current_close < limit_price):
                    limit_up_not_closed_stocks.append(stock_code)
                        
            except Exception:
                continue  # 如果处理出错，跳过这只股票
        
        end_time = time.time()
        print(f"曾涨停未封板股票识别完成，处理了 {processed_count} 只股票，耗时: {end_time - start_time:.2f}秒")
        
        return limit_up_not_closed_stocks
    
    def get_limit_up_stocks_from_api(self, date_str: str) -> List[str]:
        """从API获取涨停股票（备用方法）"""
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
            print(f"从API获取涨停股票数据失败 {date_str}: {e}")
            return []
    
    def generate_stock_pool(self, target_date: str) -> Dict:
        """生成指定日期的股票池"""
        print(f"开始生成 {target_date} 的股票池...")
        
        # 获取前一个交易日
        prev_trading_date = self.get_last_trading_date(target_date)
        print(f"前一个交易日: {prev_trading_date}")
        
        # 从前一个交易日的数据中获取涨停封板股票
        print("正在从内存数据获取前一日涨停封板股票...")
        limit_up_stocks = self.get_limit_up_stocks_from_daily_data(prev_trading_date)
        
        # 获取前前一个交易日（用于判断是否为首板）
        prev_2_trading_date = self.get_last_trading_date(prev_trading_date)
        print(f"前前一个交易日: {prev_2_trading_date}")
        
        limit_up_2_days_ago = self.get_limit_up_stocks_from_daily_data(prev_2_trading_date)
        
        # 计算首板涨停封板股票（昨日涨停封板但前日未涨停）
        first_board_stocks = [stock for stock in limit_up_stocks if stock not in limit_up_2_days_ago]
                
        # 获取曾涨停未封板股票（炸板票）
        limit_up_not_closed_stocks = self.get_ever_limit_up_not_closed_stocks_from_daily_data(prev_trading_date)
        
        # 确保首板股票和炸板股票是互斥的
        # 首板股票是涨停封板的股票，炸板股票是曾触及涨停但未封板的股票
        # 因此它们本质上是不同的，无需额外处理
        
        # 保存股票池数据
        pool_data = {
            'target_date': target_date,
            'prev_trading_date': prev_trading_date,
            'prev_2_trading_date': prev_2_trading_date,
            'limit_up_stocks': limit_up_stocks,
            'limit_up_2_days_ago': limit_up_2_days_ago,
            'first_board_stocks': first_board_stocks,
            'limit_up_not_closed_stocks': limit_up_not_closed_stocks,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 保存到JSON文件
        pool_file = os.path.join(self.pool_data_dir, f"pool_{target_date}.json")
        import json
        with open(pool_file, 'w', encoding='utf-8') as f:
            json.dump(pool_data, f, ensure_ascii=False, indent=2)
        
        print(f"股票池生成完成！")
        print(f"- 前一日涨停封板股票数量: {len(limit_up_stocks)}")
        print(f"- 前前日涨停封板股票数量: {len(limit_up_2_days_ago)}")
        print(f"- 首板涨停封板股票数量: {len(first_board_stocks)}")
        print(f"- 前一日炸板股票数量: {len(limit_up_not_closed_stocks)}")
        print(f"保存路径: {pool_file}")
        
        return pool_data
    
    def load_stock_pool(self, date_str: str) -> Dict:
        """加载指定日期的股票池"""
        pool_file = os.path.join(self.pool_data_dir, f"pool_{date_str}.json")
        if os.path.exists(pool_file):
            import json
            with open(pool_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def batch_generate_stock_pool(self, start_date: str, end_date: str, max_workers: int = 5):
        """批量生成指定时间段的股票池数据"""
        from datetime import datetime, timedelta
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # 生成所有需要处理的日期
        all_dates = []
        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            # 检查该日期是否有对应的交易日数据
            trading_dates = self._get_trading_dates()
            trading_date_obj = current_date.date()
            if trading_date_obj in [dt for dt in trading_dates]:
                all_dates.append(date_str)
            else:
                print(f"  - 跳过 {date_str} (非交易日)")
            
            current_date += timedelta(days=1)
        
        processed_dates = []
        
        print(f"开始批量生成 {start_date} 到 {end_date} 的股票池数据...")
        print(f"需要处理 {len(all_dates)} 个交易日")
        
        def process_single_date(date_str):
            try:
                print(f"正在生成 {date_str} 的股票池...")
                pool_data = self.generate_stock_pool(date_str)
                return date_str, True
            except Exception as e:
                print(f"  - 生成 {date_str} 时出错: {e}")
                return date_str, False
        
        if all_dates:
            print(f"正在并行处理 {len(all_dates)} 个交易日...")
            start_time = time.time()
            
            # 使用线程池并行处理多个日期
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(process_single_date, all_dates))
            
            # 统计成功处理的日期
            for date_str, success in results:
                if success:
                    processed_dates.append(date_str)
            
            end_time = time.time()
            print(f"并行处理完成，耗时: {end_time - start_time:.2f}秒")
        
        print(f"批量生成完成！共处理了 {len(processed_dates)} 个交易日")
        print(f"处理的日期: {processed_dates}")
        return processed_dates

    def get_latest_data_date(self) -> str:
        """获取本地数据的最新日期"""
        latest_date = None
        for file in os.listdir(self.daily_data_dir):
            if file.endswith('.csv'):
                csv_path = os.path.join(self.daily_data_dir, file)
                try:
                    df = pd.read_csv(csv_path)
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                        file_latest = df['date'].max()
                        if latest_date is None or file_latest > latest_date:
                            latest_date = file_latest
                except:
                    continue
        
        if latest_date:
            return latest_date.strftime('%Y-%m-%d')
        return "无数据"

def main():
    """主函数 - 演示股票池生成"""
    print("股票池生成器")
    print("="*50)
    
    generator = StockPoolGenerator()
    
    # 获取当前日期作为目标日期
    target_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"当前日期: {target_date}")
    
    # 获取本地数据最新日期
    latest_data_date = generator.get_latest_data_date()
    print(f"本地数据最新日期: {latest_data_date}")
    
    if latest_data_date == "无数据":
        print("没有找到本地数据，请先下载数据")
        return
    
    # 询问用户操作类型
    print(f"\n请选择操作类型:")
    print("1. 单日生成股票池")
    print("2. 批量生成股票池（指定时间段）")
    
    choice = input("请输入选项 (1 或 2): ")
    
    if choice == "1":
        target_date = input(f"请输入目标日期 (格式 YYYY-MM-DD，默认 {target_date}): ") or target_date
        
        print(f"\n是否要生成 {target_date} 的股票池？")
        print("这将使用前一个交易日的数据来生成涨停股票池")
        
        response = input("输入 'y' 确认生成，其他键退出: ")
        if response.lower() != 'y':
            print("取消生成")
            return
        
        # 生成股票池
        pool_data = generator.generate_stock_pool(target_date)
        
        print(f"\n股票池生成完成！")
        print(f"前一日涨停股票 (共{len(pool_data['limit_up_stocks'])}只): {pool_data['limit_up_stocks'][:10]}...")  # 显示前10只
        print(f"首板股票 (共{len(pool_data['first_board_stocks'])}只): {pool_data['first_board_stocks'][:10]}...")  # 显示前10只
    
    elif choice == "2":
        print("批量生成股票池（会覆盖已有数据）")
        start_date = input("请输入开始日期 (格式 YYYY-MM-DD): ")
        end_date = input("请输入结束日期 (格式 YYYY-MM-DD): ")
        
        if start_date and end_date:
            print(f"\n是否要生成 {start_date} 到 {end_date} 的股票池？")
            print("(这将覆盖已存在的数据)")
            
            response = input("输入 'y' 确认生成，其他键退出: ")
            if response.lower() != 'y':
                print("取消生成")
                return
            
            # 批量生成股票池
            processed_dates = generator.batch_generate_stock_pool(start_date, end_date)
            
            print(f"\n批量生成完成！")
        else:
            print("日期输入不完整")
    
    else:
        print("无效选项")


if __name__ == "__main__":
    main()