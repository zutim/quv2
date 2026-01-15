"""
增量数据下载脚本
用于下载新日期的股票数据，避免重复下载
"""

import akshare as ak
import pandas as pd
import numpy as np
import os
import sqlite3
from datetime import datetime, timedelta
import time
import warnings
import concurrent.futures
warnings.filterwarnings('ignore')

class IncrementalDataDownloader:
    def __init__(self, data_dir="full_stock_data", max_workers=10):
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "stock_data.db")
        self.max_workers = max_workers  # 多线程并发数
        self.ensure_directories()
        self.init_database()
    
    def ensure_directories(self):
        """确保数据目录存在"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        if not os.path.exists(os.path.join(self.data_dir, "daily_data")):
            os.makedirs(os.path.join(self.data_dir, "daily_data"))
    
    def init_database(self):
        """初始化SQLite数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建股票列表表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_list (
                code TEXT PRIMARY KEY,
                name TEXT,
                listing_date TEXT,
                update_time TEXT
            )
        ''')
        
        # 创建数据更新记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_update_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT,
                last_update_date TEXT,
                status TEXT,
                message TEXT,
                update_time TEXT
            )
        ''')
        
        # 创建下载记录表，记录每只股票的下载日期范围
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS download_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT,
                start_date TEXT,
                end_date TEXT,
                download_time TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_all_a_stocks(self):
        """获取所有A股股票列表"""
        print("正在获取所有A股股票列表...")
        try:
            # 获取所有A股股票信息
            stock_info = ak.stock_info_a_code_name()
            
            # 过滤掉B股（以9开头的上海B股和以2开头的深圳B股）
            filtered_stocks = []
            for _, row in stock_info.iterrows():
                code = str(row['code'])
                # 过滤B股和其它非A股
                if not (code.startswith('9') or code.startswith('2')):  # B股
                    # 转换为聚宽格式
                    code_str = code.zfill(6)
                    if code_str.startswith('60'):
                        jq_code = f"{code_str}.XSHG"
                    else:
                        jq_code = f"{code_str}.XSHE"
                    filtered_stocks.append({
                        'code': jq_code,
                        'name': row['name']
                    })
            
            print(f"获取到 {len(filtered_stocks)} 只A股股票")
            return filtered_stocks
            
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return []
    
    def get_existing_stock_data_info(self, stock_code):
        """获取已有股票数据的信息（最早和最晚日期）"""
        ak_code = stock_code.replace('.XSHG', '').replace('.XSHE', '')
        csv_path = os.path.join(self.data_dir, "daily_data", f"{ak_code}.csv")
        
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                if 'date' in df.columns and not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    earliest_date = df['date'].min()  # 保持为pandas Timestamp类型
                    latest_date = df['date'].max()    # 保持为pandas Timestamp类型
                    return earliest_date, latest_date
            except Exception as e:
                print(f"读取 {stock_code} 数据失败: {e}")
        
        return None, None
    
    def update_single_stock_data(self, stock_info, days=30):
        """更新单只股票数据 - 用于多线程"""
        code = stock_info['code']
        ak_code = code.replace('.XSHG', '').replace('.XSHE', '')
        
        # 获取现有数据的日期范围
        existing_start, existing_end = self.get_existing_stock_data_info(code)
        
        if existing_end is not None:
            # 将现有结束日期转换为字符串格式
            # 处理不同的日期类型
            if hasattr(existing_end, 'strftime'):
                # 如果是pandas Timestamp或datetime对象
                existing_end_str = existing_end.strftime('%Y-%m-%d')
            elif isinstance(existing_end, str):
                existing_end_str = existing_end
            else:
                # 其他情况，尝试转换为字符串
                existing_end_str = str(existing_end)
            
            # 从现有数据的最后一天开始下载新数据
            existing_end_dt = datetime.strptime(existing_end_str, '%Y-%m-%d')
            start_date = (existing_end_dt + timedelta(days=1)).strftime('%Y%m%d')
        else:
            # 如果没有现有数据，下载最近的数据
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        end_date = datetime.now().strftime('%Y%m%d')
        
        try:
            # 获取日线数据
            df = ak.stock_zh_a_hist(
                symbol=ak_code, 
                period="daily", 
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if df is not None and not df.empty:
                # 重命名列
                df.rename(columns={
                    '开盘': 'open', 
                    '收盘': 'close', 
                    '最高': 'high', 
                    '最低': 'low', 
                    '成交量': 'volume',
                    '成交额': 'amount',
                    '日期': 'date'
                }, inplace=True)
                
                # 确保新数据的日期列是datetime类型
                df['date'] = pd.to_datetime(df['date'])
                
                csv_path = os.path.join(self.data_dir, "daily_data", f"{ak_code}.csv")
                
                if os.path.exists(csv_path):
                    # 如果文件存在，读取现有数据并合并
                    existing_df = pd.read_csv(csv_path)
                    # 确保现有数据的日期列是datetime类型
                    if 'date' in existing_df.columns:
                        existing_df['date'] = pd.to_datetime(existing_df['date'])
                    
                    # 确保两个DataFrame具有相同的列顺序和名称
                    # 获取所有唯一列名并标准化顺序
                    all_columns = set(df.columns.tolist() + existing_df.columns.tolist())
                    all_columns = sorted(list(all_columns))  # 排序以确保一致性
                    
                    # 为每个DataFrame重新索引以确保有相同列
                    for col in all_columns:
                        if col not in df.columns:
                            df[col] = np.nan
                        if col not in existing_df.columns:
                            existing_df[col] = np.nan
                    
                    # 重新排列列顺序
                    df = df.reindex(columns=all_columns)
                    existing_df = existing_df.reindex(columns=all_columns)
                    
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    # 去重并按日期排序 - 确保日期类型一致
                    combined_df = combined_df.drop_duplicates(subset=['date']).sort_values('date')
                    combined_df.to_csv(csv_path, index=False)
                    action = f"更新 {len(df)} 条新数据"
                else:
                    # 如果文件不存在，直接保存
                    df.to_csv(csv_path, index=False)
                    action = f"新增 {len(df)} 条数据"
                
                # 记录下载记录
                self.log_download_record(code, start_date, end_date)
                
                # 返回结果供多线程处理
                result_df = pd.read_csv(csv_path)
                print(f"✓ {code} ({ak_code}): {action}, 总计 {len(result_df)} 条数据")
                return True, code, action, len(result_df)
            else:
                print(f"✗ {code}: 无新数据可下载")
                return False, code, "无新数据", 0
                
        except Exception as e:
            print(f"✗ {code} 更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False, code, f"更新失败: {e}", 0
    
    def log_download_record(self, code, start_date, end_date):
        """记录下载记录"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO download_records (code, start_date, end_date, download_time)
            VALUES (?, ?, ?, ?)
        ''', (code, start_date, end_date, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        conn.close()
    
    def get_missing_stocks(self):
        """获取还没有数据的股票"""
        all_stocks = self.get_all_a_stocks()
        daily_data_dir = os.path.join(self.data_dir, "daily_data")
        
        if not os.path.exists(daily_data_dir):
            return all_stocks
        
        existing_files = set(f.replace('.csv', '') for f in os.listdir(daily_data_dir) if f.endswith('.csv'))
        missing_stocks = []
        
        for stock in all_stocks:
            ak_code = stock['code'].replace('.XSHG', '').replace('.XSHE', '')
            if ak_code not in existing_files:
                missing_stocks.append(stock)
        
        return missing_stocks
    
    def update_all_stocks_parallel(self, days=30):
        """使用多线程并行更新所有股票的最新数据"""
        print("开始并行增量更新所有股票数据...")
        
        # 获取还没有数据的股票（新股票）
        missing_stocks = self.get_missing_stocks()
        print(f"发现 {len(missing_stocks)} 只新股票需要下载完整数据")
        
        # 获取已有数据的股票，进行增量更新
        all_stocks = self.get_all_a_stocks()
        daily_data_dir = os.path.join(self.data_dir, "daily_data")
        
        existing_files = set(f.replace('.csv', '') for f in os.listdir(daily_data_dir) if f.endswith('.csv'))
        existing_stocks = []
        
        for stock in all_stocks:
            ak_code = stock['code'].replace('.XSHG', '').replace('.XSHE', '')
            if ak_code in existing_files:
                existing_stocks.append(stock)
        
        print(f"发现 {len(existing_stocks)} 只已有数据的股票需要增量更新")
        
        total_success = 0
        total_fail = 0
        
        # 先处理新股票 - 使用多线程
        if missing_stocks:
            print(f"\n正在并行下载 {len(missing_stocks)} 只新股票的完整数据...")
            start_time = time.time()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交任务
                future_to_stock = {
                    executor.submit(self.update_single_stock_data, stock, 365): stock 
                    for stock in missing_stocks
                }
                
                # 处理结果
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        success, code, action, count = future.result()
                        if success:
                            total_success += 1
                        else:
                            total_fail += 1
                    except Exception as e:
                        print(f"处理股票 {stock['code']} 时出错: {e}")
                        total_fail += 1
            
            elapsed_time = time.time() - start_time
            print(f"新股票下载完成，耗时: {elapsed_time:.2f}秒")
        
        # 再处理已有数据的股票 - 使用多线程
        if existing_stocks:
            print(f"\n正在并行增量更新 {len(existing_stocks)} 只已有数据的股票...")
            start_time = time.time()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交任务
                future_to_stock = {
                    executor.submit(self.update_single_stock_data, stock, days): stock 
                    for stock in existing_stocks
                }
                
                # 处理结果
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        success, code, action, count = future.result()
                        if success:
                            total_success += 1
                        else:
                            total_fail += 1
                    except Exception as e:
                        print(f"处理股票 {stock['code']} 时出错: {e}")
                        total_fail += 1
            
            elapsed_time = time.time() - start_time
            print(f"已有股票更新完成，耗时: {elapsed_time:.2f}秒")
        
        print(f"\n并行增量更新完成!")
        print(f"成功: {total_success} 只")
        print(f"失败: {total_fail} 只")
        
        return total_success, total_fail

def main():
    """主函数"""
    print("增量股票数据下载工具")
    print("="*50)
    print("此工具将:")
    print("- 下载新股票的完整历史数据")
    print("- 为已有数据的股票增量添加最新数据")
    print("- 避免重复下载已有数据")
    
    # 询问更新天数
    try:
        days = int(input("请输入要更新的天数 (默认30天): ") or "30")
    except ValueError:
        days = 30
    
    # 询问线程数
    try:
        max_workers = int(input("请输入并发线程数 (默认10): ") or "10")
    except ValueError:
        max_workers = 10
    
    print(f"\n开始增量更新，更新最近 {days} 天的数据...")
    print(f"使用 {max_workers} 个并发线程进行并行处理...")
    
    downloader = IncrementalDataDownloader(max_workers=max_workers)
    
    success_count, fail_count = downloader.update_all_stocks_parallel(days=days)
    
    print(f"\n任务完成！")
    print(f"成功更新: {success_count} 只股票")
    print(f"更新失败: {fail_count} 只股票")
    print(f"数据已保存到: {downloader.data_dir}/daily_data/")

if __name__ == "__main__":
    main()
