"""
快速数据获取脚本
只获取验证策略所需的少量数据
"""

import akshare as ak
import pandas as pd
import numpy as np
import os
import sqlite3
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class QuickDataFetcher:
    def __init__(self, data_dir="stock_data"):
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "stock_data.db")
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
        
        # 创建更新记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS update_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT,
                data_type TEXT,
                update_date TEXT,
                status TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_sample_stocks(self):
        """获取样本股票列表"""
        # 选择一些活跃的股票作为样本
        sample_codes = [
            '000001', '000002', '600000', '600036', 
            '000858', '002594', '000860', '600196',
            '000651', '000895', '002415', '600519'
        ]
        
        # 转换为聚宽格式
        jq_format_stocks = []
        for code in sample_codes:
            if code.startswith('60'):
                jq_code = f"{code}.XSHG"
            else:
                jq_code = f"{code}.XSHE"
            
            # 获取股票名称
            try:
                info = ak.stock_individual_info_em(symbol=code)
                name = info[info['item'] == '股票名称']['value'].iloc[0] if '股票名称' in info['item'].values else "未知"
            except:
                name = "未知"
            
            jq_format_stocks.append({'code': jq_code, 'name': name})
        
        # 存储到数据库
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for stock in jq_format_stocks:
            cursor.execute('''
                INSERT OR REPLACE INTO stock_list 
                (code, name, listing_date, update_time) 
                VALUES (?, ?, ?, ?)
            ''', (
                stock['code'], 
                stock['name'], 
                self.get_listing_date(stock['code'].replace('.XSHG', '').replace('.XSHE', '')), 
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
        
        conn.commit()
        conn.close()
        
        return jq_format_stocks
    
    def get_listing_date(self, ak_code):
        """获取股票上市日期"""
        try:
            info = ak.stock_individual_info_em(symbol=ak_code)
            listing_date_row = info[info['item'] == '上市日期']
            if not listing_date_row.empty:
                return listing_date_row['value'].iloc[0]
        except:
            pass
        return None
    
    def fetch_daily_data(self, stock_code, days=200):
        """获取单只股票的日线数据"""
        ak_code = stock_code.replace('.XSHG', '').replace('.XSHE', '')
        
        try:
            # 获取日线数据
            df = ak.stock_zh_a_hist(
                symbol=ak_code, 
                period="daily", 
                start_date=(datetime.now() - timedelta(days=days)).strftime('%Y%m%d'),
                end_date=datetime.now().strftime('%Y%m%d'),
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
                
                # 确保目录存在
                daily_dir = os.path.join(self.data_dir, "daily_data")
                
                # 保存为CSV
                csv_path = os.path.join(daily_dir, f"{ak_code}.csv")
                df.to_csv(csv_path, index=False)
                
                # 记录更新日志
                self.log_update(stock_code, 'daily', datetime.now().strftime('%Y-%m-%d'))
                
                print(f"成功存储 {stock_code} 的日线数据 ({len(df)} 条记录)")
                return True
            else:
                print(f"获取 {stock_code} 日线数据为空")
                return False
                
        except Exception as e:
            print(f"获取 {stock_code} 日线数据失败: {e}")
            return False
    
    def log_update(self, code, data_type, update_date, status="success"):
        """记录更新日志"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO update_log (code, data_type, update_date, status)
            VALUES (?, ?, ?, ?)
        ''', (code, data_type, update_date, status))
        conn.commit()
        conn.close()
    
    def get_stock_list(self):
        """从本地数据库获取股票列表"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM stock_list", conn)
        conn.close()
        return df
    
    def get_daily_data(self, stock_code):
        """从本地获取日线数据"""
        ak_code = stock_code.replace('.XSHG', '').replace('.XSHE', '')
        csv_path = os.path.join(self.data_dir, "daily_data", f"{ak_code}.csv")
        
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            return df
        else:
            return pd.DataFrame()
    
    def fetch_all_sample_data(self):
        """获取所有样本数据"""
        print("获取样本股票列表...")
        sample_stocks = self.get_sample_stocks()
        
        print(f"获取 {len(sample_stocks)} 只样本股票的日线数据...")
        success_count = 0
        
        for i, stock in enumerate(sample_stocks):
            code = stock['code']
            print(f"获取 {code} 数据 ({i+1}/{len(sample_stocks)})")
            
            if self.fetch_daily_data(code):
                success_count += 1
        
        print(f"数据获取完成! 成功: {success_count}/{len(sample_stocks)}")


def main():
    """主函数"""
    print("快速数据获取脚本")
    print("="*30)
    
    fetcher = QuickDataFetcher()
    fetcher.fetch_all_sample_data()
    
    print("\n数据获取完成！")
    print("现在可以使用本地数据进行选股验证了")


if __name__ == "__main__":
    main()