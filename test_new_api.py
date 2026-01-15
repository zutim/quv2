#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试新的竞价数据API
"""

import requests
import json
from datetime import datetime

def test_new_api():
    """测试新的竞价数据API"""
    print("测试新的竞价数据API...")
    
    # 测试股票代码
    stock_code = "301408"
    date_str = "2026-01-12"
    
    # 将日期格式从 YYYY-MM-DD 转换为 YYYYMMDD
    target_date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    target_date_numeric = target_date_obj.strftime('%Y%m%d')
    
    # 构造API请求
    url = f"http://localhost:8080/api/trade?code={stock_code}&date={target_date_numeric}"
    
    print(f"请求URL: {url}")
    
    try:
        response = requests.get(url, timeout=10)
        print(f"响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"响应数据: {json.dumps(data, indent=2, ensure_ascii=False)}")
            
            if 'auction_data' in data:
                print("成功获取到竞价数据!")
                auction_data = data['auction_data']
                print(f"竞价时间: {auction_data.get('time', 'N/A')}")
                print(f"竞价价格: {auction_data.get('price', 'N/A')}")
                print(f"竞价成交量: {auction_data.get('volume', 'N/A')}")
            else:
                print("响应中没有auction_data字段")
        else:
            print(f"API请求失败，状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
            
    except Exception as e:
        print(f"请求过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_new_api()