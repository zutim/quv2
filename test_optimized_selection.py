#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试优化版本的选股功能
"""

import json
import os
import time
from datetime import datetime

def test_optimized_selection():
    """测试优化版本的选股功能"""
    import sys
    # 添加项目路径
    project_root = os.path.dirname(__file__)
    selection_path = os.path.join(project_root, 'selection')
    if selection_path not in sys.path:
        sys.path.insert(0, selection_path)
    
    print("开始测试优化版本的选股功能...")
    
    # 测试日期
    test_date = "2026-01-12"
    
    # 加载pool数据
    pool_file_path = os.path.join(project_root, 'full_stock_data', 'pool_data', f'pool_{test_date}.json')
    
    if not os.path.exists(pool_file_path):
        print(f"错误: 未找到{test_date}的pool数据文件: {pool_file_path}")
        return
    
    with open(pool_file_path, 'r', encoding='utf-8') as f:
        pool_data = json.load(f)
    
    print(f"已加载pool数据，包含 {len(pool_data.get('first_board_stocks', []))} 只首板股票")
    
    # 导入优化版本
    try:
        import select_2026_01_12_optimized
        selector = select_2026_01_12_optimized.TodayStockSelectorOptimized()
        
        start_time = time.time()
        results = selector.select_stocks_from_pool(test_date, pool_data)
        execution_time = time.time() - start_time
        
        print(f"\n优化版本选股完成!")
        print(f"执行时间: {execution_time:.2f}秒")
        print(f"选出股票数: {len(results)}")
        
        # 统计各策略数量
        sbgk_count = sum(1 for r in results if r['strategy'] == 'First Board High Open')
        sbdk_count = sum(1 for r in results if r['strategy'] == 'First Board Low Open')
        rzq_count = sum(1 for r in results if r['strategy'] == 'Weak to Strong')
        
        print(f"首板高开: {sbgk_count} 只")
        print(f"首板低开: {sbdk_count} 只")
        print(f"弱转强: {rzq_count} 只")
        
        if results:
            print("\n选出的股票:")
            for i, stock in enumerate(results, 1):
                print(f"  {i}. {stock['code']}: {stock['name']} ({stock['strategy']})")
        else:
            print("\n未选出任何股票")
        
        print(f"\n优化版本测试完成，耗时: {execution_time:.2f}秒")
        
    except Exception as e:
        print(f"优化版本测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_optimized_selection()