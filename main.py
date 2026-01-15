#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
量化股票分析系统主入口

项目结构：
1. 选股模块 (selection/) - 负责执行选股策略
2. 数据处理模块 (data_processing/) - 负责数据获取和处理
3. 可视化模块 (visualization/) - 负责Web界面和API服务
"""

import os
import sys
import argparse
from datetime import datetime


def run_selection(target_date=None):
    """运行选股模块"""
    if target_date is None:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"=== 运行选股模块 - {target_date} ===")
    selection_path = os.path.join(os.path.dirname(__file__), 'selection')
    
    # 尝试运行主要的选股脚本
    script_path = os.path.join(selection_path, 'select_2026_01_12.py')
    
    if os.path.exists(script_path):
        # 修改脚本以适应传入的日期
        import subprocess
        result = subprocess.run([sys.executable, script_path, target_date], 
                               cwd=selection_path, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("错误:", result.stderr)
    else:
        print(f"选股脚本不存在: {script_path}")


def run_data_processing(action='generate_pool'):
    """运行数据处理模块"""
    print("=== 运行数据处理模块 ===")
    data_proc_path = os.path.join(os.path.dirname(__file__), 'data_processing')
    
    if action == 'generate_pool':
        script_path = os.path.join(data_proc_path, 'stock_pool_generator.py')
        if os.path.exists(script_path):
            import subprocess
            result = subprocess.run([sys.executable, script_path], 
                                   cwd=data_proc_path, capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print("错误:", result.stderr)
        else:
            print(f"股票池生成脚本不存在: {script_path}")
    
    elif action == 'download_data':
        script_path = os.path.join(data_proc_path, 'incremental_download.py')
        if os.path.exists(script_path):
            import subprocess
            result = subprocess.run([sys.executable, script_path], 
                                   cwd=data_proc_path, capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print("错误:", result.stderr)
        else:
            print(f"数据下载脚本不存在: {script_path}")


def start_visualization(port=8080):
    """启动可视化模块"""
    print(f"=== 启动可视化模块 (端口 {port}) ===")
    vis_path = os.path.join(os.path.dirname(__file__), 'visualization')
    
    script_path = os.path.join(vis_path, 'quant_web_app.py')
    if os.path.exists(script_path):
        print(f"请在 {vis_path} 目录下运行: python quant_web_app.py")
        print(f"Web界面将在 http://localhost:{port} 可用")
    else:
        print(f"Web应用脚本不存在: {script_path}")


def show_help():
    """显示帮助信息"""
    print("""
量化股票分析系统

用法:
  python main.py [选项]

选项:
  --selection DATE     运行选股模块 (DATE格式: YYYY-MM-DD, 默认为今天)
  --data ACTION        运行数据处理模块 (ACTION: generate_pool/download_data)
  --visualize PORT     启动可视化模块 (PORT默认为8080)
  --all                运行所有模块
  --help               显示此帮助信息

示例:
  python main.py --selection 2026-01-12    # 运行特定日期的选股
  python main.py --data generate_pool      # 生成股票池
  python main.py --visualize               # 启动Web界面
  python main.py --all                     # 运行完整流程
""")


def main():
    parser = argparse.ArgumentParser(description='量化股票分析系统', add_help=False)
    
    # 添加自定义参数解析
    args = sys.argv[1:]
    
    if not args or '--help' in args or '-h' in args:
        show_help()
        return
    
    if '--all' in args:
        print("运行完整流程...")
        run_data_processing('generate_pool')
        run_selection()
        start_visualization()
        return
    
    i = 0
    while i < len(args):
        arg = args[i]
        
        if arg == '--selection':
            if i + 1 < len(args):
                target_date = args[i + 1]
                run_selection(target_date)
                i += 2
            else:
                print("错误: --selection 需要指定日期参数")
                return
        
        elif arg == '--data':
            if i + 1 < len(args):
                action = args[i + 1]
                run_data_processing(action)
                i += 2
            else:
                print("错误: --data 需要指定动作参数 (generate_pool/download_data)")
                return
        
        elif arg == '--visualize':
            port = 8080  # 默认端口
            if i + 1 < len(args) and args[i + 1].isdigit():
                port = int(args[i + 1])
                i += 2
            else:
                i += 1
            start_visualization(port)
        
        else:
            print(f"未知参数: {arg}")
            show_help()
            return


if __name__ == "__main__":
    main()