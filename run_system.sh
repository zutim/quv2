#!/bin/bash

# 量化股票分析系统启动脚本

echo "==========================================="
echo "  量化股票分析系统"
echo "==========================================="
echo "请选择要运行的模块："
echo "1) 生成股票池数据"
echo "2) 运行选股 (默认日期)"
echo "3) 运行选股 (指定日期)"
echo "4) 启动Web界面"
echo "5) 运行完整流程"
echo "6) 退出"
echo ""

read -p "请输入选项 (1-6): " option

case $option in
    1)
        echo "正在生成股票池数据..."
        cd data_processing
        python stock_pool_generator.py
        ;;
    2)
        echo "正在运行选股..."
        cd selection
        python select_2026_01_12.py
        ;;
    3)
        read -p "请输入日期 (格式: YYYY-MM-DD, 如 2026-01-12): " date
        echo "正在运行选股 ($date)..."
        cd selection
        python select_2026_01_12.py $date
        ;;
    4)
        echo "正在启动Web界面..."
        cd visualization
        python quant_web_app.py
        ;;
    5)
        echo "正在运行完整流程..."
        cd data_processing
        echo "生成股票池数据..."
        python stock_pool_generator.py
        cd ../selection
        echo "运行选股..."
        python select_2026_01_12.py
        echo "完成！如需启动Web界面，请单独运行选项4。"
        ;;
    6)
        echo "退出系统。"
        exit 0
        ;;
    *)
        echo "无效选项！"
        exit 1
        ;;
esac