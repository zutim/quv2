#!/bin/bash

echo "========================================="
echo "  量化选股平台 - 命令行选股"
echo "========================================="
echo ""

# 检查参数
if [ -z "$1" ]; then
    echo "📅 使用方法: ./run_strategy.sh [日期]"
    echo "   示例: ./run_strategy.sh 2026-01-06"
    echo "   留空则使用今天日期"
    echo ""
    # 使用今天的日期
    DATE=$(date +%Y-%m-%d)
    echo "📍 使用今天日期: $DATE"
else
    DATE=$1
    echo "📍 选股日期: $DATE"
fi

echo ""
echo "🚀 运行验证策略（Hybrid: 实时/回测）..."
echo ""

# 临时修改 local_strategy.py 中的日期
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# 备份原文件
cp local_strategy.py local_strategy.py.bak

# 替换日期
sed -i.tmp "s/context.current_dt = datetime([0-9]*, [0-9]*, [0-9]*, 9, 30)/context.current_dt = datetime($YEAR, $MONTH, $DAY, 9, 30)/" local_strategy.py

# 运行策略
python3 local_strategy.py

# 恢复原文件
mv local_strategy.py.bak local_strategy.py
rm -f local_strategy.py.tmp

echo ""
echo "✅ 选股完成！"
