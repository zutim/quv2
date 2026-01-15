#!/bin/bash

echo "========================================="
echo "  量化选股平台 - Web界面启动脚本"
echo "========================================="
echo ""

# 检查虚拟环境是否存在
if [ ! -d "quant_env" ] && [ ! -d "venv" ]; then
    echo "❌ 错误: 虚拟环境不存在！请先创建虚拟环境 (quant_env 或 venv)。"
    echo "💡 创建虚拟环境命令: python3 -m venv quant_env"
    exit 1
fi

# 激活虚拟环境
if [ -d "quant_env" ]; then
    echo "⚙️  激活虚拟环境 (quant_env)..."
    source quant_env/bin/activate
elif [ -d "venv" ]; then
    echo "⚙️  激活虚拟环境 (venv)..."
    source venv/bin/activate
fi

echo "✅ 虚拟环境已激活。"
echo ""

# 检查端口是否被占用
PORT=5007
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null ; then
    echo "⚠️  警告: 端口 $PORT 已被占用，正在尝试关闭..."
    pkill -f quant_web_app.py
    sleep 2
fi

echo "🚀 启动 Web 服务器..."
echo "📍 访问地址: http://127.0.0.1:$PORT"
echo "⏸️  按 Ctrl+C 停止服务器"
echo ""

# 启动服务器
python quant_web_app.py
