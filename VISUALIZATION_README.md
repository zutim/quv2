# 量化选股系统 - 可视化功能

## 启动可视化界面

### 1. 启动Web界面
```bash
# 方法1: 使用启动脚本
cd visualization
./start_web.sh

# 方法2: 直接运行
cd visualization
python quant_web_app.py
```

访问地址：`http://127.0.0.1:5007`

### 2. 命令行选股
```bash
# 选股指定日期
cd visualization
./run_strategy.sh 2026-01-12

# 选股当天
./run_strategy.sh
```

## 主要功能

### Web界面功能
- **股票筛选**：支持多种选股策略
- **数据可视化**：图表展示选股结果
- **实时监控**：监控股票池变化
- **历史回测**：回测选股策略效果

### 选股策略
1. **首板高开**：涨停后高开的股票
2. **首板低开**：涨停后低开的股票
3. **弱转强**：曾涨停未封板后转强的股票

## 系统架构

### 核心组件
- `quant_web_app.py` - Flask Web服务器
- `fast_web_strategy.py` - 快速选股策略
- `backtest_engine.py` - 回测引擎
- `templates/index.html` - 前端界面

### 数据接口
- `/api/screen_stocks` - 执行股票筛选
- `/api/fast_screen_stocks` - 快速股票筛选
- `/api/trade` - 获取股票竞价数据
- `/api/data_status` - 获取本地数据状态
- `/api/update_today_data` - 增量更新今日数据
- `/api/generate_stock_pool` - 生成股票池

## 环境配置

### 依赖安装
```bash
pip install flask pandas numpy akshare requests
```

### 数据准备
在启动可视化前，需要准备以下数据：
1. `full_stock_data/daily_data/` - 股票日线数据
2. `full_stock_data/pool_data/` - 股票池数据

## 使用流程

### 1. 准备数据
```bash
# 更新股票数据
python update_data_smart.py

# 生成股票池
cd data_processing
python stock_pool_generator.py
```

### 2. 启动服务
```bash
cd visualization
./start_web.sh
```

### 3. 访问界面
打开浏览器访问 `http://127.0.0.1:5007`

### 4. 执行选股
在Web界面上选择日期和策略，点击选股按钮

## 常见问题

### 端口冲突
如果5007端口被占用，启动脚本会自动尝试关闭占用进程

### 数据缺失
确保 `full_stock_data` 目录包含必要的数据文件

### API连接问题
检查本地数据和网络连接是否正常