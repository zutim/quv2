# 量化股票分析系统使用指南

## 项目结构

项目已重新组织为三个主要部分：

### 1. 选股模块 (selection/)
负责执行股票筛选策略

**主要文件：**
- `select_2026_01_12.py` - 主要选股脚本，按指定日期进行选股
- `select_today_stocks.py` - 今日选股器
- `enhanced_stock_selector.py` - 增强选股器
- `local_strategy.py` - 本地策略实现
- `aa.py` - 原始策略参考

**运行方法：**
```bash
cd selection
python select_2026_01_12.py          # 使用默认日期
python select_2026_01_12.py 2026-01-12  # 指定日期
```

### 2. 数据处理模块 (data_processing/)
负责数据获取、处理和股票池生成

**主要文件：**
- `stock_pool_generator.py` - 生成指定日期的股票池数据
- `incremental_download.py` - 增量下载股票数据
- `local_data_manager.py` - 本地数据管理
- `get_market_caps.py` - 获取股票市值数据

**运行方法：**
```bash
cd data_processing
python stock_pool_generator.py       # 生成股票池
python incremental_download.py      # 增量下载数据
```

### 3. 可视化模块 (visualization/)
提供Web界面和API服务

**主要文件：**
- `quant_web_app.py` - Web应用程序
- `fast_web_strategy.py` - Web选股策略
- `backtest_engine.py` - 回测引擎
- `templates/` - HTML模板

**运行方法：**
```bash
cd visualization
python quant_web_app.py             # 启动Web服务
# 访问 http://localhost:8080
```

## 快速开始

### 方法1：使用主入口脚本
```bash
python main.py --data generate_pool    # 生成股票池
python main.py --selection 2026-01-12  # 运行选股
python main.py --visualize             # 启动Web界面
python main.py --all                   # 运行完整流程
```

### 方法2：分步执行
1. **准备数据：**
```bash
cd data_processing
python stock_pool_generator.py
```

2. **运行选股：**
```bash
cd selection
python select_2026_01_12.py
```

3. **启动Web界面（可选）：**
```bash
cd visualization
python quant_web_app.py
```

## 策略说明

### 首板高开策略
- 昨日涨停，前日未涨停
- 均价获利比例 >= 7%
- 成交额在5.5亿-20亿之间
- 开盘价在前收盘价1%-6%之间（高开但未涨停）

### 首板低开策略
- 昨日涨停，前日未涨停
- 低开幅度在3%-4.5%之间
- 相对位置 <= 50%

### 弱转强策略
- 昨日曾涨停但未封板
- 前3日涨幅 <= 28%
- 前日跌幅 >= -5%

## API接口

Web应用提供以下API接口：
- `GET /api/screen_stocks` - 股票筛选
- `GET /api/fast_screen_stocks` - 快速股票筛选
- `GET /api/trade?code=xxx&date=yyyymmdd` - 获取竞价数据
- `GET /api/data_status` - 获取数据状态

## 故障排除

1. **缺少依赖包：**
```bash
pip install akshare pandas numpy flask requests
```

2. **数据文件不存在：**
   - 运行 `stock_pool_generator.py` 生成股票池数据
   - 运行 `incremental_download.py` 下载股票数据

3. **选股结果为空：**
   - 检查股票池数据是否正确生成
   - 确认目标日期的股票数据是否存在

## 维护说明

- 股票池数据存储在 `full_stock_data/pool_data/`
- 历史数据存储在 `full_stock_data/daily_data/`
- 日志文件位于项目根目录

## 注意事项

1. 系统依赖网络连接获取实时数据
2. 选股结果仅供参考，不构成投资建议
3. 请确保遵守数据供应商的使用条款
4. 定期更新股票池数据以保证准确性