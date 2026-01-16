# 量化选股系统 v2

基于AkShare的本地量化选股平台，支持股票池生成、数据更新、选股和可视化功能。

## 核心功能

- **股票池生成**：生成每日涨停股票池
- **数据更新**：更新股票日线数据
- **智能选股**：基于涨停板策略的选股算法（使用select_2026_01_12.py）
- **市值管理**：股票市值数据管理
- **可视化界面**：Web界面进行交互式选股

## 环境安装

```bash
# 创建并激活虚拟环境
python3 -m venv quant_env
source quant_env/bin/activate

# 安装依赖
pip install -r requirements.txt
# 或单独安装
pip install akshare pandas numpy flask requests
```

## 主要命令

### 1. 更新股票日线数据

```bash
# 智能更新（推荐）- 只更新涨停/破板股票
python update_data_smart.py

# 增量更新 - 更新所有股票的最新数据
cd data_processing && python incremental_download.py
```

### 2. 生成股票池数据

```bash
# 生成指定日期的股票池数据
cd data_processing && python stock_pool_generator.py

# 按提示选择单日或批量生成
```

### 3. 执行选股（仅使用select_2026_01_12.py）

```bash
# 按指定日期选股（仅使用select_2026_01_12.py）
cd selection && python select_2026_01_12.py 2026-01-12

# 或直接运行select_2026_01_12.py
python selection/select_2026_01_12.py <日期>

# 保留快速选股功能，但所有选股均通过select_2026_01_12.py执行
```

### 4. 数据管理

```bash
# 更新市值数据
cd data_processing && python get_market_caps.py update

# 获取特定股票市值
cd data_processing && python get_market_caps.py get <股票代码> <日期>

# 本地数据管理
cd data_processing && python local_data_manager.py
```

## 可视化功能

### 启动Web界面

```bash
# 方法1: 使用启动脚本
cd visualization
./start_web.sh

# 方法2: 直接运行
cd visualization
python quant_web_app.py
```

访问地址：`http://127.0.0.1:5007`

### Web界面功能
- **股票筛选**：支持多种选股策略
- **数据可视化**：图表展示选股结果
- **实时监控**：监控股票池变化
- **历史回测**：回测选股策略效果

### API接口
- `/api/screen_stocks` - 执行股票筛选
- `/api/fast_screen_stocks` - 快速股票筛选
- `/api/trade` - 获取股票竞价数据
- `/api/data_status` - 获取本地数据状态
- `/api/update_today_data` - 增量更新今日数据
- `/api/generate_stock_pool` - 生成股票池

## data_processing中的可视化组件

### 1. 股票池生成器可视化
- `stock_pool_generator.py` - 生成每日股票池数据

### 2. 数据下载管理
- `incremental_download.py` - 增量数据下载

### 3. 市值管理
- `get_market_caps.py` - 市值数据管理

### 4. 本地数据管理
- `local_data_manager.py` - 本地数据管理

## 完整流程示例

```bash
# 1. 更新数据（每天收盘后执行）
python update_data_smart.py

# 2. 生成股票池（使用最新数据）
cd data_processing && python stock_pool_generator.py

# 3. 启动Web界面进行选股（次日开盘前）
cd visualization && ./start_web.sh

# 4. 或直接命令行选股（仅使用select_2026_01_12.py）
python selection/select_2026_01_12.py 2026-01-13
```

## 数据目录说明

- `full_stock_data/daily_data/` - 股票日线数据（CSV格式）
- `full_stock_data/pool_data/` - 股票池数据（JSON格式）
- `data_processing/` - 数据处理模块
- `selection/` - 选股模块（仅使用select_2026_01_12.py）
- `visualization/` - 可视化模块

## 选股策略

系统实现以下选股策略：

1. **首板高开**：寻找涨停后高开的股票
2. **首板低开**：寻找涨停后低开的股票
3. **弱转强**：寻找曾经涨停但未封板，随后转强的股票

## 项目结构

```
quantv2/
├── data_processing/           # 数据处理模块
│   ├── stock_pool_generator.py    # 股票池生成器
│   ├── incremental_download.py    # 增量数据下载
│   ├── get_market_caps.py         # 市值数据管理
│   └── local_data_manager.py      # 本地数据管理
├── selection/               # 选股模块
│   └── select_2026_01_12.py       # 选股主程序（唯一选股文件）
├── visualization/           # 可视化模块
│   ├── quant_web_app.py           # Web应用
│   ├── start_web.sh               # 启动脚本
│   ├── templates/                 # 前端模板
│   └── static/                    # 静态资源
├── full_stock_data/         # 本地数据目录
│   ├── daily_data/          # 日线数据CSV文件
│   └── pool_data/           # 股票池数据
├── update_data_smart.py     # 智能数据更新器
└── optimized_tdx_handler.py # TDX数据处理器
```

## 维护命令

```bash
# 检查数据完整性
python -c "from data_processing.local_data_manager import LocalDataManager; dm = LocalDataManager(); print('Latest date:', dm.get_latest_data_date())"

# 生成最新股票列表
python -c "from data_processing.local_data_manager import LocalDataManager; dm = LocalDataManager(); dm.update_stock_list()"

# 运行快速选股验证（仅通过select_2026_01_12.py）
cd selection && python select_2026_01_12.py 2026-01-12
```



