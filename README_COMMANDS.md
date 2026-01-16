# 量化选股系统 v2

基于AkShare的本地量化选股平台，支持股票池生成、数据更新和选股功能。

## 核心功能

- **股票池生成**：生成每日涨停股票池
- **数据更新**：更新股票日线数据
- **智能选股**：基于涨停板策略的选股算法
- **市值管理**：股票市值数据管理

## 环境安装

```bash
# 创建并激活虚拟环境
python3 -m venv quant_env
source quant_env/bin/activate

# 安装依赖
pip install akshare pandas numpy
```

## 主要执行命令

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

### 3. 执行选股
```bash
# 按指定日期选股
cd selection && python select_2026_01_12.py 2026-01-12

# 或直接运行
python selection/select_2026_01_12.py <日期>
```

## 完整流程示例

```bash
# 1. 更新数据（每天收盘后执行）
python update_data_smart.py

# 2. 生成股票池（使用最新数据）
cd data_processing && python stock_pool_generator.py

# 3. 执行选股（次日开盘前）
python selection/select_2026_01_12.py 2026-01-13
```

## 数据目录

- `full_stock_data/daily_data/` - 股票日线数据
- `full_stock_data/pool_data/` - 股票池数据（JSON格式）

## 系统组件

- **数据更新**：`update_data_smart.py` - 智能更新涨停股票数据
- **股票池生成**：`data_processing/stock_pool_generator.py` - 生成选股所需数据
- **选股引擎**：`selection/select_2026_01_12.py` - 执行选股策略

## 选股策略

系统实现以下选股策略：

1. **首板高开**：寻找涨停后高开的股票
2. **首板低开**：寻找涨停后低开的股票
3. **弱转强**：寻找曾经涨停但未封板，随后转强的股票

## 使用示例

### 完整流程

```bash
# 1. 更新数据
python update_data_smart.py

# 2. 生成股票池（使用前一日数据）
python -m data_processing.stock_pool_generator

# 3. 执行选股
python selection/select_2026_01_12.py 2026-01-12
```

### 选股参数说明

- `limit_up_stocks`：前一日涨停股票
- `first_board_stocks`：首板股票（昨日涨停但前日未涨停）
- `limit_up_not_closed_stocks`：前一日曾涨停未封板股票

## 数据目录说明

- `full_stock_data/daily_data/`：存储各股票的日线数据，每个股票一个CSV文件
- `full_stock_data/pool_data/`：存储生成的股票池数据，按日期命名的JSON文件

## 维护命令

```bash
# 检查数据完整性
python -c "from data_processing.local_data_manager import LocalDataManager; dm = LocalDataManager(); print('Latest date:', dm.get_latest_data_date())"

# 生成最新股票列表
python -c "from data_processing.local_data_manager import LocalDataManager; dm = LocalDataManager(); dm.update_stock_list()"
```