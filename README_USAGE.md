# 量化选股平台 - 使用说明

## 🚀 快速启动

### 方式1：Web界面（推荐）

```bash
# 启动Web服务器
chmod +x start_web.sh
./start_web.sh
```

然后在浏览器打开：**http://127.0.0.1:5007**

功能：
- ✅ **股票筛选**：选择日期和策略进行选股
- ✅ **策略回测**：测试策略的历史表现
- ✅ **验证策略（Hybrid）**：自动选择实时/历史数据

---

### 方式2：命令行选股

```bash
# 给脚本添加执行权限
chmod +x run_strategy.sh

# 选今天的股票
./run_strategy.sh

# 选指定日期的股票
./run_strategy.sh 2025-11-12
```

---

## 📁 核心文件说明

| 文件 | 用途 | 说明 |
|------|------|------|
| `start_web.sh` | **Web启动脚本** | 启动Web界面（推荐） |
| `run_strategy.sh` | **命令行选股** | 快速命令行选股 |
| `quant_web_app.py` | Web服务器 | Flask应用 |
| `local_strategy.py` | 核心策略 | 精确的aa.py策略 |
| `local_data_manager.py` | 数据管理 | 数据获取和缓存 |
| `quant_engine.py` | 引擎 | 模拟JoinQuant API |

---

## 🔧 策略说明

### ✅ 验证策略（Hybrid）
- **实时模式**（日期=今天）：使用腾讯接口获取09:25竞价数据
- **回测模式**（历史日期）：使用09:30分钟数据或日线开盘价作为代理
- **特点**：最精确，与aa.py逻辑完全一致

### 🔴 混合策略（旧版）
- 简化版策略，准确度较低
- 不推荐使用

---

## 💡 常见问题

**Q: Web界面打不开？**
- 检查端口5007是否被占用：`lsof -i :5007`
- 重启脚本：`./start_web.sh`

**Q: 命令行选股没有输出？**
- 检查日期格式是否正确（YYYY-MM-DD）
- 查看是否有错误信息

**Q: 选不出股票？**
- 正常情况，可能当天确实没有符合条件的股票
- 可以尝试其他日期

---

## 📞 技术支持

如有问题，请检查：
1. Python版本（需要Python 3.8+）
2. 虚拟环境是否激活
3. 依赖是否安装完整

---

**祝您选股顺利！** 🎯

批量生成pool数据
cd /Users/ztm/gopath/src/test1/quantv2 && python3 stock_pool_generator.py


选股策略
python3 select_2026_01_12.py 26-01-12

# 项目结构说明

## 项目已重构为三个核心部分

### 1. 选股模块 (selection/)
- `select_2026_01_12.py` - 主要选股脚本
- `select_today_stocks.py` - 今日选股器
- `enhanced_stock_selector.py` - 增强选股模块
- `local_strategy.py` - 本地策略实现
- `aa.py` - 原始策略参考

### 2. 数据处理模块 (data_processing/)
- `stock_pool_generator.py` - 股票池生成器
- `incremental_download.py` - 增量数据下载
- `local_data_manager.py` - 本地数据管理
- `quick_data_fetcher.py` - 快速数据获取
- `get_market_caps.py` - 市值数据获取

### 3. 可视化模块 (visualization/)
- `quant_web_app.py` - Web应用程序
- `fast_web_strategy.py` - Web选股策略
- `backtest_engine.py` - 回测引擎
- `templates/` - HTML模板

## 已删除的测试文件
已清理所有临时测试文件，保留核心功能模块。

## 运行方式
- 选股：`cd selection && python select_2026_01_12.py`
- 数据处理：`cd data_processing && python stock_pool_generator.py`
- 可视化：`cd visualization && python quant_web_app.py`
