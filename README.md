# QMT 数据存储系统

一个基于 QMT（迅投量化交易平台）的股票数据存储和管理系统，支持数据同步、本地存储和增量更新。主要用于解决在本地非 Windows 环境下无法使用 QMT 数据服务的问题。

- 在 Windows 服务器运行 `start_server.py` 用于启动 API 服务，通过接口间接提取 QMT 数据；`complete.py` 用于补全服务端 QMT 数据。
- 在本地（可为非 Windows 系统）运行 `start_client.py` 用于调用 API 更新本地数据库；`importdb.py` 用于初始化（或有大量级的历史分时数据需求）时导入 CSV 格式数据。

## 功能特性

- 📊 **数据获取**: 通过 QMT 接口获取股票行情数据（日线、分钟线）
- 💾 **本地存储**: 使用 DuckDB 高效存储和管理股票数据
- 🔄 **增量同步**: 自动比对本地和远程数据，智能补全缺失数据
- 🔧 **数据重建**: 支持数据重建模式，自动合并去重处理重叠数据
- 🌐 **API 服务**: 提供 FastAPI 数据服务接口
- ⏰ **定时任务**: 支持定时下载最新数据
- 📥 **数据导入**: 支持从 CSV 文件批量导入历史数据
- 📈 **指数数据补全**: 通过 AkShare 自动获取指定指数（如 `sh000001`）的日线行情，并与本地数据合并去重后写入数据库

## 项目结构

```
qmt-store/
├── qka/                    # 核心模块
│   ├── client.py          # 数据服务客户端
│   ├── server.py          # 数据服务服务器
│   └── data.py            # 数据处理工具
├── utils/                  # 工具模块
│   ├── duckdb.py          # DuckDB 操作工具
│   ├── clean.py           # 数据清洗工具
│   └── tools.py           # 通用工具函数
├── start_server.py         # 启动数据服务器
├── start_client.py         # 启动数据客户端（同步数据）
├── importdb.py             # 导入 CSV 数据到数据库
├── crontab.py              # 定时任务脚本
├── config.ini              # 配置文件
└── requirements.txt        # 依赖包列表
```

## 环境要求

- Python 3.10+
- QMT 量化交易平台
- DuckDB

## 安装步骤

1. **克隆项目**
```bash
git clone <repository-url>
cd qmt-store
```

2. **安装依赖**
```bash
pip install -r requirements.txt
```

3. **配置 QMT**
   - 确保已安装并配置好 QMT 量化交易平台
   - 将 `xtquant` 目录放置在项目根目录下

4. **配置参数**
   - 复制 `config.copy.ini` 为 `config.ini`
   - 根据实际情况修改配置参数：
     - `SOURCE.path`: CSV 数据源路径
     - `TARGET.path`: DuckDB 数据库路径
     - `QMT-SERVER.base_url`: 数据服务器地址
     - `QMT-SERVER.token`: 访问令牌
     - `COMPLETION`: 数据补全配置
       - `rebuild`: 是否启用数据重建模式（当补全范围可能与已有数据有重叠时设为 `true`）
       - `start_date` / `end_date`: 本地数据日期范围
       - `remote_1d_start_date` / `remote_1m_start_date`: 远端数据开始日期

## 使用方法

### 1. 启动数据服务器

启动 FastAPI 数据服务，提供数据获取接口：

```bash
python start_server.py
```

服务器默认运行在 `http://0.0.0.0:8000`，启动后会显示授权 Token，客户端需要使用此 Token 进行访问。

### 2. 同步数据到本地数据库

启动客户端，从服务器获取数据并更新本地 DuckDB 数据库：

```bash
python start_client.py
```

功能说明：
- 获取线上数据库股票列表
- 与本地数据库进行逐票比对（日线、分钟线数据表）
- 获取个股需补全的日期区间
- 获取个股增量数据并更新本地数据库
- 支持数据重建模式：当补全范围可能与已有数据有重叠时，可启用重建模式自动合并去重

### 3. 导入 CSV 数据

因QMT仅支持近两年分时数据调取，如有更长时间的数据需求，可将 CSV 格式的历史分时行情数据导入到 DuckDB：  
历史分时数据下载: https://pan.baidu.com/s/1-czTMI0zuoICIofOkqYxbg?pwd=8mti 提取码: 8mti

```bash
python importdb.py
```

### 4. 定时任务

设置定时任务，每日自动下载最新数据：

```bash
python crontab.py
```

建议使用 cron 或任务计划程序设置定时执行（如每日 15:30）。

## 配置说明

### config.ini 配置项

```ini
[SOURCE]
# 分钟数据源路径（CSV 文件目录）
path = /path/to/stock_data

[TARGET]
# 目标数据库路径（DuckDB 文件路径）
path = /path/to/stock.duckdb

[QMT-SERVER]
# 数据服务器地址
base_url = http://localhost:8000
# 访问令牌（需与服务器 Token 一致）
token = your_token_here

[COMPLETION]
# 是否需要重建（当补全范围可能与已有数据有重叠时，需要重建）
# true: 合并已有数据和增量数据，去重后重建该股票数据
# false: 直接插入增量数据（默认，适用于无重叠的情况）
rebuild = false
# 本地数据开始日期
start_date = 20200101
# 本地数据结束日期
end_date = 20251127
# 远端日线数据开始日期
remote_1d_start_date = 20200101
# 远端分钟线数据开始日期
remote_1m_start_date = 20251001
```

## 数据表结构

### daily_1day（日线数据表）
- `code`: 股票代码
- `time`: 时间戳（毫秒）
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `close`: 收盘价
- `volume`: 成交量
- `amount`: 成交额

### daily_1min（分钟线数据表）
字段同 `daily_1day`，数据粒度为一分钟。

### index_daily（指数日线数据表）
- `code`: 指数代码（如 `sh000001`）
- `time`: 时间戳（毫秒）
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `close`: 收盘价
- `volume`: 成交量
- `amount`: 成交额

## API 接口

数据服务器提供以下 API 接口（需要 Token 认证）：

- `get_stock_list_in_sector`: 获取板块成分股
- `get_stock_list_in_main_board`: 获取沪深A股主板成分股
- `download_stock_history_data`: 下载股票历史K线数据
- `get_daily_bars`: 获取行情数据
- `get_stock_data_range`: 获取个股数据范围（交易日期列表）

详细接口文档请参考 `qka/server.py` 和 `qka/client.py`。

## 开发说明

### 核心模块

- **qka/data.py**: 提供数据获取和处理功能，包括：
  - 股票代码处理（添加后缀、市场类型判断）
  - 交易日历获取
  - 历史数据下载
  - 行情数据获取

- **qka/client.py**: 数据服务客户端，封装 API 调用。

- **qka/server.py**: 数据服务服务器，自动将 `data.py` 中的函数转换为 API 端点。

- **utils/duckdb.py**: DuckDB 数据库操作工具类，包括表创建、数据插入、个股数据与交易日查询等。

- **utils/clean.py**: 指数数据清洗与字段标准化工具，配合 AkShare 指数数据入库使用。

## 注意事项

1. **Token 安全**: 请妥善保管服务器 Token，不要泄露
2. **数据路径**: 确保配置的数据路径有读写权限
3. **内存管理**: 大数据量导入时注意内存使用，已做分块处理优化
4. **网络连接**: 客户端需要能够访问数据服务器地址
5. **QMT 环境**: 确保 QMT 平台正常运行，数据获取功能依赖 QMT

## 贡献

欢迎提交 Issue 和 Pull Request！

