"""
启动QMT数据客户端， 并检查更新本地duckdb数据库

1. 获取线上数据库股票列表
2. 与本地数据库进行逐票比对（日线、分钟线数据表），获取个股需补全的日期区间
3. 获取个股增量数据（日线、分钟线数据）
4. 更新本地数据库
"""

import configparser
from qka.client import QMTDataClient
from utils.duckdb import DuckDBHelper
from utils.tools import compare_lists


def load_config(config_path='config.ini'):
    """加载配置文件"""
    config = configparser.ConfigParser()
    with open(config_path, 'r', encoding='utf-8') as f:
        config.read_file(f)
    return config


def get_client_and_helper(config):
    """初始化客户端和DuckDB辅助类"""
    qmt_server_config = config.get('QMT-SERVER', 'base_url')
    qmt_server_token = config.get('QMT-SERVER', 'token')
    duckdb_path = config.get('TARGET', 'path')

    client = QMTDataClient(base_url=qmt_server_config, token=qmt_server_token)
    duckdb_helper = DuckDBHelper(duckdb_path)
    return client, duckdb_helper


def check_and_update_local_data(
    client, duckdb_helper,
    stock_list, start_date, end_date, table_name
):
    """
    检查并更新本地duckdb数据库指定表的个股数据。

    Args:
        client: QMTDataClient 实例
        duckdb_helper: DuckDBHelper 实例
        stock_list (list): 股票代码列表
        start_date (str): 开始日期
        end_date (str): 结束日期
        table_name (str): 本地duckdb表名（如 'daily_1min' 或 'daily_1d'）
    """
    # 定义表名与周期的映射关系，使用枚举模式
    TABLE_PERIOD_MAP = {
        'daily_1d': '1d',
        'daily_1min': '1m',
        # 未来可根据需要扩充更多表对应的周期
    }

    total = len(stock_list)
    for idx, stock_code in enumerate(stock_list):
        # 获取表名对应的周期，找不到则抛出异常或者设置默认周期
        period = TABLE_PERIOD_MAP.get(table_name)
        if not period:
            raise ValueError(f"未知的表名 '{table_name}'，无法确定对应的周期。请在 TABLE_PERIOD_MAP 中添加该表与周期的映射。")
        # 获取个股数据范围（交易日历日期列表）
        trade_dates = client.get_stock_data_range(stock_code, period, start_date, end_date)
        # 检查指定表
        local_trade_dates = duckdb_helper.query_stock_trade_dates(stock_code, table_name)
        compare_dates = compare_lists(local_trade_dates, trade_dates)
        # 返回需要补全的日期区间
        if compare_dates:
            print(f"个股{stock_code}需要补全的日期区间: {compare_dates} (进度: {idx + 1}/{total})")
            incremental_data = client.get_daily_bars([stock_code], period, compare_dates[0], compare_dates[-1])
            duckdb_helper.insert_df_to_duckdb(incremental_data, table_name)


def main():
    # 读取配置文件
    config = load_config()
    start_date = config.get('COMPLETION', 'start_date')
    end_date = config.get('COMPLETION', 'end_date')

    client, duckdb_helper = get_client_and_helper(config)

    stock_list = client.get_stock_list_in_main_board()

    check_and_update_local_data(client, duckdb_helper, stock_list, start_date, end_date, "daily_1d")
    check_and_update_local_data(client, duckdb_helper, stock_list, start_date, end_date, "daily_1min")


if __name__ == "__main__":
    main()