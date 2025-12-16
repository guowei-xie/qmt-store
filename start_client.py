"""
启动QMT数据客户端， 并检查更新本地duckdb数据库

1. 获取线上数据库股票列表
2. 与本地数据库进行逐票比对（日线、分钟线数据表），获取个股需补全的日期区间
3. 获取个股增量数据（日线、分钟线数据）
4. 更新本地数据库
"""

import pandas as pd
import configparser
from qka.client import QMTDataClient
from utils.duckdb import DuckDBHelper
from utils.tools import compare_lists
import akshare as ak
from utils.clean import clean_index_data


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
    stock_list, start_date, end_date, table_name, period, rebuild
):
    """
    检查并更新本地duckdb数据库指定表的个股数据。

    Args:
        client: QMTDataClient 实例
        duckdb_helper: DuckDBHelper 实例
        stock_list (list): 股票代码列表
        start_date (str): 开始日期
        end_date (str): 结束日期
        table_name (str): 本地duckdb表名（如 'daily_1min' 或 'daily_1day'）
        period (str): 对应QMT周期（如 '1d' 或 '1m'）
    """
    total = len(stock_list)
    for idx, stock_code in enumerate(stock_list):
        # 获取个股数据范围（交易日历日期列表）
        trade_dates = client.get_stock_data_range(stock_code, start_date, end_date)
        # 检查指定表
        local_trade_dates = duckdb_helper.get_stock_trade_dates(stock_code, table_name)
        compare_dates = compare_lists(local_trade_dates, trade_dates)
        # 返回需要补全的日期区间
        if compare_dates:
            print(f"表{table_name}，个股{stock_code}需要补全的日期区间: {compare_dates[0]} 到 {compare_dates[-1]} (进度: {idx + 1}/{total})")
            incremental_data = client.get_daily_bars([stock_code], period, compare_dates[0], compare_dates[-1])
            stock_data = incremental_data.get(stock_code)
            if not stock_data:  # 如果incremental_data为空，跳过
                print(f"个股{stock_code}在区间{compare_dates[0]}到{compare_dates[-1]}无增量数据，跳过。")
                continue
            df = pd.DataFrame(stock_data)
            df['code'] = stock_code
            # 仅保留列
            df = df[['code', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount']]
            
            # 是否需要重建（当补全范围可能与已有数据有重叠时，需要重建）
            if rebuild:
                # 合并并去重后重建该股票数据
                existing_df = duckdb_helper.get_stock_data(stock_code, table_name)
                merged_df = pd.concat([existing_df, df], ignore_index=True) if not existing_df.empty else df
                merged_df = (
                    merged_df.sort_values('time')
                    .drop_duplicates(subset='time', keep='last')
                    .reset_index(drop=True)
                )
                duckdb_helper.delete_stock_data(stock_code, table_name)
                duckdb_helper.insert_df_to_duckdb(merged_df, table_name)
            else:
                duckdb_helper.insert_df_to_duckdb(df, table_name)

def insert_stock_list_to_duckdb(duckdb_helper, stock_list):
    """
    将股票列表插入到DuckDB中
    """
    df = pd.DataFrame(stock_list, columns=['code'])
    duckdb_helper.insert_df_to_duckdb(df, 'stock_list', overwrite=True)

# 获取akshare指数数据并插入数据库
def get_akshare_index_data_and_insert_to_duckdb(duckdb_helper, symbol_list, start_date, end_date):
    """
    获取akshare指数数据并与数据库当前数据合并去重后插入到DuckDB中
    """
    for symbol in symbol_list:
        df = ak.stock_zh_index_daily_em(symbol=symbol, start_date=start_date, end_date=end_date)
        df['code'] = symbol
        df = clean_index_data(df)
        existing_df = duckdb_helper.get_stock_data(symbol, 'index_daily')
        merged_df = pd.concat([existing_df, df], ignore_index=True) if not existing_df.empty else df
        merged_df = (
            merged_df.sort_values('time')
            .drop_duplicates(subset='time', keep='last')
            .reset_index(drop=True)
        )
        duckdb_helper.delete_stock_data(symbol, 'index_daily')
        duckdb_helper.insert_df_to_duckdb(merged_df, 'index_daily')

def main():
    # 读取配置文件
    config = load_config()
    start_date = config.get('COMPLETION', 'start_date')
    end_date = config.get('COMPLETION', 'end_date')
    rebuild = config.get('COMPLETION', 'rebuild')

    client, duckdb_helper = get_client_and_helper(config)

    # 获取akshare指数数据并插入数据库
    symbol_list = ['sh000001']
    get_akshare_index_data_and_insert_to_duckdb(duckdb_helper, symbol_list, start_date, end_date)
    
    # 获取股票列表,写入股票列表到本地库
    stock_list = client.get_stock_list_in_main_board()

    insert_stock_list_to_duckdb(duckdb_helper, stock_list)
    # 检查并更新本地库行情数据表
    check_and_update_local_data(client, duckdb_helper, stock_list, start_date, end_date, "daily_1day", "1d", rebuild)
    check_and_update_local_data(client, duckdb_helper, stock_list, start_date, end_date, "daily_1min", "1m", rebuild)


if __name__ == "__main__":
    main()