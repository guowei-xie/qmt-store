"""
测试DuckDB连接/读取功能
"""

import duckdb
import configparser

import os
import sys

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.duckdb import DuckDBHelper

def test_duckdb_connection_read():
    """测试DuckDB连接并读取数据"""
    config = configparser.ConfigParser()
    with open('config.ini', 'r', encoding='utf-8') as f:
        config.read_file(f)

    path = config.get('TARGET', 'path')
    conn = duckdb.connect(path)
    try:
        df = conn.execute('SELECT * FROM daily_1min LIMIT 100').df()
        print(df)
        return df
    finally:
        conn.close()

def test_query_stock_trade_dates():
    """测试查询指定股票在指定表的交易日期列表"""
    config = configparser.ConfigParser()
    with open('config.ini', 'r', encoding='utf-8') as f:
        config.read_file(f)

    path = config.get('TARGET', 'path')
    duckdb_helper = DuckDBHelper(path)
    trade_dates = duckdb_helper.query_stock_trade_dates("000001.SZ", "daily_1min")
    print(trade_dates)
    return trade_dates

if __name__ == "__main__":
    # test_duckdb_connection_read()
    test_query_stock_trade_dates()