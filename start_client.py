"""
启动QMT数据客户端， 并检查更新本地duckdb数据库

1. 获取股票列表
2. 检查本地数据
3. 获取增量数据
4. 更新数据库
"""

import configparser
import os
import sys

from qka.client import QMTDataClient

if __name__ == "__main__":
    # 读取配置文件
    config = configparser.ConfigParser()
    with open('config.ini', 'r', encoding='utf-8') as f:
        config.read_file(f)

    qmt_server_config = config.get('QMT-SERVER', 'base_url')
    qmt_server_token = config.get('QMT-SERVER', 'token')

    client = QMTDataClient(base_url=qmt_server_config, token=qmt_server_token)
    stock_list = client.get_stock_list_in_main_board()
    print(stock_list)