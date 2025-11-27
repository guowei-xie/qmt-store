"""
服务端运行补全QMT数据客户端的数据
"""

import configparser

from qka.data import get_stock_list_in_main_board, download_stock_history_data

def load_config(config_path='config.ini'):
    """加载配置文件"""
    config = configparser.ConfigParser()
    with open(config_path, 'r', encoding='utf-8') as f:
        config.read_file(f)
    return config

if __name__ == "__main__":
    config = load_config()
    remote_1d_start_date = config.get('COMPLETION', 'remote_1d_start_date')
    remote_1m_start_date = config.get('COMPLETION', 'remote_1m_start_date')
    print(f"远端1d数据开始日期: {remote_1d_start_date}")
    print(f"远端1m数据开始日期: {remote_1m_start_date}")
    stock_list = get_stock_list_in_main_board()
    print(f"待补全主板股票数量: {len(stock_list)}")
    print(f"开始下载远端1d数据")
    download_stock_history_data(stock_list, remote_1d_start_date, period="1d")
    print(f"开始下载远端1m数据")
    download_stock_history_data(stock_list, remote_1m_start_date, period="1m")
    print(f"补全完成")