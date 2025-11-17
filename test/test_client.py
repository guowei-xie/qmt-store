"""
测试客户端
"""
import os
import sys
import configparser

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from qka.client import QMTDataClient

if __name__ == "__main__":
    client = QMTDataClient(base_url='http://127.0.0.1:8000', token='8a46748832f63cea21f79f6d2577403422c31a83123ee2868af4ab5d92773153')
    # stock_list = client.get_stock_list_in_main_board()
    # print(stock_list)
    stock_list = ['000001.SZ', '000002.SZ']
    daily_bars = client.get_daily_bars(stock_list, period='1d', start_time='20251103', end_time='20251103')
    print(daily_bars)