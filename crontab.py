"""
定时任务
"""

import configparser
import os
import sys

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from qka.data import download_stock_history_data, get_stock_list_in_main_board

if __name__ == "__main__":
    print("定时任务:每日15:30下载历史数据")
    stock_list = get_stock_list_in_main_board()
    current_date = datetime.now().strftime("%Y%m%d")
    download_stock_history_data(stock_list, start_time=current_date, end_time=current_date, period="1d")
    download_stock_history_data(stock_list, start_time=current_date, end_time=current_date, period="1m")