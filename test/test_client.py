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

# 读取配置文件中的 base_url 和 token
config = configparser.ConfigParser()
config.read(os.path.join(project_root, 'config.ini'))
base_url = config['QMT-SERVER']['base_url']
token = config['QMT-SERVER']['token']


def test_get_daily_bars():
    """测试获取日线数据"""
    client = QMTDataClient(base_url=base_url, token=token)
    stock_list = ['000001.SZ', '000002.SZ']
    daily_bars = client.get_daily_bars(stock_list, period='1d', start_time='20251103', end_time='20251103')
    print(daily_bars)
    return daily_bars


def test_get_stock_list_in_main_board():
    """测试获取主板股票列表"""
    client = QMTDataClient(base_url=base_url, token=token)
    stock_list = client.get_stock_list_in_main_board()
    print(stock_list)
    return stock_list


if __name__ == "__main__":
    # 测试获取日线数据
    test_get_daily_bars()
    
    # 测试获取主板股票列表（取消注释即可测试）
    # test_get_stock_list_in_main_board()