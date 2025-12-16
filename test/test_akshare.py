import akshare as ak
import os
import sys

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from utils.clean import clean_index_data

def test_get_akshare_index_data():
    """测试获取akshare指数数据"""
    start_date = '20200101'
    end_date = '20251125'
    df = ak.stock_zh_index_daily_em(symbol="sh000001", start_date=start_date, end_date=end_date)
    df['code'] = 'sh000001'
    df = clean_index_data(df)
    print(df)

if __name__ == "__main__":
    test_get_akshare_index_data()