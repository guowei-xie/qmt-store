"""
测试数据模块
"""
import sys
from pathlib import Path

# 确保项目根目录在模块搜索路径中
CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import qka.data as qd
import pandas as pd


def test_get_stock_data_range():
    """测试获取指定股票的历史数据范围"""
    stock_code = "000001.SZ"
    period = "1d"
    start_time = "20250101"
    end_time = "20251126"
    stock_trade_dates = qd.get_stock_data_range(stock_code, period, start_time, end_time)
    print(f"股票 {stock_code} 在 {start_time} 到 {end_time} 期间的交易日期:")
    print(stock_trade_dates)
    return stock_trade_dates


def main():
    """主测试函数"""
    test_get_stock_data_range()


if __name__ == "__main__":
    main()