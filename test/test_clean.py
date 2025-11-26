"""
测试数据清洗功能
"""


import pandas as pd
import os
import sys

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.tools import date_to_timestamp, timestamp_to_date

def test_date_to_timestamp():
    """测试日期字符转换为时间戳"""
    date_str = "20251126"
    timestamp = date_to_timestamp(date_str)
    print(f"日期字符 {date_str} 转换为时间戳: {timestamp}")
    return timestamp

def test_timestamp_to_date():
    """测试时间戳转换为日期字符"""
    timestamp = 1737753600000
    date_str = timestamp_to_date(timestamp)
    print(f"时间戳 {timestamp} 转换为日期字符: {date_str}")
    return date_str

if __name__ == "__main__":
    test_date_to_timestamp()
    test_timestamp_to_date()