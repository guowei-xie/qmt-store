"""
数据工具模块
提供数据处理和获取功能
"""

import pandas as pd
import akshare as ak
from xtquant import xtdata
from tqdm import tqdm

def add_stock_suffix(stock_code):
    """
    为给定的股票代码添加相应的后缀
    Args:
        stock_code: 股票代码，可以带后缀如.SH/.SZ，也可以不带
    Returns:
        str: 添加后缀的股票代码，如'000001.SH'
    """
    # 如果已经有后缀，直接返回
    if '.' in stock_code:
        return stock_code
        
    # 检查股票代码是否为6位数字
    if len(stock_code) != 6 or not stock_code.isdigit():
        raise ValueError("股票代码必须是6位数字")

    # 根据股票代码的前缀添加相应的后缀
    if stock_code.startswith(("00", "30", "15", "16", "18", "12")):
        return f"{stock_code}.SZ"  # 深圳证券交易所
    elif stock_code.startswith(("60", "68", "11")):
        return f"{stock_code}.SH"  # 上海证券交易所
    elif stock_code.startswith(("83", "43")):
        return f"{stock_code}.BJ"  # 北京证券交易所
    
    return f"{stock_code}.SH"  # 默认为上海证券交易所

def add_stock_suffix_list(stock_list: list) -> list:
    """
    为给定的股票代码列表添加相应的后缀
    Args:
        stock_list: 股票代码列表
    Returns:
        list: 添加后缀的股票代码列表
    """
    return [add_stock_suffix(stock_code) for stock_code in stock_list]

def get_stock_market_type(stock_code: str) -> str:
    """
    根据股票代码判断股票所属市场类型
    Args:
        stock_code: 股票代码，可以带后缀如.SH/.SZ，也可以不带
    Returns:
        str: 市场类型，'主板'/'创业板'/'科创板'/'北交所'
    """
    symbol = add_stock_suffix(stock_code)
    if '.' in symbol:
        symbol = symbol.split('.')[0]
    if symbol.startswith('688') or symbol.startswith('689'):
        return '科创板'
    elif symbol.startswith('30'):
        return '创业板'
    elif symbol.startswith('83'):
        return '北交所'
    else:
        return '主板'

# 获取交易日历
def get_trade_calendar(start_time: str, end_time: str, format: str = 'number') -> list:
    """
    获取交易日历
    Args:
        start_time: 开始时间，格式为'number'或'str'
        end_time: 结束时间，格式为'number'或'str'
        format: 格式，'number'或'str'
            'number': 返回日期为数字格式，如20240101
            'str': 返回日期为字符串格式，如'2024-01-01'
    Returns:
        list: 交易日历，格式为'number'或'str'
    """
    start = pd.to_datetime(start_time)
    end = pd.to_datetime(end_time)
    
    try:
        dates = ak.tool_trade_date_hist_sina()['trade_date']
    except Exception as e:
        raise RuntimeError(f"调用akshare交易日历接口失败: {e}")

    dates = pd.to_datetime(dates)
    mask = (dates >= start) & (dates <= end)

    if format == 'number':
        return [dt.strftime('%Y%m%d') for dt in dates[mask]]
    elif format == 'str':
        return [dt.strftime('%Y-%m-%d') for dt in dates[mask]]
    else:
        raise ValueError(f"无效的格式: {format}")

# 获取板块成分股
def get_stock_list_in_sector(sector_name: str) -> list:
    """
    获取板块成分股
    Args:
        sector_name: 板块名称(如: '沪深A股')
    Returns:
        list: 板块成分股代码列表
    """
    try:
        stock_list = xtdata.get_stock_list_in_sector(sector_name)
        return stock_list
    except Exception as e:
        raise RuntimeError(f"获取板块成分股失败: {e}")

def get_stock_list_in_main_board() -> list:
    """
    获取沪深A股主板成分股
    Returns:
        list: 沪深A股主板成分股代码列表
    """
    try:
        sector_name = '沪深A股'
        stock_list = get_stock_list_in_sector(sector_name)
        stock_list = [stock for stock in stock_list if get_stock_market_type(stock) == '主板']
        return stock_list
    except Exception as e:
        raise RuntimeError(f"获取{sector_name}主板成分股失败: {e}")

# 下载股票历史数据
def download_stock_history_data(stock_list: list, start_time: str, end_time: str = '', period: str = '1d', process_bar: bool = True) -> bool:
    """
    下载股票历史K线数据
    Args:
        stock_list: 股票代码列表
        start_time: 开始时间
        period: 周期
            '1d': 日线(默认)
            '1m': 1分钟线
        process_bar: 进度条显示，默认显示
    Returns:
        bool: 是否成功
    """
    if not stock_list:
        raise ValueError(f"股票列表为空")

    if not start_time:
        raise ValueError(f"开始时间不能为空")
    
    if not period:
        raise ValueError(f"周期不能为空")
    
    iterator = tqdm(stock_list, desc=f"下载历史数据", ncols=100, colour="green") if process_bar else stock_list
    for code in iterator:
        xtdata.download_history_data(code, period=period, start_time=start_time, end_time=end_time, incrementally=True)
    return True

# 获取行情数据
def get_daily_bars(stock_list: list, period: str = '1d', start_time: str = '', end_time: str = '', count: int = -1) -> dict:
    """
    获取行情数据
    Args:
        stock_list: 股票列表
        period: 周期
        start_time: 开始时间
        end_time: 结束时间
        count: 数量
    Returns:
        dict: 行情数据
    """
    try:
        dict_data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=add_stock_suffix_list(stock_list),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type='none',
            fill_data=True
        )

        # 清洗数据，价格字段保留两位小数
        for stock in dict_data:
            for field in dict_data[stock]:
                if field in ['open', 'high', 'low', 'close', 'preClose']:
                    dict_data[stock][field] = dict_data[stock][field].astype(float).round(2)
        return dict_data
    except Exception as e:
        raise RuntimeError(f"获取行情数据失败: {e}")
