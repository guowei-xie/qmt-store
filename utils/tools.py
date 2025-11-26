# 日期字符转换为时间戳，如日期字符 20251126 转换为时间戳: 1764115200000
def date_to_timestamp(date_str):
    """
    日期字符转换为时间戳
    Args:
        date_str: 日期字符，如“20251126”
    Returns:
        int: 时间戳（毫秒级）
    """
    # 明确指定format防止不规则解析
    ts = pd.to_datetime(date_str, format='%Y%m%d', errors='raise')
    # 转为纳秒后转毫秒（注意astype("int64")是纳秒）
    return int(ts.value // 10**6)

# 时间戳转换为日期字符，如时间戳 1737753600000 转换为日期字符: 20250124
def timestamp_to_date(timestamp):
    """
    时间戳转换为日期字符
    Args:
        timestamp: 时间戳（毫秒级）
    Returns:
        str: 日期字符
    """
    # 必须是整数类型（毫秒级）
    ts = pd.to_datetime(int(timestamp), unit='ms', errors='raise')
    return ts.strftime('%Y%m%d')

# 比对列表元素，从B列表中删除A列表中的元素
def compare_lists(list_a, list_b):
    """
    比对列表元素，从B列表中删除A列表中的元素
    Args:
        list_a: 列表A
        list_b: 列表B
    Returns:
        list: 列表B
    """
    return list(set(list_b) - set(list_a))
