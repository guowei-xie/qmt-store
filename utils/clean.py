"""
读取并清洗CSV数据，将数据结构转换为同QMT一致
"""

import pandas as pd
import os
# 递归获取所有CSV文件
def get_all_csv_files(root_path):
    """
    递归获取指定路径下所有CSV文件的完整路径
    
    Args:
        root_path: 根目录路径
        
    Returns:
        list: 所有CSV文件的完整路径列表
    """
    csv_files = []
    for root, dirs, files in os.walk(root_path):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

def clean_data(df):
    """
    清洗CSV数据，将数据结构转换为同QMT一致
    
    优化说明：
    - 避免使用 .copy() 以减少内存占用
    - 使用向量化操作替代 apply() 提高效率
    """
    # 选择需要的列（不创建副本，直接视图）
    df = df[['代码', '时间', '开盘价', '最高价', '最低价', '收盘价', '成交量', '成交额']]

    # 重命名列（原地操作，不创建副本）
    df.columns = ['code', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount']

    # 将time列转换为datetime类型
    df['time'] = pd.to_datetime(df['time'])

    # time列为QMT兼容的13位毫秒时间戳
    df['time'] = (df['time'].astype("int64") // 10**6)  # datetime64是纳秒为单位

    # code格式转换为QMT兼容的格式，使用向量化操作替代apply提高效率
    # 取code前两位转换为大写后放置在数值后方
    if len(df) > 0:
        # 使用向量化操作处理code列，避免apply的开销
        code_series = df['code'].astype(str)
        mask = code_series.str.len() > 2
        df.loc[mask, 'code'] = code_series[mask].str[2:] + '.' + code_series[mask].str[:2].str.upper()
        df.loc[~mask, 'code'] = code_series[~mask].str.upper()

    return df

