"""
初始化数据库，将1分钟数据 CSV 文件导入数据库
"""

import configparser
import os
import gc
import pandas as pd
from utils.clean import clean_data, get_all_csv_files
from utils.duckdb import DuckDBHelper
from tqdm import tqdm

# 读取配置文件
config = configparser.ConfigParser()
with open('config.ini', 'r', encoding='utf-8') as f:
    config.read_file(f)

# 读取源路径
source_path = config.get('SOURCE', 'path')

# 读取目标地址
target_path = config.get('TARGET', 'path')

# 分块读取大小（可根据内存情况调整，单位：行数）
CHUNK_SIZE = 50000  # 每次读取5万行

# 获取所有CSV文件
source_files = get_all_csv_files(source_path)

# 初始化DuckDBHelper
duckdb_helper = DuckDBHelper(target_path)

# 遍历源文件，插入指定库表中（1min数据），添加进度条
for file_path in tqdm(source_files, desc="导入CSV文件进度"):
    # 使用分块读取大文件，避免一次性加载到内存
    try:
        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
            # 清洗数据块
            chunk = clean_data(chunk)
            # 插入数据块到DuckDB
            duckdb_helper.insert_df_to_duckdb(chunk, config.get('TARGET', 'min_table'))
            # 显式删除chunk以释放内存
            del chunk
            # 强制垃圾回收
            gc.collect()
    except Exception as e:
        print(f"处理文件 {file_path} 时出错: {e}")
        continue

# 关闭DuckDB连接
duckdb_helper.close()
        
