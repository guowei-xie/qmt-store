"""
测试DuckDB连接/读取功能
"""

import duckdb
import configparser

config = configparser.ConfigParser()
with open('config.ini', 'r', encoding='utf-8') as f:
    config.read_file(f)

# 连接到DuckDB
path = config.get('TARGET', 'path')
conn = duckdb.connect(path)

# 读取表
df = conn.execute('SELECT * FROM daily_1min LIMIT 100').df()
print(df)

# 关闭连接
conn.close()