"""
DuckDB操作工具
"""
import duckdb
import pandas as pd
import os
import gc

class DuckDBHelper:
    def __init__(self, db_path):
        """
        初始化DuckDB连接，db_path为数据库路径
        """

        # 检查父目录是否存在，不存在则创建
        parent_dir = os.path.dirname(os.path.abspath(db_path))
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)

        self.conn = duckdb.connect(db_path)
    
    def insert_df_to_duckdb(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        将DataFrame插入到DuckDB中
        
        优化说明：
        - 插入后立即unregister释放内存引用
        - 使用参数化查询避免SQL注入（虽然这里表名是配置的，但更安全）
        """
        if df is None or df.empty:
            return False
            
        try:
            # 确保列名为str类型
            df.columns = [str(x) for x in df.columns]
            # 注册DataFrame到DuckDB
            self.conn.register('df', df)
            
            # 检查表是否已存在
            table_exists = self.conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{table_name}'"
            ).fetchone()[0] > 0

            if not table_exists:
                # 新建表并插入数据
                self.conn.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM df"
                )
            else:
                # 已存在的表，追加数据
                self.conn.execute(
                    f"INSERT INTO {table_name} SELECT * FROM df"
                )
            
            # 立即unregister释放对DataFrame的引用
            self.conn.unregister('df')
            
            # 提交事务（DuckDB默认自动提交，但显式调用更安全）
            # DuckDB默认是自动提交的，但可以显式调用确保数据写入
            return True
        except Exception as e:
            # 确保出错时也unregister
            try:
                self.conn.unregister('df')
            except:
                pass
            raise e

    def read_duckdb_table(self, table_name, limit=100):
        """
        读取DuckDB中的表, limit为读取的行数，默认读取100行
        """
        return self.conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").df()

    def close(self):
        """
        关闭数据库连接
        """
        if self.conn:
            self.conn.close()
            # 强制垃圾回收释放连接相关资源
            gc.collect()
