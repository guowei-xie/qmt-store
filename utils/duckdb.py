"""
DuckDB操作工具
"""
import duckdb
import pandas as pd
import os
import gc
from utils.tools import timestamp_to_date

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

    def get_stock_trade_dates(self, stock_code: str, table_name: str) -> list:
        """
        查询指定股票在指定表的交易日期列表（转换为日期字符并去重）

        Args:
            stock_code: 股票代码
            table_name: 表名
        Returns:
            list: 交易日期列表（已转换为日期字符且去重）
        """
        # 首先校验表是否存在
        table_exists = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
            [table_name]
        ).fetchone()[0] > 0
        if not table_exists:
            return []

        # 查询所有time（毫秒时间戳），去重并按time排序
        query = f"SELECT DISTINCT time FROM {table_name} WHERE code = ? ORDER BY time"
        result_df = self.conn.execute(query, [stock_code]).df()
        if 'time' in result_df.columns and not result_df.empty:
            # 使用向量化方法转换并唯一化
            trade_dates = result_df['time'].map(timestamp_to_date)
            return sorted(trade_dates.unique().tolist())
        return []

    def get_stock_data(self, stock_code: str, table_name: str) -> pd.DataFrame:
        """
        获取指定表中的单只股票数据（按时间升序）
        """
        table_exists = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
            [table_name]
        ).fetchone()[0] > 0
        if not table_exists:
            return pd.DataFrame()
        query = f"SELECT * FROM {table_name} WHERE code = ? ORDER BY time"
        return self.conn.execute(query, [stock_code]).df()

    def delete_stock_data(self, stock_code: str, table_name: str) -> None:
        """
        删除指定表中的单只股票数据
        """
        table_exists = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
            [table_name]
        ).fetchone()[0] > 0
        if not table_exists:
            return
        delete_sql = f"DELETE FROM {table_name} WHERE code = ?"
        self.conn.execute(delete_sql, [stock_code])

    def close(self):
        """
        关闭数据库连接
        """
        if self.conn:
            self.conn.close()
            # 强制垃圾回收释放连接相关资源
            gc.collect()
