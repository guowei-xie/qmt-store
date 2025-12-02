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
    
    def insert_df_to_duckdb(self, df: pd.DataFrame, table_name: str, overwrite: bool = False) -> bool:
        """
        将DataFrame插入到DuckDB中

        参数:
            df: 要插入的pandas DataFrame
            table_name: 目标表名
            overwrite: 是否覆盖写入（为True时会删除原表并重建）

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
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?", [table_name]
            ).fetchone()[0] > 0

            if overwrite and table_exists:
                # 覆盖写入：先删除原表
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                table_exists = False  # 此时视为不存在，转下方新建逻辑

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

        # 查询所有time（毫秒时间戳），在SQL侧完成去重与日期转换，避免DataFrame开销
        # 注意：DuckDB的strftime参数顺序是(TIMESTAMP, FORMAT)，且返回格式需与timestamp_to_date一致(%Y%m%d)
        query = (
            f"SELECT DISTINCT strftime(to_timestamp(time::DOUBLE / 1000), '%Y%m%d') AS trade_date "
            f"FROM {table_name} WHERE code = ? AND time IS NOT NULL ORDER BY trade_date"
        )
        rows = self.conn.execute(query, [stock_code]).fetchall()
        # fetchall返回list[tuple]，避免额外的DataFrame构建
        return [row[0] for row in rows if row and row[0]]

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
