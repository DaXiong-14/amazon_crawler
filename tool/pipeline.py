import json
import logging
from typing import Any
from typing import List, Dict, Optional
import pymysql
from pymysql import Error

from contextlib import contextmanager
from mysql.connector import Error

from config.config import db_config

logger = logging.getLogger(__name__)

def toJson(data: List[Dict[str, Any]], filename: str, wb="a"):
    """
    将数据写入 JSON 文件
    :param data: 数据列表
    :param filename: 文件名
    :param wb: 文件保存方式
    """
    f = open(filename, wb, encoding='utf-8')
    for item in data:
        try:
            json_str = json.dumps(item, ensure_ascii=False)
            f.write(json_str + '\n')
        except Exception as e:
            logger.error(f"序列化数据失败: {e}")
            continue
    f.close()


class MySQLPipeline:
    """
    MySQL 数据管道类 (PyMySQL 实现)

    功能特点:
    - 连接池管理
    - 批量插入/更新
    - 自动重试机制
    - 事务支持
    - 错误处理和日志记录
    """

    def __init__(self, host: str, user: str, password: str, database: str,
                 port: int = 3306, charset: str = 'utf8mb4',
                 pool_size: int = 5):
        """
        初始化MySQL管道

        参数:
            host: 数据库主机
            user: 用户名
            password: 密码
            database: 数据库名
            port: 端口号，默认3306
            charset: 字符集，默认utf8mb4
            pool_size: 连接池大小
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.charset = charset
        self.pool_size = pool_size
        self.connection_pool = []
        self._initialize_pool()

    def _initialize_pool(self):
        """初始化连接池"""
        try:
            for _ in range(self.pool_size):
                conn = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    port=self.port,
                    charset=self.charset,
                    cursorclass=pymysql.cursors.DictCursor
                )
                self.connection_pool.append(conn)
            logger.info(f"MySQL连接池初始化成功，大小: {self.pool_size}")
        except Error as e:
            logger.error(f"初始化MySQL连接池失败: {e}")
            self._close_all_connections()
            raise

    def _close_all_connections(self):
        """关闭所有连接"""
        for conn in self.connection_pool:
            try:
                conn.close()
            except:
                pass
        self.connection_pool = []

    @contextmanager
    def get_connection(self):
        """
        从连接池获取连接的上下文管理器

        使用示例:
        with pipeline.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM table")
        """
        if not self.connection_pool:
            raise Error("连接池未初始化或已耗尽")

        conn = self.connection_pool.pop()
        try:
            yield conn
        finally:
            self.connection_pool.append(conn)

    def create_table_if_not_exists(self, table_name: str, schema: Dict):
        """
        创建表（如果不存在）

        参数:
            table_name: 表名
            schema: 表结构定义字典
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # 构建创建表的SQL
                    columns = []
                    for col_name, col_def in schema.items():
                        if col_name == "rank":
                            col_name = "`rank`"
                        columns.append(f"{col_name} {col_def}")

                    create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {', '.join(columns)}
                    ) ENGINE=InnoDB DEFAULT CHARSET={self.charset};
                    """

                    cursor.execute(create_sql)
                    conn.commit()
                    logger.info(f"表 {table_name} 已创建或已存在")

                except Error as e:
                    conn.rollback()
                    logger.error(f"创建表 {table_name} 失败: {e}")
                    raise

    def batch_upsert(self, table_name: str, data: List[Dict],
                     primary_key: str = 'asin',
                     batch_size: int = 100,
                     schema: Optional[Dict] = None):
        """
        批量插入/更新数据（UPSERT操作）

        参数:
            table_name: 表名
            data: 要处理的数据列表
            primary_key: 主键字段名
            batch_size: 每批次处理的数据量
            schema: 表结构定义（可选，用于自动创建表）
        """
        if not data:
            logger.info("没有数据需要处理")
            return

        if schema:
            self.create_table_if_not_exists(table_name, schema)

        total_records = len(data)
        processed = 0
        batches = (total_records // batch_size) + 1

        logger.info(f"开始处理 {total_records} 条记录，分 {batches} 批次")

        for i in range(0, total_records, batch_size):
            batch = data[i:i + batch_size]
            self._process_batch(table_name, batch, primary_key)
            processed += len(batch)
            logger.info(f"进度: {processed}/{total_records} ({processed / total_records:.1%})")

        logger.info(f"数据处理完成，共处理 {total_records} 条记录")

    def _process_batch(self, table_name: str, batch: List[Dict], primary_key: str = 'asin'):
        """
        处理单批次数据（根据asin更新规则）

        参数:
            table_name: 表名
            batch: 当前批次数据
            primary_key: 主键字段名，默认为'asin'
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # 开始事务
                    conn.begin()

                    if not batch:
                        return

                    # 获取所有字段名（排除主键）
                    all_fields = [k for k in batch[0].keys() if k != primary_key]

                    # 构建动态的ON DUPLICATE KEY UPDATE部分
                    update_clauses = ["`rank`=VALUES(`rank`)"]  # rank必须更新

                    for field in all_fields:
                        if field != 'rank':  # 已处理rank字段
                            # 对于其他字段，只有当新值不为NULL时才更新
                            update_clauses.append(
                                f"`{field}`=IF(VALUES(`{field}`) IS NOT NULL, VALUES(`{field}`), `{field}`)")

                    update_clause = ', '.join(update_clauses)

                    # 构建完整的INSERT ... ON DUPLICATE KEY UPDATE语句
                    placeholders = ', '.join(['%s'] * len(all_fields))

                    insert_sql = f"""
                    INSERT INTO {table_name} (`{primary_key}`, {', '.join([f"`{f}`" for f in all_fields])})
                    VALUES (%s, {placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause}
                    """

                    # 准备批量数据
                    batch_values = []
                    for item in batch:
                        values = [item.get(primary_key)]
                        values.extend([item.get(f) for f in all_fields])
                        batch_values.append(values)

                    # 执行批量操作
                    cursor.executemany(insert_sql, batch_values)
                    conn.commit()

                    logger.info(f"成功处理批次: {len(batch)} 条记录（智能更新模式）")

                except Error as e:
                    conn.rollback()
                    logger.error(f"处理批次失败: {e}")
                    raise


    def execute_query(self, query: str, params: Optional[tuple] = None):
        """
        执行查询并返回结果

        参数:
            query: SQL查询语句
            params: 查询参数

        返回:
            查询结果列表
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(query, params or ())
                    result = cursor.fetchall()
                    return result
                except Error as e:
                    logger.error(f"执行查询失败: {e}")
                    raise

    def close(self):
        """关闭所有连接"""
        self._close_all_connections()
        logger.info("MySQL连接池已关闭")

