import logging
import os

import pymysql
import json
from datetime import datetime, date
from config.config import db_config
from src.amazon_selection_crawler import selection_slave
from tool.JSONToExcel import AmazonExcelExporter
from tool.pipeline import toJson

logger = logging.getLogger(__name__)


def queryMaster(cid, site):
    """
    :param cid:
    :param site:
    :param p:
    :return:
    """

    json_data = query_data_to_json_list(
        host=db_config.get('host'),
        user=db_config.get('user'),
        password=db_config.get('password'),
        database=db_config.get('database'),
        table_name=f'{cid}_{site}'
    )
    conf = {
        'cid': cid,
        'site': site,
    }

    items = selection_slave(conf, json_data)

    # todo 下沉 文件
    fileJSON = os.path.join(os.getcwd(), f'temp\\cn\\{cid}_{site}.json')
    toJson(items, fileJSON, wb="w")

    try:
        filename = os.path.join(os.getcwd(), f'temp\\cn\\{cid}_{site}.xlsx')
        ex = AmazonExcelExporter(filename=filename)
        ex.create_worksheet("产品数据")
        for item in items:
            ex.add_product_data(json.loads(item))
        ex.save()
        ex.close()

    except Exception as e:
        logger.error(f'转存成 excel 失败！{e}')



def query_data_to_json_list(host, user, password, database, table_name):
    """
    查询数据库表并按条件转换为JSON list
    参数:
        host: 数据库主机
        user: 用户名
        password: 密码
        database: 数据库名
        table_name: 表名
    """
    connection = None
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection.cursor() as cursor:
            # 使用更简单的查询，避免字段名冲突
            sql = f"""
                WITH RankedData AS (
                    SELECT 
                    *,
                    -- 主排序：按 rank 升序
                    -- 组内次级排序：优先取 description 不为 NULL 的记录（标志为0）
                    -- 组内最终排序：按更新时间取最早的一条
                    ROW_NUMBER() OVER (
                        PARTITION BY `rank` 
                        ORDER BY 
                        CASE WHEN description IS NOT NULL THEN 0 ELSE 1 END ASC,
                        COALESCE(updated_at, '1970-01-01') ASC
                    ) AS rn
                    FROM {table_name}
                )
                SELECT 
                *
                FROM RankedData
                WHERE rn = 1
                ORDER BY `rank` ASC; -- 最终结果再按 rank 排序以确保整体顺序
                """
            cursor.execute(sql)
            results = cursor.fetchall()

            json_list = []
            for row in results:
                processed_row = {}
                for key, value in row.items():
                    if isinstance(value, (datetime, date)):
                        processed_row[key] = value.isoformat()
                    else:
                        processed_row[key] = value
                json_list.append(processed_row)

            return json_list

    except pymysql.Error as e:
        logger.error(f"数据库查询错误: {e}")
        return []
    finally:
        if connection:
            connection.close()
