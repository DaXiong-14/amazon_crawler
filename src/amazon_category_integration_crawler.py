import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from bs4 import BeautifulSoup

from src.amazon_selection_crawler import crawl_item_info
from tool.pipeline import MySQLPipeline
from tool.utils import _get_site_url, merge_list_of_dicts, SeleniumPool

logger = logging.getLogger(__name__)


def category_integration_master(cid, site):
    """
    亚马逊 类目综合数据 爬虫主方法
    :param cid: 类目ID
    :param site: 站点
    """
    logger.info(f'开始爬取类目 {cid} 综合数据...')
    start_time = datetime.now()

    pool = SeleniumPool(pool_size=10, site=site)

    # todo 异步加载
    items = []
    data_lock = threading.Lock()  # 保护结果列表
    stop_event = threading.Event()  # 用于通知所有线程停止
    # todo 定义一个异步执行方法
    def process_batch_category(c, g, p):
        """
        异步执行方法
        :param c: cid
        :param g: page
        :param p: pool
        """
        try:
            if stop_event.is_set(): # todo 关键
                return False
            items_json = crawl_category_integration(c, g, p)
            with data_lock:
                items.extend(items_json.get('items'))
                pageMax = items_json.get('pageMax')
                if len(items) >= 500:
                    # todo 关键达到500条数据，设置停止事件
                    stop_event.set()
            return True
        except Exception as e:
            logger.error(f"处理失败: {e}")
            raise  # 重新抛出异常以便主线程捕获

    # todo 使用线程池控制并发数
    with ThreadPoolExecutor(max_workers=3) as executor:
        # 提交所有任务
        futures = []
        for page in range(1, 50):  # 假设最多爬取100页
            if stop_event.is_set():
                logger.info("已达到数据上限，停止提交新任务。")
                break
            futures.append(executor.submit(process_batch_category, cid, page, pool))
            time.sleep(random.uniform(2,5))
        # 等待所有任务完成并处理异常
        for future in futures:
            try:
                future.result()  # 获取结果（会抛出线程中的异常）
                if stop_event.is_set():
                    logger.info("已达到数据上限，停止等待其他任务。")
                    executor.shutdown(wait=False)
                    break
            except Exception as e:
                logger.error(f"任务执行出错: {e}")
    # todo 数据去重 排序 重构
    ranked_items = process_and_rank_items(items)
    # todo 获取详细数据
    processed_data = crawl_item_info(ranked_items, pool, site)
    # todo 更新items
    reItems = merge_list_of_dicts(ranked_items, processed_data)

    # todo 释放浏览器
    pool.close_all()

    # todo 计算时间差
    end_time = datetime.now()
    time_diff = end_time - start_time
    logger.info(f'类目 {cid} 综合数据抓取完成！总用时: {time_diff.total_seconds()} 秒')

    # todo 下沉
    # fileJSON = f"data\\category_integration\\{cid}_{site}.json"
    # toJson(reItems, os.path.join(os.getcwd(), fileJSON))

    # todo MySQL 转存
    from config.config import db_config

    # 表结构定义
    product_schema = {
        "id": "INT AUTO_INCREMENT PRIMARY KEY",
        "asin": "VARCHAR(20) NOT NULL UNIQUE",
        "image": "VARCHAR(255)",
        "`rank`": "INT",
        "title": "VARCHAR(500)",
        "rating": "DECIMAL(2,1)",
        "reviewCount": "INT",  # 保持一致
        "current_price": "VARCHAR(20)",
        "discount_percentage": "VARCHAR(20)",
        "original_price": "VARCHAR(20)",
        "material": "TEXT",
        "description": "TEXT",
        "similarList": "TEXT",
        'aliexpress': "TEXT",
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
    }

    # 使用管道
    pipeline = MySQLPipeline(**db_config, pool_size=3)

    try:
        # 批量插入/更新数据
        pipeline.batch_upsert(
            table_name=f"{cid}_{site}",
            data=reItems,
            primary_key="asin",
            batch_size=50,
            schema=product_schema
        )
    finally:
        pipeline.close()

    return reItems



def crawl_category_integration(cid, page, pool):
    """
    爬取亚马逊类目综合数据
    :param cid: 类目ID
    :param page: 页码
    :param pool: selenium 连接池
    """
    baseurl = f'{_get_site_url(pool.site)}/s?rh=n%3A{cid}&fs=true&page={str(page)}'
    logger.info(f'正在访问: {baseurl}')

    pageSource = pool.get_page_source(baseurl).get('pageSource')
    # todo 重试机制
    for _ in range(3):
        if pageSource:
            break
        else:
            pageSource = pool.get_page_source(baseurl).get('pageSource')

    soup = BeautifulSoup(pageSource, 'html.parser')
    items = []
    # todo 解析页面
    itemBoxs = soup.find_all('div', {'role': 'listitem', 'data-component-type': 's-search-result'})
    if not itemBoxs:
        logger.error('页面没有商品数据!')
        # 回调
        return crawl_category_integration(cid, page, pool)
    for itemBox in itemBoxs:
        try:
            asin = itemBox.get('data-asin')
            image = None
            imgBox = itemBox.find('img', class_='s-image')
            if imgBox:
                image = imgBox.get('src')
            items.append({
                'asin': asin,
                'image': image,
                'page': page,
                'cid': cid,
                'index': itemBoxs.index(itemBox) + 1,
            })
        except Exception as e:
            logger.error(f'解析商品信息失败: {e}')
            continue

    maxPage = None
    divPageBox = soup.find_all('div', {'aria-label': 'pagination', 'role': 'navigation'})
    if divPageBox:
        spanBox = soup.select_one('span.s-pagination-item.s-pagination-disabled')
        if spanBox:
            try:
                maxPage = int(spanBox.get_text(strip=True))
            except Exception as e:
                logger.error(f'页码值无效: {e}')

    return {
        'items': items,
        'page': page,
        'maxPage': maxPage,
    }


def process_and_rank_items(items):
    """
    处理items列表：
    1. 根据asin去重（保留最后出现的记录）
    2. 按原规则排序（先page后index）
    3. 删除page和index字段
    4. 添加rank字段（从1开始）
    """
    # 1. 去重 - 使用字典保留最后出现的asin记录
    unique_items = {}
    for item in items:
        unique_items[item['asin']] = item

    # 2. 转换为列表并排序
    deduplicated = list(unique_items.values())
    sorted_items = sorted(deduplicated, key=lambda x: (x['page'], x['index']))

    # 3. 重构数据并添加rank
    ranked_items = []
    for rank, item in enumerate(sorted_items, start=1):
        ranked_items.append({
            'asin': item['asin'],
            'image': item['image'],
            'rank': rank  # 添加rank字段，从1开始
        })

    return ranked_items


