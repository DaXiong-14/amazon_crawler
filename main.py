# todo 项目启动类
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
from datetime import datetime

import requests

from src.amazon_listing_crawler import crawl_search_results
from src.amazon_product_extractor import get_product_details
from tool.pipeline import toJson
from tool.utils import _fetch_category_data, ThreadSafeConstant, SeleniumPool
from src.amazon_category_integration_crawler import category_integration_master
from src.amazon_selection_crawler import selection_master, selection_slave


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler(f'project.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

setup_logging()
logger = logging.getLogger(__name__)


def ranking_start(site="US"):
    """
    亚马逊 畅销排名 与新品排名 爬虫启动方法
    :param site: 站点
    :return:
    """
    # todo 调用 函数 生成类目列表
    category_datalist = _fetch_category_data(site)

    for category_data in category_datalist:
        rank_core(category_data)


def rank_core(datajson, site="US"):
    """
    :param datajson:
    :param site:
    :return:
    """
    # todo 获取类目列表数据
    result_json = crawl_search_results(datajson['baseurl'], site=site)
    cookies = result_json['cookies']
    results = result_json['data']

    # todo 初始化"常量" （异步 cookie 值）
    SAFE_CONST = ThreadSafeConstant(cookies)

    # todo 定义数据 存储列表
    processed_data = []
    data_lock = threading.Lock()  # 保护结果列表

    # todo 定义一个异步执行方法
    def process_batch(result):
        """
        异步执行方法
        :param result:
        """
        try:
            data = get_product_details(result, SAFE_CONST.cookies, site)
            if 'cookies' in data:
                SAFE_CONST.update(data['cookies'])
            with data_lock:
                processed_data.append(data['data'])
            return data['data']
        except Exception as e:
            logger.error(f"处理失败: {e}")
            raise  # 重新抛出异常以便主线程捕获

    # todo 使用线程池控制并发数
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 提交所有任务
        futures = [executor.submit(process_batch, result.updata({
            'category_id': datajson['id'],
            'category': datajson['category'],
            'bs': datajson['bs'],
        })) for result in results]
        # 等待所有任务完成并处理异常
        for future in as_completed(futures):
            try:
                future.result()  # 获取结果（会抛出线程中的异常）
            except Exception as e:
                logger.error(f"任务执行出错: {e}")

    # todo 调用数据存储管道



def selection_start():
    """
    亚马逊 精选商品 爬虫启动方法
    :return:
    """
    # todo 初始化 selenium 连接池
    # todo 获取id 配置文件
    with open('config/categoryID.txt', 'r', encoding='utf-8') as f:
        ids = f.read().splitlines()
    # todo 遍历 ids 列表
    for c in ids:
        # todo 调用类目整合主方法
        if c:
            category_id = c.split(',')
            selection_core(category_id[0], category_id[1])


def selection_core(cid, site):
    """
    :param cid: 类目id
    :param site: 站点
    :return:
    """
    logger.info(f'开始抓取 {cid}，{site}, 选品数据.......')
    start_time = datetime.now()
    pool = SeleniumPool(pool_size=12, site=site)
    i_url = 'https://www.sellersprite.com/v2/competitor-lookup/nodes'
    params = {
        'marketId': 4,  # 4 德国站
        'able': 'bsr_sales_nearly',
        'nodeLabelPath': cid
    }
    res = requests.get(i_url, params=params)
    resJson = res.json()
    if not resJson.get('items'):
        return
    categoryItem = None
    for item in resJson.get('items'):
        if cid == item.get('id').split(':')[-1]:
            categoryItem = item
            break
    if not categoryItem:
        return
    pageMax = 0
    if int(categoryItem.get('products')) >= 500:
        pageMax = 5
    else:
        pageMax = int(int(categoryItem.get('products')) // 100) + 1
    items = []
    conf = {
        'site': site,
        'category_id': categoryItem.get('id'),
        'maxVariations': 10,
        'maxWeights': 960,
        'category_name': categoryItem.get('label').split(':')[-1],
    }
    for i in (1, pageMax + 1):
        conf['page'] = i
        newItems = selection_master(conf, items)
        # todo 合并数据
        items.extend(newItems)

    reItems = selection_slave(conf, pool, items)
    pool.close_all()
    # todo 调用存储管道
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = f'data\\selection\\amazon_{cid}_{site}_{current_time}.json'
    toJson(reItems, os.path.join(os.getcwd(), path))

    # todo 计算时间差
    end_time = datetime.now()
    time_diff = end_time - start_time
    logger.info(f'类目 {cid}，{site} 选品数据抓取完成！总用时: {time_diff.total_seconds()} 秒')



def category_integration_start(site="US"):
    """
    亚马逊 类目整合 爬虫启动方法
    :param site: 站点 默认美国
    :return:
    """
    # todo 获取id 配置文件
    with open('config/categoryID.txt', 'r', encoding='utf-8') as f:
        ids = f.read().splitlines()
    # todo 遍历 ids 列表
    for c in ids:
        # todo 调用类目整合主方法
        if c:
            category_id = c.split(',')
            category_integration_master(category_id[0], site=category_id[1])




if __name__ == '__main__':
    # selection_start()
    # category_integration_start()
    # _uc_amazon_product('https://www.amazon.de/s?rh=n%3A1981774031&fs=true&page=5')
    # _selenium_amazon_product('https://www.amazon.de')
    # a = AliExpressCrawler()
    # a.search_by_image('https://m.media-amazon.com/images/I/71leFiNSN9L._AC_SX679_.jpg')

    print(json.dumps({'config':'1'}))



