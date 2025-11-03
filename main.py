# todo 项目启动类
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
from datetime import datetime

import requests

from config.config import flask_host, PORT
from tool.JSONToExcel import AmazonExcelExporter
from tool.pipeline import toJson
from tool.utils import  ThreadSafeConstant, SeleniumPool, _get_marketId
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


def selection_start():
    """
    亚马逊 精选商品 爬虫启动方法
    :return:
    """
    print(" 此程序用于卖家精灵选品数据抓取！")
    print(" 传入 类目ID 与 site（US,DE） 参数 ")
    cid = input('请输入 CID: ')
    site = input('请输入 site: ')
    try:
        selection_core(cid, site=site)
    except Exception as e:
        logger.error(f'程序执行失败！{cid} - {e}')


def selection_core(cid, site):
    """
    :param cid: 类目id
    :param site: 站点
    :return:
    """
    logger.info(f'开始抓取 {cid}，{site}, 选品数据.......')
    start_time = datetime.now()
    pool = SeleniumPool(pool_size=8, site=site)
    i_url = 'https://www.sellersprite.com/v2/competitor-lookup/nodes'
    params = {
        'marketId': _get_marketId(site=site),  # 4 德国站
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
    path = f'temp\\selection\\amazon_{cid}_{site}_{current_time}.json'
    toJson(reItems, os.path.join(os.getcwd(), path))

    # todo 计算时间差
    end_time = datetime.now()
    time_diff = end_time - start_time
    logger.info(f'类目 {cid}，{site} 选品数据抓取完成！总用时: {time_diff.total_seconds()} 秒')



def category_integration_start(site="US", method='f'):
    """
    亚马逊 类目整合 爬虫启动方法
    :param site: 站点 默认美国
    :param method: 执行方式 f 为 flask 交互 m为本地
    :return:
    """
    # todo 获取id 配置文件
    with open('config/categoryID.txt', 'r', encoding='utf-8') as f:
        ids = f.read().splitlines()

    if method == 'm':
        # todo 遍历 ids 列表
        for c in ids:
            # todo 调用类目整合主方法
            if c:
                category_id = c.split(',')
                category_integration_master(category_id[0], site=category_id[1])

    if method == 'f':
        result_dict = {}
        for line in ids:
            if line:
                cid, site = line.strip().split(',')
                if site not in result_dict:
                    result_dict[site] = []
                result_dict[site].append({
                    'cid': cid,
                    'site': site
                })

        # todo 提交服务器
        for k in result_dict.keys():
            host = flask_host.get(k)
            url = f'http://{host}:{str(PORT)}/api/crawler/cn'
            response = requests.post(url, json={'data': result_dict[k]})
            if response.status_code != 202:
                logger.error(f'爬虫程序失败！{response.json().get('error')}')



if __name__ == '__main__':
    # selection_start()
    # category_integration_start(method='f')
    with open('temp/cn/1981665031_DE.json', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    datas = [json.loads(line) for line in lines if line]
    try:
        ex = AmazonExcelExporter(filename='temp/df.xlsx', site='DE')
        ex.create_worksheet("产品数据")
        for item in datas:
            ex.add_product_data(item)
        ex.save()
        ex.close()

    except Exception as e:
        logger.error(f'转存成 excel 失败！{e}')

