# todo 亚马逊选品爬虫
import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from tool.SLC import login_sellersprite
from src.amazon_product_extractor import processing_title, processing_image, processing_CustomerReviews, processingPrices, \
    processing_description
from tool.keywords_amount_utils import export_token
from tool.utils import _read_user, fetch_amazon_selection_data, fetch_amazon_detailed_data, _get_site_url, \
    merge_list_of_dicts, ThreadSafeConstant, get_amazon_product
from src.AliExpressCrawler import AliExpressCrawler



logger = logging.getLogger(__name__)

def selection_master(conf: dict, reItems):
    """
    :param conf: 配置文件
    :param reItems: 重构的 items
    :return:
    """
    # todo 1. 生成请求 params
    params = {
        "market": conf.get('site'),
        "page": conf.get('page'),
        "size": 100,
        "symbolFlag": True,
        "monthName": "bsr_sales_nearly",
        "selectType": "2",
        "filterSub": False,
        "weightUnit": "g",
        "order": {
            "field": "bsr_rank",
            "desc": False
        },
        "productTags": [],
        "nodeIdPaths": [
            conf.get('category_id'),
        ],
        "sellerTypes": [],
        "eligibility": [],
        "pkgDimensionTypeList": [],
        "sellerNationList": [],
        "smallAndLight": "N",
        "lowPrice": "N",
        "putawayMonth": "6",
        'maxVariations': conf.get('maxVariations'),
        'maxWeights': conf.get('maxWeights')
    }

    # todo 2. 获取账号
    user = _read_user()
    cookie = None

    # todo 3. 获取cookie
    try:
        cookie = login_sellersprite(user.get('username'), user.get('password'))
    except Exception as e:
        logger.error(f'登录 SellerSprite 失败: {e}')
        selection_master(conf, reItems)

    # todo 4. 获取响应数据
    items = fetch_amazon_selection_data(cookie=cookie, params=params)
    logger.info('获取选品数据完成, items 数量: {}'.format(len(items)))
    if not items:
        return []

    # todo 4.2 删除已经采集的数据
    for item in items:
        for reItem in reItems:
            if item.get('asin') in reItem:
                items.remove(item)

    if not items:
        return []

    return items

def selection_slave(conf:dict, items, pool=None):
    # todo 5. 获取 token
    user = _read_user()
    token = export_token(user.get('username'), user.get('password'))

    # todo 6. 取 asins
    asinList = []
    for item in items:
        asinList.append(item.get('asin'))

    # todo 7. 第一次更新 items
    newItems = updataItems(items, asinList, token, conf, t=False)
    logger.info('第一次更新 items 完成, newItems 数量: {}'.format(len(newItems)))

    # todo 8. 第二次更新 items
    asinList = [i.get('asin') for i in newItems if i]
    finalItems = updataItems(newItems, asinList, token, conf, t=True)
    logger.info('第二次更新 items 完成, finalItems 数量: {}'.format(len(finalItems)))

    if pool is None:
        return finalItems

    processed_data = crawl_item_info(finalItems, pool, conf.get('site'), t=True)

    # todo 10. 合并亚马逊数据
    newItems = merge_list_of_dicts(finalItems, processed_data)

    # todo 10.1 添加产品 站点
    new_webItems = []
    for newItem in newItems:
        newItem.update({'itemWEB': _get_site_url(conf.get('site'))})
        new_webItems.append(newItem)

    return new_webItems



def crawl_item_info(finalItems, pool , site, t):
    """
    爬取商品详细信息
    :param finalItems:
    :param pool: selenium pool
    :param site:
    :param t: 是否阿里搜索
    :return:
    """
    # todo 9.1 定义 cookies 变量
    # cookies = None
    # todo 9.2 初始化"常量" （异步 cookie 值）
    # SAFE_CONST = ThreadSafeConstant(cookies)
    # todo 9.3 定义数据 存储列表
    processed_data = []
    data_lock = threading.Lock()  # 保护结果列表

    # todo 9.4 定义一个异步执行方法
    def process_batch(a, i, s, p, rt):
        """
        异步执行方法
        :param a: asin
        :param i: image url
        :param s: site
        :param p: pool
        :param rt: t
        """
        web = _get_site_url(s)
        baseurl = f'{web}/dp/{a}?psc=1'
        try:
            # productJSON = get_amazon_product(baseurl, SAFE_CONST.cookies, s)
            # if 'cookies' in productJSON:
            #     SAFE_CONST.update(productJSON['cookies'])
            productJSON = {}
            rCont = 0
            # todo 重试机制
            productJSON = p.get_page_source(baseurl, body={'image':i})
            for _ in range(3):
                if processed_data:
                    break
                else:
                    productJSON = p.get_page_source(baseurl, body={'image': i})
            with data_lock:
                # todo 解析数据
                product_data = deconstruct_pageSource(productJSON.get("pageSource"), a)
                # todo 查找同款
                product_data['similarList'] = json.dumps(productJSON.get('similarList'))
                # todo 阿里搜索
                if rt:
                    aliexpress_crawler = AliExpressCrawler()
                    ai_items = aliexpress_crawler.search_by_image(i)
                    product_data['aliexpress'] = json.dumps(ai_items)
                # todo 存储数据
                processed_data.append(product_data)
            return {
                'asin': a,
                'm': 'success',
            }
        except Exception as e:
            logger.error(f"处理 {a} 失败: {e}")
            raise  # 重新抛出异常以便主线程捕获

    # todo 9.5 使用线程池控制并发数
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 提交所有任务
        futures = []
        for item in finalItems:
            if item.get('asin') is not None:
                imageUrl = item.get('image')
                if imageUrl is None:
                    imageUrl = item.get('imageUrl')
                if imageUrl is not None:
                    futures.append(executor.submit(process_batch, item.get('asin'), imageUrl, site, pool, t))
                    time.sleep(random.uniform(1,2))
        # 等待所有任务完成并处理异常
        for future in as_completed(futures):
            try:
                future.result()  # 获取结果（会抛出线程中的异常）
            except Exception as e:
                logger.error(f"任务执行出错: {e}")
    return processed_data


def deconstruct_pageSource(pageSource, asin):
    """
    解析 pageSource 获取商品数据
    :param pageSource:
    :param asin:
    :return:
    """
    logger.info(f"解析 ASIN: {asin} 的页面数据")
    # todo 数据返回值
    product_data = {}
    # todo 解析 pageSource
    soup = BeautifulSoup(pageSource, 'html.parser')
    # todo asin
    product_data['asin'] = asin
    # todo 标题
    product_data['title'] = processing_title(soup)
    # 主图
    image = processing_image(soup)
    product_data['image'] = image
    # todo 评分，评论
    product_data.update(processing_CustomerReviews(soup))
    # todo 价格处理
    product_data.update(processingPrices(soup))
    # todo 产品材质，描述
    product_data.update(processing_description(soup))

    return product_data



def updataItems(items, asinList, token, conf, t):
    asinList = asinList
    newItems = []
    token = token
    r = 0
    # todo 1. 循环筛选数据
    while True:
        asins = asins = ','.join([asin for asin in asinList[0:101] if asin is not None])
        if len(asinList) < 100:
            asins = ','.join([asin for asin in asinList[0:len(asinList)+1] if asin is not None])
        # todo 2. 获取数据
        dataJson = fetch_amazon_detailed_data(token=token, asins=asins, site=conf.get('site'), t=t)
        if dataJson.get('message'):
            break
        token = dataJson.get('token')
        new_datas = dataJson.get('data')
        for data in new_datas:
            for item in items:
                if data.get('asin') == item.get('asin'):
                    item.update({k: v for k, v in data.items() if v})
                    newItems.append(item)
                    # todo 3. 关键 删除 asinList 中已处理的 asin
                    asinList.remove(item.get('asin'))
                    break
        if not asinList:
            break

    # todo 4. 处理未完成的 asin
    # if asinList:
    #     for a in asinList:
    #         dataJson = fetch_amazon_detailed_data(token=token, asins=a, site=conf.get('site'), t=t)
    #         time.sleep(random.uniform(2,5))
    #         if dataJson.get('message'):
    #             continue
    #         token = dataJson.get('token')
    #         try:
    #             new_data = dataJson.get('data')[0]
    #             for item in items:
    #                 if new_data.get('asin') == item.get('asin'):
    #                     item.update(new_data)
    #                     newItems.append(item)
    #                     break
    #         except Exception as e:
    #             logger.error(f"获取 {a} 单个商品数据失败: {e}")
    #             continue

    return newItems
