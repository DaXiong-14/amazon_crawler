# todo 功能 用于获取 亚马逊 类目 排名数据
import json
import re

from tool.utils import get_amazon_product


def crawl_search_results(baseurl, cookies=None, site=None):
    """
    亚马逊 类目列表爬虫
    :param baseurl: 亚马逊列表 url
    :param cookies:
    :param site: 爬虫站点
    :return:
    """
    retryCount = 0
    while True:
        try:
            response_json = get_amazon_product(baseurl, cookies, site)
            results = extract_product_info(response_json['pageSource'])
            print(results)
            return {
                'cookies': response_json['cookies'],
                'data': results,
            }
        except Exception as e:
            print(f'解析类目列表页面失败: {e}\n 正在重试！')
            retryCount = retryCount + 1
            if retryCount > 10:
                break
            crawl_search_results(baseurl, site)
    # todo 在这里上报后端服务器


    return {}

def extract_product_info(html_text):
    """
    从HTML文本中提取产品信息
    """
    # 方法1: 尝试从data-client-recs-list属性中提取JSON数据
    json_pattern = r'data-client-recs-list="([^"]+)"'
    json_match = re.search(json_pattern, html_text)

    products = []

    if json_match:
        try:
            # 清理JSON字符串并解析
            json_str = json_match.group(1).replace('&quot;', '"')
            product_list = json.loads(json_str)
            for product in product_list:
                product_info = {
                    'asin': product.get('id', ''),
                    'rank': product.get('metadataMap', {}).get('render.zg.rank', ''),
                }
                products.append(product_info)

        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {e}")

    # 方法2: 如果没有找到JSON数据，尝试从其他模式中提取
    if not products:
        # 尝试提取产品ID模式 (通常是B开头的10位代码)
        product_id_pattern = r'"id":"(B[A-Z0-9]{9})"'
        product_ids = re.findall(product_id_pattern, html_text)

        # 尝试提取排名信息
        rank_pattern = r'"render\.zg\.rank":"(\d+)"'
        ranks = re.findall(rank_pattern, html_text)

        for i, product_id in enumerate(product_ids):
            product_info = {
                'asin': product_id,
                'rank': ranks[i] if i < len(ranks) else '',
            }
            products.append(product_info)

    return products
