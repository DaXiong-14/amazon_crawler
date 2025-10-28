# todo 功能 用于获取 亚马逊 详情页面数据
import json
import logging
from datetime import datetime
import re

from tool.utils import get_amazon_product, _get_site_url
from src.amazon_similar_products import fetch_amazon_similar_products
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def get_product_details(result, cookies=None, site=None):
    """
    # 管关键方法 无法获取到数据 就一直回调
    # 超过 10次 放弃该页面 错误上报给服务器后端
    :param result: 列表排名信息
    :param cookies: 未过期的 selenium cookies
    :param site: 站点 US DE
    :return: json 数据
    """
    # todo 解析 result
    web = _get_site_url(site)
    asin = result['asin']
    baseurl = f'{web}/dp/{asin}?psc=1'
    retryCount = 0
    while True:
        try:
            response_json = get_amazon_product(baseurl, cookies, site)
            re_cookies = response_json['cookies']
            page_source = response_json.get("pageSource")
            # todo 数据返回值
            product_data = {}
            # todo 解析数据 (bs4)
            soup = BeautifulSoup(page_source, 'html.parser')
            # todo asin
            product_data['asin'] = asin
            # todo 标题
            product_data['title'] = processing_title(soup)
            # 主图
            image = processing_image(soup)
            product_data['image'] = image
            # todo 评分，评论
            product_data.update(processing_CustomerReviews(soup))
            # todo 附加信息 尺码 颜色 等
            product_data['additionalINFO'] = processing_additional(soup)
            # todo 价格处理
            product_data.update(processingPrices(soup))
            # todo 产品材质，描述
            product_data.update(processing_description(soup))
            # todo 站点
            product_data['site'] = site
            # todo 当前数据抓取时间 (时间戳 以秒为单位)
            current_timestamp = datetime.now().timestamp()
            product_data['timestamp'] = current_timestamp
            # todo 类目
            product_data['category'] = result['category']
            product_data['category_id'] = result['category_id']
            # todo 排名
            product_data['ranking'] = result['rank']
            # todo 畅销 or 新品
            product_data['bs'] = result['bs']
            # todo  亚马逊同款
            if image:
                same = fetch_amazon_similar_products(web, image, re_cookies)
                product_data['same'] = json.dumps(same, ensure_ascii=False)
            else:
                product_data['same'] = None
            logger.info('解析详情页面成功！')
            # todo 返回数据
            return {
                'cookies': re_cookies,
                'data': product_data,
            }
        except Exception as e:
            logger.error(f'解析详情页面失败: {e}\n 正在重试！')
            retryCount = retryCount + 1
            if retryCount > 10:
                break
            get_amazon_product(baseurl, cookies, site)
    # todo 在这里上报后端服务器

    return {}


def processing_title(soup):
    """
    标题处理
    :param soup:
    :return:
    """
    title = None
    try:
        titleBOX = soup.find('span', id='title')
        if not titleBOX:
            titleBOX = soup.find('span', id='productTitle')
        if not titleBOX:
            titleBOX = soup.find('h1', id='title')
        title = titleBOX.get_text(strip=True)
    except Exception as e:
        logger.error(f'处理标题失败: {e}')
    return title


def processing_image(soup):
    image = None
    try:
        imageBOX = soup.find('div', id='imgTagWrapperId')
        if imageBOX:
            image = imageBOX.find('img').attrs['src']
    except Exception as e:
        logger.error(f'图片处理失败: {e}')
    return image


def processing_CustomerReviews(soup):
    """
    评分，评论处理
    :param soup:
    :return:
    """
    try:
        item_box = soup.find('div', id='averageCustomerReviews')
        if item_box:
            rating = item_box.find('span', class_='a-color-base').get_text(strip=True)
            reviewCount = item_box.find('span', id='acrCustomerReviewText').get_text(strip=True)
            return {
                'rating': rating,
                'reviewCount': re.sub(r'\D', '', reviewCount),
            }
        item_box = soup.find('a', id='acrCustomerReviewLink')
        if item_box:
            span_boxs = item_box.find_all('span')
            if span_boxs:
                rating = span_boxs[0].get_text(strip=True)
                reviewCount = span_boxs[-1].get_text(strip=True)
                return {
                    'rating': rating,
                    'reviewCount': re.sub(r'\D', '', reviewCount),
                }

    except Exception as e:
        logger.error(f'处理评分评论失败: {e}')

    return {
        'rating': None,
        'reviewCount': None,
    }


def processing_additional(soup):
    """
    处理附加信息
    :param soup:
    :return:
    """
    additionalINFO = None
    try:
        additional_box = soup.find('div', id='twister-plus-inline-twister-card')
        if not additional_box:
            additional_box = soup.find('div', id='twister-plus-mobile-inline-twister-card')
        additionalSpans = additional_box.find_all('span')
        if additionalSpans:
            additionalINFO = '\n'.join([a.get_text(strip=True) for a in additionalSpans if not a.get_text(strip=True).isspace()])
            additionalINFO = re.sub(r'\n+', '\n', additionalINFO)

    except Exception as e:
        logger.error(f'附加信息解析失败<{e}>')

    return additionalINFO


def processingPrices(soup):
    """
    价格处理
    :param soup:
    :return:
    """
    try:
        prices = {}
        # todo 关键 这里容器选择 有多个
        prices_source = soup.find('div', id='corePriceDisplay_mobile_feature_div')
        if prices_source is None:
            prices_source = soup.find('div', id='corePriceDisplay_desktop_feature_div')

        # todo 查找当前价格信息
        price_symbol = prices_source.find('span', class_='a-price-symbol')
        price_whole = prices_source.find('span', class_='a-price-whole')
        price_fraction = prices_source.find('span', class_='a-price-fraction')
        if price_whole and price_fraction:
            current_price = f"{
            price_symbol.get_text(strip=True) if price_symbol else ''
            }{
            price_whole.get_text(strip=True)
            }.{
            price_fraction.get_text(strip=True)
            }"
            prices['current_price'] = current_price.replace('..','.')
        # 备用方案：从aria-hidden内容提取
        elif 'current_price' not in prices:
            price_span = prices_source.find('span', class_='aok-offscreen')
            if price_span:
                current_price = price_span.get_text(strip=True)
                prices['current_price'] = current_price
        else:
            prices['current_price'] = None

        # todo 查找优惠 百分比
        discount_elem = prices_source.find('span', class_='savingPriceOverride')
        if discount_elem:
            prices['discount_percentage'] = discount_elem.get_text(strip=True)
        else:
            prices['discount_percentage'] = None

        # todo 查找原价
        original_price_elem = prices_source.find('span', class_='a-text-price')
        if original_price_elem:
            original_price = original_price_elem.find('span', class_='a-offscreen')
            if original_price:
                prices['original_price'] = original_price.get_text(strip=True)
        else:
            prices['original_price'] = None

        # todo 以 json 形式返回
        return prices

    except Exception as e:
        logger.error(f'处理价格信息失败: {e}')

    return {
        'current_price': None,
        'discount_percentage': None,
        'original_price': None,
    }


def processing_description(soup):
    """
        描述材料处理
        :param soup:
        :return:
    """
    try:
        description = {}
        # todo 多个 取值规则
        descriptionBox = soup.find('div', id='productFactsDesktopExpander')
        if not descriptionBox:
            descriptionBox = soup.find('div', id='featurebullets_feature_div')
        if not descriptionBox:
            descriptionBox = soup.find('div', id='productFacts_T1_feature_div')
        if not descriptionBox:
            descriptionBox = soup.find('div', id='hoc-topHighlights-expander')
        # todo 描述
        line_spans = descriptionBox.select('ul > li > span')
        des = None
        if line_spans:
            des = '\n'.join([span.get_text(strip=True) for span in line_spans])
        description['description'] = des
        # todo 材质
        materials = descriptionBox.find_all('div', class_='product-facts-detail')
        if materials:
            material = ""
            for material_box in materials:
                left = material_box.find('div', class_='a-col-left')
                right = material_box.find('div', class_='a-col-right')
                material += left.get_text(strip=True) + ':' + right.get_text(strip=True) + '\n'
            description['material'] = material.strip()
        else:
            description['material'] = None
        # 取材质的第二种方法
        if description['material'] is None:
            materials = descriptionBox.find_all('div', attrs={'class': 'a-row', 'role': 'listitem'})
            if materials:
                material = ""
                for material_box in materials:
                    spans = material_box.find_all('span')
                    material += spans[0].get_text(strip=True) + ':' + spans[-1].get_text(strip=True) + '\n'
                description['material'] = material.strip()
            else:
                description['material'] = None

        return description

    except Exception as e:
        logger.error(f'处理产品描述内容失败: {e}')

    return {
        'description': None,
        'material': None,
    }
