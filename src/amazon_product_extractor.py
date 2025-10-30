# todo 功能 用于解析 亚马逊 详情页面数据
import logging
import re

logger = logging.getLogger(__name__)


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
