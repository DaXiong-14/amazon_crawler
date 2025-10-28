# todo 功能：用于 亚马逊 同款搜索
import json
import time
import logging
import os
from selenium import webdriver
from urllib.parse import quote
from tool.utils import _get_browser_options, _handle_browser_popups, process_intercepted_data

logger = logging.getLogger(__name__)


def fetch_amazon_similar_products(origin, image_url, max_retries=3):
    """
    方法获取 亚马逊同款 信息
    关键后端接口 https://www.amazon.com/stylesnap/upload?stylesnapToken=
    :param origin: 站点 <https://www.amazon.com>
    :param image_url:  亚马逊主图 url
    :param max_retries: 最大重试次数
    :return:
    """

    base_url = f'{origin}/stylesnap?q={quote(image_url)}'
    driver = webdriver.Edge(options=_get_browser_options())
    try:
        # todo 钩子提前注入，收集 url+body
        with open(os.path.join(os.getcwd(), 'js\\selenium_hook.js'), 'r', encoding='utf-8') as f:
            js_hook = f.read()
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': js_hook})

        # todo 访问页面
        logger.info(f'🚀 访问页面: {base_url}')
        driver.get(base_url)

        # todo 处理可能的弹窗
        _handle_browser_popups(driver, origin)
        # todo 时间等待
        driver.implicitly_wait(20)
        # todo 轮询等待拦截数据
        retry_count = 0
        while retry_count < max_retries:
            max_wait_time = 20
            poll_interval = 0.5
            waited = 0
            while waited < max_wait_time:
                intercepted_arr = driver.execute_script('return window._interceptedStylesnapArr;')
                if intercepted_arr and isinstance(intercepted_arr, list):
                    logger.info(f'🎯 发现目标接口')
                    for intercepted in intercepted_arr:
                        try:
                            json.loads(intercepted['body'])
                            logger.info('✨ 拦截完成!')
                            return process_intercepted_data(intercepted['body'])
                        except Exception as e:
                            logger.error(f'❌ 响应数据不是Json: {e}')
                            continue
                if waited % 5 == 0:
                    logger.info(f'⏰ 已等待 {waited} 秒，尚未拦截到 JSON 数据...')
                time.sleep(poll_interval)
                waited += poll_interval
            retry_count += 1
            logger.info(f'⚠️ 超时：未拦截到 JSON 数据，刷新页面重试（第{retry_count}次）...')
            driver.refresh()
            # todo 清空拦截数组，防止旧数据影响
            driver.execute_script('window._interceptedStylesnapArr = [];')
        logger.info('⏹️ 多次刷新后仍未拦截到 JSON 数据，停止加载')
        return []

    except Exception as e:
        logger.error(f'💥 执行过程中出错: {e}')
        return []

    finally:
        driver.quit()
        logger.info('🔚 浏览器已关闭')



