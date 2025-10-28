import json
import logging
import os
import random
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from tool.utils import _get_browser_options

class AliExpressCrawler:
    """阿里巴巴1688 搜索产品"""
    def __init__(self):
        """初始化浏览器驱动"""
        self.logger = logging.getLogger(__name__)
        options = _get_browser_options()
        driver_path = os.path.join(os.getcwd(), 'drivers\\chromedriver.exe')
        service = Service(executable_path=driver_path)

        self.driver = webdriver.Chrome(
            options=options,
            service=service,
        )


    def search_by_image(self, image_url, max_retries = 3):
        """
        通过图片链接在1688搜索相似产品，全部用selenium元素操作提取数据
        :param max_retries:
        :param image_url: 产品图片URL
        :return: 相似产品列表
        """
        self.logger.info(f"开始在1688搜索图片: {image_url}")
        ai_items = []
        try:
            self.driver.get(
                "https://aibuy.1688.com/landingpage?bizType=selectionTool&customerId=sellerspriteLP&lang=zh&currency=CNY"
            )

            # todo 处理可能的弹窗
            try:
                button = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="driver-popover-content"]/footer/span[2]/button[2]'))
                )
                button.click()
                self.logger.info('成功处理弹窗！')
            except Exception as e:
                self.logger.warning(f'没有找到弹窗 {str(e)} ，继续执行...')
                self.driver.refresh()
                self.driver.implicitly_wait(20)

            # todo 钩子提前注入，收集 url+body
            with open(os.path.join(os.getcwd(), 'js\\selenium_hook.js'), 'r', encoding='utf-8') as f:
                js_hook = f.read().replace('upload?stylesnapToken', 'mtop.mbox.fc.common.gateway/1.0/')
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': js_hook})

            # todo 点击搜图按钮
            image_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//span[contains(text(),"图片链接搜索")]'))
            )
            image_button.click()
            time.sleep(random.uniform(0,1))

            # todo 输入图片链接
            textarea = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="rc-tabs-0-panel-imageUrl"]/div/span/textarea'))
            )
            textarea.clear()
            textarea.send_keys(image_url)
            time.sleep(random.uniform(0, 1))

            # todo 点击搜索按钮
            searchButton = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//div[@class="ant-modal-footer"]/span[contains(text(),"确定")]'))
            )
            searchButton.click()
            self.driver.implicitly_wait(20)
            time.sleep(random.uniform(0,1))
            self.driver.refresh()

            # todo api 接口数据
            api_data = None

            # todo 轮询等待拦截数据
            retry_count = 0

            while retry_count < max_retries:
                max_wait_time = 20
                poll_interval = 0.5
                waited = 0
                while waited < max_wait_time:
                    intercepted_arr = self.driver.execute_script('return window._interceptedStylesnapArr;')
                    if intercepted_arr and isinstance(intercepted_arr, list):
                        self.logger.info(f'🎯 发现目标接口')
                        for intercepted in intercepted_arr:
                            try:
                                api_data = json.loads(intercepted['body'])
                                self.logger.info('✨ 拦截完成!')
                                reData = api_data['data']['result']['data']
                                self.close()
                                return reData
                            except Exception as e:
                                self.logger.error(f'❌ 响应数据不是Json: {e}')
                                continue
                    if waited % 5 == 0:
                        self.logger.info(f'⏰ 已等待 {waited} 秒，尚未拦截到 JSON 数据...')
                    time.sleep(poll_interval)
                    waited += poll_interval
                retry_count += 1
                self.logger.info(f'⚠️ 超时：未拦截到 JSON 数据，刷新页面重试（第{retry_count}次）...')
                self.driver.refresh()
                # todo 清空拦截数组，防止旧数据影响
                self.driver.execute_script('window._interceptedStylesnapArr = [];')

            self.logger.info('⏹️ 多次刷新后仍未拦截到 JSON 数据，停止加载')
            return []

        except Exception as e:
            self.logger.error(f"1688图片搜索失败: {e}")
            self.close()
            return []


    def close(self):
        """关闭浏览器驱动"""
        try:
            if self.driver:
                self.driver.quit()
                self.logger.info("1688浏览器驱动已关闭")
        except Exception as e:
            self.logger.error(f"关闭1688浏览器驱动时出错: {e}")

