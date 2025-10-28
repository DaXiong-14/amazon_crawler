import requests
import time
import random
from fake_useragent import UserAgent
from faker import Faker
import json
from lxml import etree
import os
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)


class AmazonCategoryFetcher:
    """
    亚马逊类目信息爬虫类
    主要功能：获取类目名和类目链接，生成配置文件
    优化点：简化日志输出，使用更简洁的打印方式
    """

    def __init__(self, site="US", bs="BSR"):
        """初始化爬虫实例"""
        self.site = site
        self._bs = bs
        logger.info(f"初始化亚马逊{bs}类目爬虫，站点: {site}")

        # 设置站点URL
        self.WEB = self._get_site_url(site)

        # 初始化存储
        os.makedirs('../temp', exist_ok=True)
        self.cookies = None
        self.products = []

    @staticmethod
    def _get_site_url(site):
        """获取站点URL"""
        sites = {
            "US": "https://www.amazon.com",
            "DE": "https://www.amazon.de",
            "UK": "https://www.amazon.co.uk",
            "FR": "https://www.amazon.fr"
        }
        return sites.get(site, "https://www.amazon.com")

    def get_amazon_product(self, asin_or_url):
        """
        获取亚马逊商品页面内容
        """
        logger.info(f"开始获取商品页面: {asin_or_url}")
        try:
            # 创建会话和设置headers
            session = requests.Session()
            headers = self._get_request_headers()

            # 设置cookies
            for cookie in self.cookies:
                session.cookies.set(cookie['name'], cookie['value'])

            # 随机延迟
            delay = random.uniform(1, 5)
            time.sleep(delay)

            # 发送请求
            response = session.get(asin_or_url, headers=headers, timeout=10)
            response.raise_for_status()
            session.close()
            # 页面加载失败
            if response.status_code != 200:
                return self._init_driver(asin_or_url)

            # 检查反爬虫
            if 'robot' in response.url or 'captcha' in response.text:
                logger.warning("触发反爬虫验证，切换至浏览器模式")
                return self._init_driver(asin_or_url)

            if 'Request was throttled' in response.text:
                logger.warning("请求被限制，重新尝试")
                return self.get_amazon_product(asin_or_url)

            logger.info("成功获取商品页面内容")

            return response.text

        except Exception as e:
            logger.error(f"请求失败: {str(e)}")
            return None

    def _get_request_headers(self):
        """生成请求头"""
        ua = UserAgent()
        return {
            'User-Agent': ua.random,
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': f'{self.WEB}/'
        }

    def _init_driver(self, base_url):
        """
        初始化浏览器驱动
        """
        logger.info(f"初始化浏览器驱动，访问URL: {base_url}")

        try:
            options = self._get_browser_options()
            driver = webdriver.Edge(options=options)

            # 访问页面
            driver.get(base_url)
            driver.implicitly_wait(20)
            wait = WebDriverWait(driver, 10)

            # 处理弹窗
            self._handle_browser_popups(driver, wait)

            # 获取页面数据
            self.cookies = driver.get_cookies()
            html_page = driver.page_source.encode('utf-8').strip()
            driver.close()

            logger.info("浏览器驱动成功获取页面内容")

            return html_page

        except Exception as e:
            logger.error(f"浏览器驱动初始化失败: {str(e)}")
            return None

    @staticmethod
    def _get_browser_options():
        """获取浏览器选项"""
        options = Options()

        # 性能优化选项
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")

        # 反检测选项
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # 随机User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
        ]
        options.add_argument(f"user-agent={random.choice(user_agents)}")

        # 禁用图片和CSS
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2
        }
        options.add_experimental_option("prefs", prefs)

        return options

    @staticmethod
    def _handle_browser_popups(driver, wait):
        """处理浏览器弹窗"""
        try:
            button = driver.find_element(By.CLASS_NAME, "a-button-text")
            button.click()
        except:
            pass

        # 尝试点击Cookie同意按钮
        try:
            accept_button = wait.until(EC.element_to_be_clickable((By.ID, "sp-cc-accept")))
            accept_button.click()
        except:
            pass

    def _get_postal_code(self):
        """根据国家生成随机邮编"""
        country_fakers = {
            "US": 'en_US',
            "DE": 'de_DE',
            "UK": 'en_GB',
            "FR": 'fr_FR'
        }

        if self.site in country_fakers:
            fake = Faker(country_fakers[self.site])
            return fake.postcode()
        return "10001"  # 默认邮编

    def make_bestsellers(self, link):
        """
        获取畅销品类目
        """
        logger.info(f"开始获取类目: {link}")
        page_info = None
        try:
            # 获取页面内容
            page_info = None
            if self.cookies is not None:
                page_info = self.get_amazon_product(link)
            else:
                page_info = self._init_driver(link)
            # 解析类目
            datas = self._parse_category_html(page_info)
            logger.info(f"成功解析{len(datas)}个类目")
            return datas

        except Exception as e:
            logger.error(f"获取类目失败: {str(e)}")
            logger.info("尝试重新获取...")
            return self.make_bestsellers(link)

    def _parse_category_html(self, html_content):
        """解析类目HTML"""
        try:
            html = etree.HTML(html_content)
            masterUL_elements = html.xpath('//div[@role="group"]/ul//ul')
            datas = []
            if masterUL_elements:
                masterUL_element = masterUL_elements[-1]
                items = masterUL_element.xpath('./li')
                for item in items:
                    a_box = item.xpath('./a')
                    if a_box:
                        category = a_box[0].text.strip()
                        baseurl = a_box[0].attrib['href']
                        if self.WEB not in baseurl:
                            baseurl = self.WEB + baseurl
                        id = a_box[0].attrib['href'].split('/')[-2]
                        if baseurl[-1] == '/':
                            id = a_box[0].attrib['href'].split('/')[-1]
                        datas.append({
                            'id': id,
                            'category': category,
                            'baseurl': baseurl,
                        })
                    else:
                        return []
                return datas
            else:
                raise Exception("解析类目HTML失败")
        except Exception as e:
            logger.error(f"解析类目HTML失败: {str(e)}")
            raise Exception("解析类目HTML失败")

    @staticmethod
    def make_splice(parent, items):
        """
        拼接数据文本
        添加进数据池
        :param parent:
        :param items:
        :return:
        """
        return json.dumps({
            'parent': parent,
            'size': len(items),
            'items': items
        })

    def make_task(self, item):
        """执行单个任务"""
        baseurl = item['baseurl']
        # todo 获取类目数据
        items = self.make_bestsellers(baseurl)
        logger.info(self.make_splice(item, items))
        self.products.append(self.make_splice(item, items))
        if items:
            for s in items:
                self.make_task(s)

    def Main(self):
        """主执行函数"""
        logger.info("启动主程序")

        try:
            make_link = f"{self.WEB}/-/en/gp/bestsellers"
            if self._bs == "NR":
                make_link = f"{self.WEB}/-/en/gp/new-releases"
            # todo 获取初始类目数据
            items = self.make_bestsellers(make_link)
            # todo 拼接数据
            self.products.append(self.make_splice({
                'id': '',
                'category': '',
                'baseurl': make_link,
            }, items))
            if items:
                # todo 使用线程池处理任务
                with ThreadPoolExecutor(max_workers=10) as executor:
                    for item in items:
                        executor.submit(self.make_task, item)
                    logger.info("任务执行完成!")
            # todo 下沉数据
            with open(f'temp/category_config-{self.site}-{self._bs}.json', 'w', encoding='utf-8') as f:
                f.write('\n'.join(self.products))

        except Exception as e:
            logger.error(f"主程序执行失败: {str(e)}")
        finally:
            logger.info("程序执行结束")


if __name__ == '__main__':
    # 运行爬虫
    # AmazonCategoryFetcher(bs='NR').Main()
    AmazonCategoryFetcher(bs='BSR').Main()
