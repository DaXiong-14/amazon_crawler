# todo 工具集成类
import json
import os
import random
import threading
import time
from typing import List, Dict, Any
import logging
import requests
from queue import Queue
from tool.keywords_amount_utils import export_tk, export_token
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import undetected_chromedriver as uc
from urllib.parse import quote


logger = logging.getLogger(__name__)


class SeleniumPool:
    def __init__(self, site, pool_size=5):
        """
        初始化Selenium实例池
        :param pool_size: 池大小，默认5个实例
        """
        logger.info('初始化 Selenium 浏览器实例池...')
        self.pool_size = pool_size
        self.site = site
        self.drivers = []  # 存储所有driver实例
        self.available = Queue()  # 可用driver队列
        self.locks = {}  # 每个driver的锁
        self._init_pool()


    def _init_pool(self):
        """初始化浏览器实例池"""
        for _ in range(self.pool_size):
            driver = self._create_driver()
            self.drivers.append(driver)
            self.available.put(driver)
            self.locks[driver] = threading.Lock()


    def _create_driver(self):
        """创建单个浏览器实例"""
        try:
            options = _get_browser_options()
            driver_path = os.path.join(os.getcwd(), 'drivers\\chromedriver.exe')
            service = Service(executable_path=driver_path)
            driver = webdriver.Chrome(
                options=options,
                service=service
            )
            # 隐藏特征
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                """
            })
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_script("window.chrome = {runtime: {}};")
            driver.get(_get_site_url(site=self.site))
            _handle_browser_popups(driver, _get_site_url(self.site))
            time.sleep(random.uniform(1,2))
            # 设置邮政编码
            _setup_postal_code(driver, site=self.site)
            driver.implicitly_wait(20)
            return driver
        except Exception as e:
            logger.error(f"创建浏览器实例失败: {str(e)}, 重试中...")
            return self.get_driver()

    def get_driver(self):
        """
        获取一个可用的浏览器实例
        返回: (driver, release_func) 元组
        """
        driver = self.available.get()
        lock = self.locks[driver]
        lock.acquire()

        def release():
            """释放driver回池中"""
            lock.release()
            self.available.put(driver)

        return driver, release

    def get_random_driver(self):
        """随机获取一个可用浏览器实例"""
        # 先尝试获取当前可用driver
        if not self.available.empty():
            return self.get_driver()

        # 如果没有立即可用的，随机选择一个
        driver = random.choice(self.drivers)
        lock = self.locks[driver]
        lock.acquire()

        def release():
            lock.release()
            self.available.put(driver)

        return driver, release

    def get_page_source(self, url, body=None, timeout=40):
        """
        获取页面源码并自动释放driver
        :param url: 要访问的URL
        :param body: 是否有图片信息
        :param timeout: 页面加载超时时间(秒)
        :return: 页面源码(HTML)
        """
        driver, release = self.get_random_driver()
        try:
            driver.set_page_load_timeout(timeout)
            # todo 启用网络拦截
            driver.execute_cdp_cmd('Network.enable', {})
            driver.execute_cdp_cmd('Network.setBlockedURLs', {
                'urls': [
                    '*.jpg', '*.png', '*.gif',  # 图片
                    '*.css',  # CSS
                    # '*.js'  # JavaScript
                ]
            })
            # 访问页面
            driver.get(url)
            driver.implicitly_wait(20)

            # # 处理反爬
            _handle_browser_popups(driver, _get_site_url(self.site))
            driver.implicitly_wait(20)

            # 获取页面数据
            cookies = driver.get_cookies()
            page_source = driver.page_source.encode('utf-8').strip()

            logger.info("浏览器驱动成功获取页面内容")

            # 恢复网络拦截 - 关键步骤
            driver.execute_cdp_cmd('Network.disable', {})
            if not body is None:
                similarList = self.get_similar_products(driver, body.get('image'), max_retries=3)
                return {
                    'cookies': cookies,
                    'pageSource': page_source,
                    'similarList': similarList
                }
            return {
                'cookies': cookies,
                'pageSource': page_source,
            }
        except Exception as e:
            logger.info(f"获取页面源码失败: {str(e)}")
            return {}
        finally:
            release()  # 确保无论如何都释放driver


    def get_similar_products(self, driver, imageUrl, max_retries):
        """
            亚马逊同款搜素
            :param driver: selenium 浏览器实例
            :param imageUrl: 图片链接
            :param max_retries: 最大重试次数
        """
        try:
            base_url = f'{_get_site_url(self.site)}/stylesnap?q={quote(imageUrl)}'
            # todo 钩子提前注入，收集 url+body
            with open(os.path.join(os.getcwd(), 'js\\selenium_hook.js'), 'r', encoding='utf-8') as f:
                js_hook = f.read()
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': js_hook})
            # todo 访问页面
            logger.info(f'🚀 访问页面: {base_url}')
            driver.get(base_url)
            # todo 处理可能的弹窗
            _handle_browser_popups(driver, _get_site_url(self.site))
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


    def close_all(self):
        """关闭所有浏览器实例"""
        for driver in self.drivers:
            try:
                driver.quit()
            except Exception as e:
                logger.error(f"关闭浏览器实例时出错: {e}")
        self.drivers.clear()
        self.available = Queue()


def _get_browser_ua():
    """获取随机浏览器User-Agent"""
    user_agents = [
        # Windows Chrome
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",

        # Windows Firefox
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:118.0) Gecko/20100101 Firefox/118.0",

        # Windows Edge
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.0.0",

        # macOS Safari
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",

        # macOS Chrome
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",

        # macOS Firefox
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.1; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:119.0) Gecko/20100101 Firefox/119.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:118.0) Gecko/20100101 Firefox/118.0",

        # Linux Chrome
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",

        # Linux Firefox
        "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:119.0) Gecko/20100101 Firefox/119.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:118.0) Gecko/20100101 Firefox/118.0"
    ]
    return random.choice(user_agents)

def _get_browser_options():
    """获取浏览器选项"""
    options = Options()

    # 性能优化选项
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    # 反检测选项
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # 随机User-Agent

    options.add_argument(f"user-agent={_get_browser_ua()}")

    options.add_argument(f"--window-size={random.randint(1200, 1920)},{random.randint(800, 1080)}")

    # todo 这个地方禁用 css 和图片加载会导致 阿里无法搜图
    # 禁用图片和CSS
    # prefs = {
    #     "profile.managed_default_content_settings.images": 2,
    #     "profile.managed_default_content_settings.stylesheets": 2
    # }
    # options.add_experimental_option("prefs", prefs)

    # 启用网络拦截
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    return options


def _get_uc_browser_options():
    """使用undetected-chromedriver创建驱动"""
    options = uc.ChromeOptions()

    # 反检测配置
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # 随机用户代理
    options.add_argument(f"--user-agent={_get_browser_ua()}")
    # 窗口大小随机化
    options.add_argument(f"--window-size={random.randint(1200, 1920)},{random.randint(800, 1080)}")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2
    }
    options.add_experimental_option("prefs", prefs)

    return options


def _handle_browser_popups(driver, origin):
    """处理亚马逊常见反爬"""
    driver.implicitly_wait(20)
    wait = WebDriverWait(driver, 3)
    if 'Request was throttled' in driver.page_source:
        driver.refresh()
        driver.implicitly_wait(20)
        logger.info("请求被限制，已重新刷新！")
    if '<h2>Tut uns Leid!' in driver.page_source:
        logger.warning("请求被限制，准备重新访问站点主页！")
        baseurl = driver.current_url
        driver.get(origin)
        driver.implicitly_wait(20)
        time.sleep(20)
        driver.get(baseurl)
        driver.implicitly_wait(20)
        logger.info("请求被限制，已重新访问站点主页！")
        # todo 回调
        _handle_browser_popups(driver, origin)
    try:
        button = driver.find_element(By.CLASS_NAME, "a-button-text")
        button.click()
        logger.info("成功处理机器人反爬!")
    except:
        pass
    time.sleep(random.uniform(1, 3))
    try:
        accept_button = wait.until(EC.element_to_be_clickable((By.ID, "sp-cc-accept")))
        accept_button.click()
        logger.info("成功点击Cookie同意按钮!")
    except:
        pass


def _setup_postal_code(driver, site="US"):
    """设置邮政编码"""
    logger.info("开始设置邮政编码!")
    driver.implicitly_wait(20)
    wait = WebDriverWait(driver, 20)
    try:
        # 点击地址选择框
        ingress_box = wait.until(EC.element_to_be_clickable((By.ID, "glow-ingress-block")))
        ingress_box.click()
        time.sleep(random.uniform(1,3))
        # 尝试设置邮政编码（最多20次）
        for attempt in range(5):
            try:
                input_box = wait.until(EC.element_to_be_clickable((By.ID, "GLUXZipUpdateInput")))
                input_box.clear()
                postal_code = _get_postal_code(site)
                input_box.send_keys(postal_code)
                time.sleep(random.uniform(1, 2))
                apply_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="GLUXZipUpdate"]/span/input'))
                )
                apply_button.click()
                driver.implicitly_wait(20)
                driver.refresh()
                logger.info(f"邮政编码设置成功: {postal_code}")
                break
            except Exception as e:
                logger.error(f"邮政编码设置尝试{attempt + 1}失败: {str(e)}")

    except Exception as e:
        logger.error(f"邮政编码设置失败: {str(e)}")


def _get_postal_code(site):
    """根据国家生成随机邮编"""
    us_zip_codes = [
        "10001", "90210", "60601", "33139", "94102",
        "75201", "98101", "19102", "20001", "90001"
    ]
    french_postal_codes = [
        "75001", "69001", "13001", "31000", "33000",
        "06000", "67000", "44000", "59000", "34000"
    ]
    german_postal_codes = [
        "10115", "80331", "60311", "50667", "70173",
        "20095", "40213", "04109", "01067", "45127"
    ]
    uk_postcodes = [
        "SW1A 1AA", "W1A 1AA", "EC1A 1BB", "N1 9GU", "M1 1AE",
        "B1 1HQ", "LS1 8DF", "G1 1DA", "EH1 1AA", "CF10 1BH"
    ]
    country_fakers = {
        "US": us_zip_codes,
        "DE": german_postal_codes,
        "UK": uk_postcodes,
        "FR": french_postal_codes
    }

    if site in country_fakers:
        postcode = random.choice(country_fakers[site])
        return postcode
    return "10001"  # 默认邮编


def _get_site_url(site):
    """获取站点URL"""
    sites = {
        "US": "https://www.amazon.com",
        "DE": "https://www.amazon.de",
        "UK": "https://www.amazon.co.uk",
        "FR": "https://www.amazon.fr"
    }
    return sites.get(site, "https://www.amazon.com")


def get_amazon_product(baseurl, cookies=None, site=None):
    """
        requests 获取 亚马逊 页面源码
        :site: 站点 DE US
        :baseurl: 页面链接
        :cookies: selenium 获取的 cookies
        :return: {
            'cookies': 未过期的cookie,
            'page_source': 页面源码
        }
    """

    try:
        # 创建会话和设置headers
        session = requests.Session()
        headers = {
            'User-Agent': _get_browser_ua(),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': f'{_get_site_url(site)}/'
        }

        # 设置cookies
        if cookies and isinstance(cookies, list):
            for cookie in cookies:
                session.cookies.set(cookie['name'], cookie['value'])

        # 随机延迟
        delay = random.uniform(1, 1.5)
        logger.info(f"设置随机延迟: {delay:.2f}秒")
        time.sleep(delay)

        # 发送请求
        response = session.get(baseurl, headers=headers, timeout=10)
        response.raise_for_status()
        session.close()

        if response.status_code != 200:
            return _selenium_amazon_product(baseurl, site=site)

        # 检查反爬虫
        if 'robot' in response.url or 'captcha' in response.text:
            logger.warning("触发反爬虫验证，切换至浏览器模式")
            return _selenium_amazon_product(baseurl, site=site)

        if 'Request was throttled' or '<h2>Tut uns Leid!' in response.text:
            logger.warning("请求被限制，重新尝试")
            return _selenium_amazon_product(baseurl, site=site)

        logger.info("成功获取商品页面内容")
        return {
            'cookies': cookies,
            'pageSource': response.text,
        }

    except Exception as e:
        logger.error(f"请求失败: {str(e)}")
        return None


def _selenium_amazon_product(baseurl, site=None):
    """
        selenium 获取 亚马逊 页面源码
        :site: 站点 DE US
        :baseurl: 页面链接
        :return: {
            'cookies': 更新后的cookie,
            'page_source': 页面源码
        }
    """
    logger.info(f"初始化浏览器驱动，访问URL: {baseurl}")
    try:
        options = _get_browser_options()
        service = Service(executable_path='../drivers/chromedriver.exe')
        driver = webdriver.Chrome(
            options=options,
            service=service
        )
        # 隐藏特征
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("window.chrome = {runtime: {}};")

        # 访问页面
        driver.get(baseurl)
        driver.implicitly_wait(20)

        # 处理反爬
        _handle_browser_popups(driver, _get_site_url(site))
        driver.implicitly_wait(20)

        # 设置邮政编码
        # _setup_postal_code(driver, site=site)

        # 获取页面数据
        cookies = driver.get_cookies()
        page_source = driver.page_source.encode('utf-8').strip()

        driver.close()

        logger.info("浏览器驱动成功获取页面内容")
        return {
            'cookies': cookies,
            'pageSource': page_source,
        }
    except Exception as e:
        logger.error(f"浏览器驱动初始化失败: {e}")
        return None


def _uc_amazon_product(baseurl, site=None):
    """
        undetected-chromedriver 获取 亚马逊 页面源码
        :site: 站点 DE US
        :baseurl: 页面链接
    """
    logger.info('初始化undetected-chromedriver浏览器驱动...')
    try:
        options = _get_uc_browser_options()
        driver = uc.Chrome(
            headless=False,
            driver_executable_path='../drivers/chromedriver.exe',
            options=options,
            version_main=141,
            use_subprocess=True,
        )

        # 移除WebDriver特征
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                window.chrome = {runtime: {}};
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
                """
        })

        # 修改插件信息
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [{
                        description: 'Portable Document Format',
                        filename: 'internal-pdf-viewer',
                        length: 1,
                        name: 'PDF Viewer'
                    }]
                });
                """
        })

        # 修改语言设置
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
                """
        })

        # 访问页面
        driver.get(baseurl)
        driver.implicitly_wait(20)

        # 处理反爬
        _handle_browser_popups(driver, _get_site_url(site))
        driver.implicitly_wait(20)

        # 获取页面数据
        cookies = driver.get_cookies()
        page_source = driver.page_source.encode('utf-8').strip()

        driver.close()

        logger.info("浏览器驱动成功获取页面内容")
        return {
            'cookies': cookies,
            'pageSource': page_source,
        }

    except Exception as e:
        print(e)
        logger.error(f"浏览器undetected-chromedriver驱动初始化失败: {e}")
        return None


def _fetch_category_data(site: str) -> List[Dict[str, Any]]:
    """
    读取配置文件匹配类目列表
    :param site: 站点名称
    :return:
    """
    datalist = []

    # 读取配置文件
    with open(os.path.join(os.getcwd(), 'config\\requirement-category.txt'), 'r', encoding='utf-8') as f:
        category_ids = [line.strip() for line in f if line.strip()]

    # 读取JSON数据
    def read_json_data(file_path: str) -> List[Dict[str, Any]]:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [json.loads(line.strip()) for line in f if line.strip()]

    nrs = read_json_data(os.path.join(os.getcwd(), f'temp\\category_config-{site}-NR.json'))
    bsrs = read_json_data(os.path.join(os.getcwd(), f'temp\\category_config-{site}-BSR.json'))

    # 合并两种排名类型数据
    rank_data = {'NR': nrs, 'BSR': bsrs}

    def process_category(category_id: str, rank_type: str) -> None:
        """递归处理分类数据"""
        for item in rank_data[rank_type]:
            if category_id == item['parent']['id']:
                parent_data = item['parent'].copy()
                parent_data['bs'] = rank_type
                datalist.append(parent_data)

                # 处理子项
                if item.get('items'):
                    for child_item in item['items']:
                        process_category(child_item['id'], rank_type)

    # 处理所有分类ID
    for rank_type in ['NR', 'BSR']:
        for category_id in category_ids:
            process_category(category_id, rank_type)

    return datalist


def fetch_amazon_selection_data(cookie: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    读取亚马逊选品JSON数据
    :param cookie: heders cookie
    :param params: 请求 body
    :return: 数据列表
    """
    session: requests.Session = requests.session()
    baseurl = 'https://www.sellersprite.com/v3/api/product-research'
    headers = _sellersprite_headers(cookie=cookie)
    # todo 重试 3 次
    for _ in range(3):
        try:
            response = session.post(url=baseurl, headers=headers, json=params, timeout=20)
            response.raise_for_status()
            response_json = response.json()
            data = response_json.get('data')
            if not data:
                continue
            if not data.get('items'):
                continue
            return data.get('items')
        except Exception as e:
            logger.error(f"获取JSON数据失败: {e}")
    return []


def _sellersprite_headers(token=None, cookie=None):
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0",
        "accept": "application/json",
        "content-type": "application/json",
        "sec-fetch-site": "none",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "zh-CN,zh;q=0.9",
        "priority": "u=1, i",
    }
    if token is not None:
        headers.update({
            'auth-token': token
        })
    if cookie is not None:
        headers.update({
            'cookie': cookie
        })
    return headers


def _read_user():
    """
    随机 获取用户
    :return:
    """

    with open(os.path.join(os.getcwd(), 'config\\users.txt'), 'r', encoding='utf-8') as f:
        lines = f.readlines()
    user = random.choice(lines).strip()
    return {
        'username': user.split(',')[0],
        'password': user.split(',')[1]
    }


def fetch_amazon_detailed_data(token: str, asins: str, site: str, t=False) -> Dict[str, Any]:
    """
    详细 itme 数据获取
    :param token: 卖家精灵身份令牌
    :param asins: 产生 asins 逗号分割
    :param site: 站点
    :param t: 是否为内容数据 默认否
    :return:
    """
    tk = export_tk(asins)
    session: requests.Session = requests.Session()

    def re_data(u, k, p):
        response = session.get(url=u, headers=_sellersprite_headers(token=k), params=p, timeout=20)
        time.sleep(random.uniform(2, 5))
        response.raise_for_status()
        data = response.json()
        message = ['令牌过期，请退出再重新登录。', '抱歉，目前您使用过于频繁，请验证后再使用。', '令牌过期，请续签令牌。']
        # todo 处理令牌过期情况
        if data.get('message') in message:
            logger.warning('令牌失效，尝试刷新令牌')
            user = _read_user()
            k = export_token(user.get('username'), user.get('password'))
            if token != '4480':
                try:
                    new_response = session.get(url=u, headers=_sellersprite_headers(token=k), timeout=20,
                                               params=p)
                    time.sleep(random.uniform(2, 5))
                    new_response.raise_for_status()
                    data = new_response.json
                except Exception as e:
                    logger.error(f"刷新令牌请求失败: {e}")
                    raise Exception(f'令牌刷新失败，请检查账号状态！{user}')
        reData = data.get('data')
        if not reData:
            raise Exception("无效数据！")
        if not reData.get('items'):
            raise Exception("无效数据！")
        return reData.get('items')

    try:
        logger.info(f"开始获取ASIN: {asins} 数据")
        params = {
            'asins': asins,
            'source': 'edge',
            'miniMode': False,
            'withRelation': True,
            'withSaleTrend': False,
            'tk': tk,
            'version': '4.8.1',
            'language': 'ZH_CN',
            'extension': 'ecanjpklimgeijdcdpdfoooofephbbln'
        }
        baseurl = f'https://www.sellersprite.com/v2/extension/competitor-lookup/{site}'
        if t:
            baseurl = f'https://www.sellersprite.com/v2/extension/competitor-lookup/quick-view/{site}'

        for i in range(3):
            try:
                items = re_data(u=baseurl, k=token, p=params)
                logger.info(f"请求asin: {asins} 成功！")
                return {
                    'token': token,
                    'data': items
                }
            except Exception as e:
                logger.error(f'请求数据失败: {e} 正在重试 asins: {asins}')
                if i == 2:
                    raise Exception('多次请求失败!')
        return {
            'token': token,
            'data': [],
        }

    except Exception as e:
        logger.error(f"请求asin: {asins} 时出错: {e}")
        return {
            'token': token,
            'data': [],
            'message': str(e)
        }




def merge_json(left: dict, right: dict) -> dict:
    """
    字典合并数据 左连接 左边有的不更新，没有的更新 最后合并
    :param left: Json
    :param right: Json
    :return:
    """
    left.update({k: v for k, v in right.items() if k not in left})
    return left


def merge_list_of_dicts(left: List[Dict[str, Any]], right: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    列表字典合并数据
    :param left: 列表 Json
    :param right: 列表 Json
    :return:
    """
    new_list = []
    for item_left in left:
        for item_right in right:
            if item_left.get('asin') == item_right.get('asin'):
                item_left.update({k: v for k, v in item_right.items() if v is not None})
        new_list.append(item_left)
    return new_list

def process_intercepted_data(data):
    """
       处理拦截到的数据
    """
    sameList = []
    try:
        requests_json = json.loads(data)
        bbxAsinMetadataList = requests_json['searchResults'][0]['bbxAsinMetadataList']
        for item in bbxAsinMetadataList:
            sameList.append({
                'glProductGroup': item['glProductGroup'],
                'byLine': item['byLine'],
                'price': item['price'],
                'listPrice': item['listPrice'],
                'imageUrl': item['imageUrl'],
                'asin': item['asin'],
                'title': item['title'],
                'averageOverallRating': item['averageOverallRating'],
                'totalReviewCount': item['totalReviewCount'],
            })
    except Exception as e:
        logger.error(f'❌ JSON解构出错: {e}')
    return sameList

class ThreadSafeConstant:
    """线程安全的Cookie管理器"""

    def __init__(self, cookies=None):
        self._cookies = cookies if cookies else []
        self._lock = threading.Lock()

    @property
    def cookies(self):
        with self._lock:
            return self._cookies.copy()

    def update(self, new_cookies):
        if not new_cookies:  # 检查空值
            return
        with self._lock:
            self._cookies = new_cookies

