import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

import logging
from tool.utils import _get_browser_options

logger = logging.getLogger(__name__)

def login_sellersprite(username, password):
    """
        此方法 使用 selenium 技术模拟 卖家精灵平台登录操作 获取 cookie
        :param username: 卖家精灵账号
        :param password: 卖家精灵密码
        :return: cookie
    """
    logger.info(f'{username} 开始登录卖家精灵...')
    options = _get_browser_options()
    driver_path = os.path.join(os.getcwd(), 'drivers\\chromedriver.exe')
    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(
        options=options,
        service=service
    )
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.get("https://www.sellersprite.com/cn/w/user/login")
    try:
        # 等待页面加载
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.form_signin'))
        )
        # 切换到账号登录表单（如果需要）
        # 页面默认显示账号登录，但确认一下
        account_tab = driver.find_element(By.CSS_SELECTOR, 'a[href="#pills-account"]')
        if "active" not in account_tab.get_attribute("class"):
            account_tab.click()
            time.sleep(1)
        # 定位用户名输入框
        username_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#form_signin_passW input[name="email"]'))
        )
        # 定位密码输入框
        password_input = driver.find_element(By.CSS_SELECTOR, '#form_signin_passW input[type="password"]')
        # 输入用户名和密码
        username_input.clear()
        username_input.send_keys(username)
        time.sleep(1)
        password_input.clear()
        password_input.send_keys(password)
        # 点击登录按钮
        login_button = driver.find_element(By.CSS_SELECTOR, '#form_signin_passW .login-btn')
        login_button.click()
        # 等待登录成功
        WebDriverWait(driver, 10).until(
            EC.url_changes("https://www.sellersprite.com/cn/w/user/login")
        )
        logger.info("登录成功!")
        time.sleep(1)
        # 获取 cookie
        cookies = driver.get_cookies()
        # 转换 headers 能用的cookie
        cookie_str = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
        return cookie_str

    except Exception as e:
        logger.error(f"登录过程中出现错误: {str(e)}")
        return None

    finally:
        # 关闭浏览器
        driver.quit()
