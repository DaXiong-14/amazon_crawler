import logging
import os

import execjs
import json
import time
import hashlib
import requests

"""
    此类目用于 卖家精灵 登录 token 的获取
    export_tk 函数 
        传入参数 shop_id -> user
                password
        返回 js 加密的 tk 值
    md5_encrypt 函数
        传入 password
        返回 password 哈希值
    export_token 函数
        传参 user
            password
        返回 token
"""

logger = logging.getLogger(__name__)

def export_tk(shop_id, password=''):
    with open(os.path.join(os.getcwd(), 'js\\export_tk.js'), 'r', encoding='utf-8') as f:
        js_code = f.read()
    ctx = execjs.compile(js_code).call('sellersprite_token', shop_id, password)
    return ctx


def md5_encrypt(data):
    md5 = hashlib.md5()
    md5.update(data.encode('utf-8'))
    return md5.hexdigest()


def export_token(user, password):
    md5_password = md5_encrypt(password)
    tk = export_tk(user, md5_password)
    url = f'https://www.sellersprite.com/v2/extension/signin?email={user}&password={md5_password}&tk={tk}&version=4.8.1&language=zh_CN&extension=ecanjpklimgeijdcdpdfoooofephbbln&source=edge'
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0",
        "accept": "application/json",
        "content-type": "application/json",
        "sec-fetch-site": "none",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "zh-CN,zh;q=0.9",
        "priority": "u=1, i"
    }
    # 这里直接请求
    logger.info(f'正在使用账号 {user}')
    response = requests.get(url=url, headers=headers, verify=False)
    try:
        json_data = json.loads(response.text)
        token = json_data['data']['token']
        return token
    except Exception as e:
        logger.warning(f'token 获取失败 {str(e)}，正在重试')
        retry_time = 0
        while retry_time < 5:
            time.sleep(30)
            retry_time += 1
            response = requests.get(url=url, headers=headers, verify=False)
            try:
                json_data = json.loads(response.text)
                token = json_data['data']['token']
                return token
            except Exception as e:
                logger.warning(f'token 获取失败 {str(e)}，正在重试')
                continue
        return '4480'
