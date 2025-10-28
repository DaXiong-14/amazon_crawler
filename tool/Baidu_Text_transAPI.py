# -*- coding: utf-8 -*-

# This code shows an example of text translation from English to Simplified-Chinese.
# This code runs on Python 2.7.x and Python 3.x.
# You may install `requests` to run this code: pip install requests
# Please refer to `https://api.fanyi.baidu.com/doc/21` for complete api document

import requests
import random
from hashlib import md5


class BaiduTranslation:
    """
        此类为 百度翻译 api 对接
        主要函数 to_text  传入翻译文本 返回目标文本
    """
    def __init__(self):
        # app ID
        self.appid = '20250923002462036'
        # app 秘钥
        self.appkey = 'AvaQn86vrLoqyhUqv08C'
        # 翻译 api 链接
        self.apiURL = 'https://fanyi-api.baidu.com/api/trans/vip/translate'
        # 翻译语言 auto 自动识别
        self.from_lang = 'auto'
        # 翻译 目标语言 中文
        self.to_lang = 'zh'

    # Generate salt and sign
    @staticmethod
    def make_md5(s, encoding='utf-8'):
        return md5(s.encode(encoding)).hexdigest()

    def post(self, data):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        try:
            r = requests.post(self.apiURL, params=data, headers=headers)
            result = r.json()
            return result
        except Exception as e:
            return {
                "from": "auto",
                "to": "zh",
                "error": str(e),
                "trans_result": []
            }


    def to_text(self, text):
        salt = random.randint(32768, 65536)
        sign = self.make_md5(self.appid + text + str(salt) + self.appkey)
        payload = {
            'appid': self.appid,
            'q': text,
            'from': self.from_lang,
            'to': self.to_lang,
            'salt': salt,
            'sign': sign
        }
        result = self.post(payload)
        return result