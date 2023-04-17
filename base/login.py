import hashlib
import json
import os.path
import random
import time
import requests
from http import cookiejar
from requests.utils import dict_from_cookiejar
import sys

sys.path.append("/app/scripts/")
from conf import COOKIE_PATH
from redis_helper import get_redis_conn


class Login:
    def __init__(self):
        self._login_url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.login/2"
        self._headers = {
            'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            "Referer": "https://business.fordeal.com/",
            "Host": "cn-ali-gw.fordeal.com"
        }

    def _check_cookie(self, username):
        # 接口校验cookie
        ct = int(round(time.time()) * 1000)
        params = {
            "data": "{}",
            "gw_ver": 1,
            "ct": ct,
            "plat": "h5",
            "appname": "fordeal",
            "sign: ": hashlib.md5(str(ct).encode()).hexdigest(),
        }
        url = f"https://cn-ali-gw.fordeal.com/merchant/dwp.galio.myInfo/1"

        cookie_file_name = f"{COOKIE_PATH}/cookie_{username}.txt"
        session = requests.session()
        cookie = cookiejar.LWPCookieJar()
        cookie.load(cookie_file_name, ignore_discard=True, ignore_expires=True)
        cookie = dict_from_cookiejar(cookie)
        session.cookies = requests.utils.cookiejar_from_dict(cookie)
        session.headers = self._headers
        resp = session.get(url, params=params)

        resp_json = resp.json()
        if not (resp.status_code == 200 and resp_json['code'] == 1001):
            return False

        return True

    def login(self, username, password, proxies):
        time.sleep(random.uniform(0, 0.5))
        if get_redis_conn().get(f"fordeal:{username}_login_lock") is not None:
            time.sleep(5)
        # 检测cookie是否有效，若无效重新请求登录，若有效则复用
        if not os.path.exists(f"{COOKIE_PATH}/cookie_{username}.txt") or not self._check_cookie(username):
            print("重新请求获取cookie")
            get_redis_conn().set(f"fordeal:{username}_login_lock", 1)
            get_redis_conn().expire(f"fordeal:{username}_login_lock", 5)
            account_json = json.dumps({"loginName": username, "password": password}, ensure_ascii=False)
            login_data = {
                "data": account_json
            }
            session = requests.session()
            session.cookies = cookiejar.LWPCookieJar(filename=f"{COOKIE_PATH}/cookie_{username}.txt")
            session.headers = self._headers
            resp = session.post(url=self._login_url, data=login_data, proxies=proxies)
            print(f"登录返回：{resp.text}")

            resp_json = resp.json()
            if not (resp.status_code == 200 and resp_json['code'] == 1001):
                raise Exception(resp_json['msg'])
            session.cookies.save(ignore_discard=True, ignore_expires=True)
            # get_redis_conn().delete(f"fordeal:{username}_login_lock")
