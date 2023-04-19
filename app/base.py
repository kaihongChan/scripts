import datetime
import hashlib
import json
import os
import random
import sys
import time
from http import cookiejar

import requests
from requests.utils import dict_from_cookiejar

sys.path.append("/app/scripts/")
from conf import COOKIE_PATH
from utils.db_helper import get_db
from utils.redis_helper import get_redis


class Base:
    def __init__(self):
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }
        self._login_url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.login/2"
        self._url = ""

        self._db = get_db()
        self._redis = get_redis()

        self._username = None

    def __del__(self):
        self._db.close()
        self._redis.close()

    def _requests_get(self, account, params):
        """
        get请求
        :param account:
        :param params:
        :return:
        """
        self._username = self._redis.get("fordeal:spider:account")
        if self._username is None:
            print("账户错误")
            return

        if self._url != "":
            try:
                self._login(account['username'], account['password'])
                session = requests.session()
                cookie = cookiejar.LWPCookieJar()
                cookie_file_name = f"{COOKIE_PATH}/cookie_{account['username']}.txt"
                cookie.load(cookie_file_name, ignore_discard=True, ignore_expires=True)
                cookie = dict_from_cookiejar(cookie)
                session.cookies = requests.utils.cookiejar_from_dict(cookie)
                session.headers = self._headers
                resp = session.get(self._url, params=params)
                return resp
            except Exception as e:
                raise Exception(e)
        else:
            raise Exception("请求url错误！")

    def _check_cookie(self, username):
        """
        cookie有效性校验
        :param username: 登录名
        :return:
        """
        # 1、校验cookie是否存在
        cookie_file_name = f"{COOKIE_PATH}/cookie_{username}.txt"
        if not os.path.exists(cookie_file_name):
            return False

        # 2、校验cookie有效性
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
        session = requests.session()
        cookie = cookiejar.LWPCookieJar()
        cookie.load(cookie_file_name, ignore_discard=True, ignore_expires=True)
        cookie = dict_from_cookiejar(cookie)
        session.cookies = requests.utils.cookiejar_from_dict(cookie)
        session.headers = self._headers
        resp = session.get(url, params=params)
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = resp_json['data']['user']
            del data['privilege'], data['mtoken'], data['showLanguageSwitch']
            exist_sql = (
                f"select `id`, `shopId` from `shops` where `shopId`=%(shopId)s limit 1"
            )
            exist = self._db.query_row(exist_sql, {"shopId": data['shopId']})
            if exist.empty:
                # 插入
                self._db.insert("shops", data)
            else:
                self._db.update("shops", data, f"`id`={exist['id']}")

            # 更新accounts表
            account_update = {
                "shop_id": data['shopId']
            }
            self._db.update("accounts", account_update, f"`username`={self._username}")
        else:
            return False

        return True

    def _login(self, username, password, proxies=None):
        """
        模拟登录
        :param username:
        :param password:
        :param proxies:
        :return:
        """
        # 检测cookie是否有效，若无效重新请求登录，若有效则复用
        if not self._check_cookie(username):
            time.sleep(random.uniform(0.5, 5.0))
            login_lock = self._redis.get("login_lock")
            if login_lock is not None:
                return

            self._redis.set("login_lock", 1)
            self._redis.expire("login_lock", 30)
            print(f"{datetime.datetime}：重新请求获取cookie")
            account_json = json.dumps({"loginName": username, "password": password}, ensure_ascii=False)
            login_data = {
                "data": account_json
            }
            session = requests.session()
            session.cookies = cookiejar.LWPCookieJar(filename=f"{COOKIE_PATH}/cookie_{username}.txt")
            session.headers = self._headers
            resp = session.post(url=self._login_url, data=login_data, proxies=proxies)
            print(f"{datetime.datetime}：登录返回：{resp.text}")

            resp_json = resp.json()
            if not (resp.status_code == 200 and resp_json['code'] == 1001):
                raise Exception(resp_json['msg'])
            session.cookies.save(ignore_discard=True, ignore_expires=True)
