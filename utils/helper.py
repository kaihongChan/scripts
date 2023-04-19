import datetime
import json

import requests
import sys

from base.login import Login

sys.path.append("/app/scripts/")
from conf import COOKIE_PATH
from db_helper import db_query
from requests.utils import dict_from_cookiejar
from http import cookiejar


def get_yesterday():
    today = datetime.date.today()
    one_day = datetime.timedelta(days=1)
    yesterday = today - one_day
    return yesterday


def request_get(account, url, headers, params):
    try:
        # proxies = gen_proxies(account['provider_id'], account['proxy_ip'])
        proxies = None
        Login().login(account['username'], account['password'], proxies=proxies)
        cookie_file_name = f"{COOKIE_PATH}/cookie_{account['username']}.txt"
        session = requests.session()
        cookie = cookiejar.LWPCookieJar()
        cookie.load(cookie_file_name, ignore_discard=True, ignore_expires=True)
        cookie = dict_from_cookiejar(cookie)
        session.cookies = requests.utils.cookiejar_from_dict(cookie)
        session.headers = headers
        resp = session.get(url, params=params, proxies=proxies)
        return resp
    except Exception as e:
        raise Exception(e)


def gen_proxies(provider_id, proxy_ip):
    """ 组装代理IP """
    try:
        # 1、查询提供商
        query_sql = (
            "select `id`, `name`, `conf` from `proxy_providers` where `id`=%(id)s"
        )
        res = db_query(
            query_sql,
            {
                "provider_id": provider_id
            }
        )
        if res.empty:
            raise Exception("未设置代理IP")

        # 2、组装
        res = json.loads(res['conf'])
        proxy_url = "http://%(user)s:%(password)s@%(server)s" % {
            "user": res['authKey'],
            "password": res['authPassword'],
            "server": proxy_ip,
        }
        proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
        return proxies
    except:
        return {}
