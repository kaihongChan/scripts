import os
import sys
from http import cookiejar

import pandas as pd
import requests
sys.path.append("/app/scripts/")
from requests.utils import dict_from_cookiejar
from sqlalchemy import create_engine, text

from conf import db_cfg, COOKIE_PATH

if __name__ == '__main__':

    url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.logout/1"

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
        'referer': 'https://seller.fordeal.com/zh-CN/summary/index',
        'accept': 'application/json, text/plain, */*',
    }

    conn = (
        f"mysql+pymysql://{db_cfg['user']}:{db_cfg['passwd']}"
        f"@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['db']}?charset={db_cfg['charset']}"
    )
    db_engine = create_engine(conn)

    query_sql = (
        "select * from accounts where `status`=1"
    )
    accounts = pd.read_sql(text(query_sql), con=db_engine.connect())

    for _, account in accounts.iterrows():
        try:
            cookie_file_name = f"{COOKIE_PATH}/cookie_{account['username']}.txt"
            session = requests.session()
            cookie = cookiejar.LWPCookieJar()
            cookie.load(cookie_file_name, ignore_discard=True, ignore_expires=True)
            cookie = dict_from_cookiejar(cookie)
            session.cookies = requests.utils.cookiejar_from_dict(cookie)
            session.headers = header
            resp = session.get(url, data={"appname": "fordeal"})
            resp_json = resp.json()
            if resp.status_code == 200 and resp_json['code'] == 1001:
                os.remove(cookie_file_name)
            else:
                raise Exception(f"请求响应：{resp.text}")
        except Exception as e:
            print(f"【{account['username']}】注销登录失败，err：{e}")
