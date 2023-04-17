import hashlib
import random
import time
import traceback
import sys
sys.path.append("/app/scripts/")
from requests import Response
from sqlalchemy import text
from db_helper import db_insert, db_update, db_query_one, db_query
from helper import request_get


class TrafficMeta:
    def __init__(self):
        self._url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.trafficMeta/1'
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/zh-CN/summary/index',
            'accept': 'application/json, text/plain, */*',
        }

    def _parse_and_save(self, resp: Response):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = resp_json['data']
            for val in data:
                query_sql = (
                    f"select `id` from `traffic_meta` where `sourceId`=%(sourceId)s"
                )
                exist = db_query_one(query_sql=query_sql, args={"sourceId": val['sourceId']})
                if exist.empty:
                    db_insert("traffic_meta", val)
                else:
                    db_update("traffic_meta", val, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """ 发送请求 """
        try:
            # 随机选取一个账户采集数据
            query_sql = (
                "select `username`, `password`, `provider_id`, `proxy_ip`"
                " from accounts where `status`=1"
            )
            accounts = db_query(text(query_sql))
            accounts_len = len(accounts)
            loc_num = random.randint(0, accounts_len - 1)
            choose_account = accounts.iloc[loc_num]

            timestamp = int(round(time.time() * 1000))
            search_params = {
                "data": "{}",
                "gw_ver": 1,
                "ct": timestamp,
                "plat": "h5",
                "appname": "fordeal",
                "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
            }
            resp = request_get(choose_account, self._url, self._headers, search_params)
            self._parse_and_save(resp)
        except Exception as e:
            traceback.print_exc()
            print(f"数据采集异常，err：{e}")


if __name__ == '__main__':
    TrafficMeta().request_handle()