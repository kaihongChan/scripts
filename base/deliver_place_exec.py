# 店铺信息（更新频率：周）
import datetime
import hashlib
import json
import random
import time
import traceback
import pandas as pd
import sys


sys.path.append("/app/scripts/")
from db_helper import db_query_one, db_update, db_insert
from helper import request_get
from redis_helper import get_redis_conn


class DeliverPlaceExec:
    def __init__(self):
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.getDeliverPlaceInfo/1"
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/zh-CN/summary/index',
            'accept': 'application/json, text/plain, */*',
        }

    def _parse_and_save(self, resp, shop_id):
        """ 数据入库 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = resp_json['data']
            for val in data:
                val['shopId'] = shop_id
                exist_sql = (
                    f"select * from `deliver_places` where `shopId`=%(shopId)s and `deliverPlaceCode`=%(deliverPlaceCode)s limit 1"
                )
                exist = db_query_one(exist_sql, {"shopId": shop_id, "deliverPlaceCode": val['deliverPlaceCode']})
                if exist.empty:
                    db_insert("deliver_places", val)
                else:
                    db_update("deliver_places", val, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """ 发送请求 """
        watch_key = 'fordeal:deliver_place:jobs'
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=5)
            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)

                try:
                    # 构造请求参数
                    timestamp = int(round(time.time() * 1000))
                    request_params = {
                        "data": "{}",
                        "gw_ver": 1,
                        "ct": timestamp,
                        "plat": "h5",
                        "appname": "fordeal",
                        "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                    }
                    # 请求接口
                    resp = request_get(job_dict, self._url, self._headers, request_params)
                    # 数据存储
                    self._parse_and_save(resp, job_dict['shopId'])
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    continue
            else:
                print(f"{watch_key}结束监听。。。")
                break


if __name__ == '__main__':
    DeliverPlaceExec().request_handle()
