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


class ShopTrans:
    def __init__(self):
        self._url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.shopTrans/1'
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/zh-CN/summary/index',
            'accept': 'application/json, text/plain, */*',
        }

    def _parse_and_save(self, shop_id, date, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            if "data" in resp_json:
                data = resp_json['data']
                resp_dict = {}
                for key, val in data.items():
                    if type(val) == dict or type(val) == list:
                        resp_dict[key] = json.dumps(val, ensure_ascii=False)

                resp_dict['date'] = date
                resp_dict['shop_id'] = shop_id
                exist_sql = (
                    f"select `id` from `shop_trans` where shop_id=%(shop_id)s and `date`=%(date)s limit 1"
                )
                exist = db_query_one(exist_sql, {"shop_id": shop_id, "date": date})
                if exist.empty:
                    # 插入
                    db_insert("shop_trans", resp_dict)
                else:
                    db_update("shop_trans", resp_dict, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """ 发送请求 """
        watch_key = 'fordeal:shop:trans:jobs'
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=5)
            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)

                try:
                    today = datetime.date.today()
                    last_date = datetime.datetime.strptime(job_dict['last_date'], "%Y-%m-%d").date()
                    while last_date < today:
                        print(last_date)
                        timestamp = int(round(time.time() * 1000))
                        search_params = {
                            "data": json.dumps({
                                "startDate": str(last_date),
                                "endDate": str(last_date),
                                "dateType": 1,
                                "selectType": 1
                            }, ensure_ascii=False),
                            "gw_ver": 1,
                            "ct": timestamp,
                            "plat": "h5",
                            "appname": "fordeal",
                            "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                        }
                        resp = request_get(job_dict, self._url, self._headers, search_params)
                        self._parse_and_save(job_dict['shopId'], str(last_date), resp)
                        last_date += datetime.timedelta(days=1)

                    last_date -= datetime.timedelta(days=1)
                    get_redis_conn().hset("fordeal:shop:trans:last_date", str(job_dict['shopId']), str(last_date))
                except Exception as e:
                    traceback.print_exc()
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
            else:
                print(f"{watch_key}结束监听。。。")
                break


if __name__ == '__main__':
    ShopTrans().request_handle()
