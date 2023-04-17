# 店铺信息（更新频率：周）
import hashlib
import json
import time
import traceback
import sys

sys.path.append("/app/scripts/")
from db_helper import db_insert, db_update, db_query_one
from redis_helper import get_redis_conn
from helper import request_get


class ShopInfo:
    def __init__(self):
        self._url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.myInfo/1'
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/zh-CN/summary/index',
            'accept': 'application/json, text/plain, */*',
        }

    def _parse_and_save(self, resp, account_id):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = resp_json['data']['user']
            del data['privilege'], data['mtoken'], data['showLanguageSwitch']
            exist_sql = (
                f"select `id`, `shopId` from `shops` where `shopId`=%(shopId)s limit 1"
            )
            exist = db_query_one(exist_sql, {"shopId": data['shopId']})
            if exist.empty:
                # 插入
                db_insert("shops", data)
            else:
                db_update("shops", data, f"`id`={exist['id']}")

            # 更新accounts表
            account_update = {
                "shop_id": data['shopId']
            }
            db_update("accounts", account_update, f"`id`={account_id}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """ 发送请求 """
        watch_key = 'fordeal:shop:info:jobs'
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=5)
            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                try:
                    timestamp = int(round(time.time() * 1000))
                    params = {
                        "data": "",
                        "gw_ver": 1,
                        "ct": timestamp,
                        "plat": "h5",
                        "appname": "fordeal",
                        "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                    }
                    resp = request_get(job_dict, self._url, self._headers, params)
                    self._parse_and_save(resp, job_dict['id'])
                except Exception as e:
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                    traceback.print_exc()
                    continue
            else:
                print(f"{watch_key}结束监听。。。")
                break


if __name__ == '__main__':
    ShopInfo().request_handle()
