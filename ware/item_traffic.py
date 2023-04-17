import hashlib
import json
import random
import time
import traceback
import sys

sys.path.append("/app/scripts/")
from db_helper import db_query_one, db_insert, db_update
from helper import request_get
from redis_helper import get_redis_conn


class ItemTraffic:
    def __init__(self):
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.itemTraffic/1"

        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }

    def _parse_and_save(self, job_dict, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            if "data" in resp_json:
                data = resp_json['data']
                # traffic_list处理
                if "traffic_list" in data:
                    for v in data['traffic_list']:
                        insert_data = {
                            "shop_id": job_dict['shop_id'],
                            "item_id": job_dict['item_id'],
                            "date": job_dict['date'],
                            "source_id": v['sourceId'],
                            "parent_source_id": v['parentSourceId'],
                        }
                        for k, indicate in v['indicate'].items():
                            if k == "ctm_order_view_rate":
                                indicate['content'] = indicate['content'].replace('%', '')
                            insert_data[k] = indicate['content']

                        _exist_sql = (
                            "select `id` from `item_traffic_indicate` where `shop_id`=%(shop_id)s "
                            "and `item_id`=%(item_id)s and `date`=%(date)s and `source_id`=%(source_id)s limit 1"
                        )
                        _exist = db_query_one(_exist_sql, {
                            "shop_id": job_dict['shop_id'],
                            "item_id": job_dict['item_id'],
                            "date": job_dict['date'],
                            "source_id": v['sourceId'],
                        })
                        if _exist.empty:
                            db_insert("item_traffic_indicate", insert_data)
                        else:
                            db_update("item_traffic_indicate", insert_data, f"`id`={_exist['id']}")

                # =============================================
                for k, v in data.items():
                    if type(v) == dict or type(v) == list:
                        data[k] = json.dumps(v, ensure_ascii=False)

                data['shop_id'] = job_dict['shop_id']
                data['item_id'] = job_dict['item_id']
                data['date'] = job_dict['date']
                exist_sql = (
                    "select `id` from `item_traffic` where `shop_id`=%(shop_id)s "
                    "and `item_id`=%(item_id)s and `date`=%(date)s limit 1"
                )
                exist = db_query_one(
                    exist_sql, {
                        "shop_id": job_dict['shop_id'],
                        "item_id": data['item_id'],
                        "date": job_dict['date']
                    }
                )
                if exist.empty:
                    db_insert("item_traffic", data)
                else:
                    db_update("item_traffic", data, f"`id`={exist['id']}")

    def exec_handle(self):
        """ 监听任务列表 """
        watch_key = 'fordeal:item:traffic:jobs'
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=10)

            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                try:
                    # 开始采集
                    search_params = {
                        "startDate": job_dict['date'],
                        "endDate": job_dict['date'],
                        "dateType": 1,
                        "selectType": 1,
                        "itemId": job_dict['item_id']
                    }
                    timestamp = int(round(time.time() * 1000))
                    request_data = {
                        "data": json.dumps(search_params, ensure_ascii=False),
                        "ct": timestamp,
                        "plat": "h5",
                        "appname": "fordeal",
                        "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                    }
                    resp = request_get(job_dict, self._url, self._headers, request_data)
                    self._parse_and_save(job_dict, resp)
                except Exception as e:
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                    traceback.print_exc()
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    ItemTraffic().exec_handle()
