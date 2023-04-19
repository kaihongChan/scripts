import hashlib
import json
import time
import traceback
import sys

sys.path.append("/app/scripts/")
from app.base import Base


class ItemTraffic(Base):
    def __init__(self):
        super().__init__()
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.itemTraffic/1"

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
                        _exist = self._db.query_row(_exist_sql, {
                            "shop_id": job_dict['shop_id'],
                            "item_id": job_dict['item_id'],
                            "date": job_dict['date'],
                            "source_id": v['sourceId'],
                        })
                        if _exist.empty:
                            self._db.insert("item_traffic_indicate", insert_data)
                        else:
                            self._db.update("item_traffic_indicate", insert_data, f"`id`={_exist['id']}")

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
                exist = self._db.query_row(
                    exist_sql, {
                        "shop_id": job_dict['shop_id'],
                        "item_id": data['item_id'],
                        "date": job_dict['date']
                    }
                )
                if exist.empty:
                    self._db.insert("item_traffic", data)
                else:
                    self._db.update("item_traffic", data, f"`id`={exist['id']}")

    def exec_handle(self):
        """ 监听任务列表 """
        watch_key = f'fordeal:{self._username}:item:traffic:jobs'
        while True:
            json_str = self._redis.brpop([watch_key], timeout=10)

            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                print(job_dict['item_id'])
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
                    resp = self._requests_get(job_dict, request_data)
                    self._parse_and_save(job_dict, resp)
                except Exception as e:
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                    traceback.print_exc()
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    ItemTraffic().exec_handle()
