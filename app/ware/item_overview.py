import hashlib
import json
import time
import traceback
import sys


sys.path.append("/app/scripts/")
from app.base import Base


class ItemOverview(Base):
    def __init__(self):
        super().__init__()
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.itemOverview/1"
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
                if "sku_indicator_list" in data:
                    for sku in data['sku_indicator_list']:
                        sku_exist_sql = (
                            "select `id` from `sku_indicator` where `shop_id`=%(shop_id)s "
                            "and `item_id`=%(item_id)s and `date`=%(date)s and `sku_id`=%(sku_id)s limit 1"
                        )
                        sku_exist = self._db.query_row(sku_exist_sql, {
                            "shop_id": sku['shop_id'],
                            "item_id": sku['item_id'],
                            "date": job_dict['date'],
                            "sku_id": sku['sku_id'],
                        })
                        sku['date'] = job_dict['date']
                        if sku_exist.empty:
                            self._db.insert("sku_indicator", sku)
                        else:
                            self._db.update("sku_indicator", sku, f"`id`={sku_exist['id']}")
                # 保留原始数据
                for k, v in data.items():
                    if type(v) == dict or type(v) == list:
                        data[k] = json.dumps(v, ensure_ascii=False)
                data['shop_id'] = job_dict['shop_id']
                data['date'] = job_dict['date']
                exist_sql = (
                    "select `id` from `item_overview` where `shop_id`=%(shop_id)s "
                    "and `item_id`=%(item_id)s and `date`=%(date)s"
                )
                exist = self._db.query_row(exist_sql, {
                    "shop_id": job_dict['shop_id'],
                    "item_id": data['item_id'],
                    "date": job_dict['date']
                })
                if exist.empty:
                    self._db.insert("item_overview", data)
                else:
                    self._db.update("item_overview", data, f"`id`={exist['id']}")

    def exec_handle(self):
        """ 监听任务列表 """
        watch_key = f'fordeal:{self._username}:item:overview:jobs'
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
                    print(f"【{self._url}】数据采集异常，err：{e}")
                    traceback.print_exc()
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    ItemOverview().exec_handle()
