import datetime
import hashlib
import json
import time
import traceback
import sys


sys.path.append("/app/scripts/")
from db_helper import db_insert, db_update, db_query_one
from helper import request_get
from redis_helper import get_redis_conn


class ShopFlowSource:
    def __init__(self):
        self._url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.shopFlowSource/1'
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
                if "meta_list" in data:
                    data['meta_list'] = json.dumps(data['meta_list'])
                if "source" in data:
                    for item in data['source']:
                        format_item = {
                            "shop_id": shop_id,
                            "date": date,
                            "source_id": item['sourceId'],
                            "parent_source_id": item['parentSourceId'],
                            "ctm_item_click_uv": item['indicate']['ctm_item_click_uv']['content'],
                            "ctm_order_view_rate": item['indicate']['ctm_order_view_rate']['content'].replace('%', ''),
                            "item_cart_uv": item['indicate']['item_cart_uv']['content'],
                            "order_uv": item['indicate']['order_uv']['content'],
                        }
                        # 数据清洗
                        exist_sql = (
                            "select id from `shop_flow_source_format` where `shop_id`=%(shop_id)s"
                            " and `date`=%(date)s and `source_id`=%(source_id)s limit 1"
                        )
                        exist = db_query_one(exist_sql, {
                            "shop_id": shop_id,
                            "date": date,
                            "source_id": item['sourceId'],
                        })
                        if exist.empty:
                            # 插入
                            db_insert("shop_flow_source_format", format_item)
                        else:
                            db_update("shop_flow_source_format", format_item, f"`id`={exist['id']}")

                    data['source'] = json.dumps(data['source'])
                # 保留原始数据
                data['shop_id'] = shop_id
                data['date'] = date
                origin_exist_sql = (
                    "select id from `shop_flow_source` where `shop_id`=%(shop_id)s and `date`=%(date)s limit 1"
                )
                origin_exist = db_query_one(origin_exist_sql, {
                    "shop_id": shop_id,
                    "date": date,
                })
                if origin_exist.empty:
                    # 插入
                    db_insert("shop_flow_source", data)
                else:
                    db_update("shop_flow_source", data, f"`id`={origin_exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """ 发送请求 """
        watch_key = 'fordeal:shop:flow_source:job'
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=5)
            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)

                try:
                    today = datetime.date.today()
                    last_date = datetime.datetime.strptime(job_dict['last_date'], "%Y-%m-%d").date()
                    while last_date < today:
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
                        # break

                    last_date -= datetime.timedelta(days=1)
                    get_redis_conn().hset("fordeal:shop:flow_source:last_date", str(job_dict['shopId']), str(last_date))
                except Exception as e:
                    traceback.print_exc()
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                    continue
            else:
                print(f"{watch_key}结束监听。。。")
                break


if __name__ == '__main__':
    ShopFlowSource().request_handle()
