# 店铺概览数据（需二次清洗）
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


class ShopTraffic:
    def __init__(self):
        self._url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.shopTraffic/1'
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/zh-CN/summary/index',
            'accept': 'application/json, text/plain, */*',
        }

    def _parse_and_save(self, job_dict, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = resp_json['data']
            if "meta_list" in data:
                data['meta_list'] = json.dumps(data['meta_list'], ensure_ascii=False)
            if "first_traffic_trend" in data:
                for ftt in data['first_traffic_trend']:
                    if ftt['date'] != job_dict['date']:
                        continue
                    query_sql = (
                        f"select `id` from `shop_first_traffic_trend` where `shop_id`=%(shop_id)s"
                        f" and `date`=%(date)s and `source_id`=%(source_id)s limit 1"
                    )
                    exist = db_query_one(query_sql, {
                        "shop_id": ftt['shop_id'],
                        "date": ftt['date'],
                        "source_id": ftt['source_id'],
                    })
                    if exist.empty:
                        db_insert("shop_first_traffic_trend", ftt)
                    else:
                        db_update("shop_first_traffic_trend", ftt, f"id={exist['id']}")
                data['first_traffic_trend'] = json.dumps(data['first_traffic_trend'], ensure_ascii=False)
            if "second_traffic_rank" in data:
                for sstr in data['second_traffic_rank']:
                    sstr['date'] = job_dict['date']
                    sstr['shop_id'] = job_dict['shopId']
                    if "ctm_order_view_rate" in sstr:
                        sstr['ctm_order_view_rate'] = float(sstr['ctm_order_view_rate'].replace('%', '')) / 100
                    query_sql = (
                        f"select `id` from `shop_second_traffic_rank` where `shop_id`=%(shop_id)s"
                        f" and `date`=%(date)s and `source_l2_id`=%(source_l2_id)s"
                    )
                    exist = db_query_one(query_sql, {
                        "shop_id": sstr['shop_id'],
                        "date": sstr['date'],
                        "source_l2_id": sstr['source_l2_id'],
                    })
                    if exist.empty:
                        db_insert("shop_second_traffic_rank", sstr)
                    else:
                        db_update("shop_second_traffic_rank", sstr, f"id={exist['id']}")
                data['second_traffic_rank'] = json.dumps(data['second_traffic_rank'], ensure_ascii=False)
            data['shop_id'] = job_dict['shopId']
            data['request_date'] = job_dict['date']

            query_sql = (
                f"select `id` from `shop_traffic` where `shop_id`=%(shop_id)s and `request_date`=%(request_date)s"
            )
            exist = db_query_one(query_sql, {
                "shop_id": job_dict['shopId'],
                "request_date": job_dict['date'],
            })
            if exist.empty:
                db_insert("shop_traffic", data)
            else:
                db_update("shop_traffic", data, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """ 发送请求 """
        watch_key = 'fordeal:shop:traffic:jobs'
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
                            "data": json.dumps(
                                {
                                    "startDate": str(last_date),
                                    "endDate": str(last_date),
                                    "dateType": 1,
                                    "selectType": 4
                                }, ensure_ascii=False
                            ),
                            "gw_ver": 1,
                            "ct": timestamp,
                            "plat": "h5",
                            "appname": "fordeal",
                            "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                        }
                        resp = request_get(job_dict, self._url, self._headers, search_params)
                        job_dict['date'] = str(last_date)
                        self._parse_and_save(job_dict, resp)
                        last_date += datetime.timedelta(days=1)

                    last_date -= datetime.timedelta(days=1)
                    get_redis_conn().hset("fordeal:shop:traffic:last_date", str(job_dict['shopId']), str(last_date))
                except Exception as e:
                    traceback.print_exc()
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
            else:
                print(f"{watch_key}结束监听。。。")
                break


if __name__ == '__main__':
    ShopTraffic().request_handle()
