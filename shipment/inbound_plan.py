# 入库计划
import hashlib
import json
import math
import random
import time
import traceback
import sys


sys.path.append("/app/scripts/")
from db_helper import db_insert, db_update, db_query_one
from helper import request_get
from redis_helper import get_redis_conn


class InboundPlan:
    def __init__(self):
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.queryInboundPlan/1"

        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }

        self._page_size = 60
        self._total = 0

    def _parse_and_save(self, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = resp_json['data']
            self._total = data['total']
            if "dataList" in data:
                for item in data['dataList']:
                    if "shipFromAddress" in item:
                        item['shipFromAddress'] = json.dumps(item['shipFromAddress'], ensure_ascii=False)
                    if "createdAt" in item and item['createdAt'] > 0:
                        item['createdAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item['createdAt'] / 1000))
                    if "updatedAt" in item and item['updatedAt'] > 0:
                        item['updatedAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item['updatedAt'] / 1000))
                    exist_sql = (
                        f"select `id` from `inbound_plans` where `shopId`=%(shopId)s and `planId`=%(planId)s"
                    )
                    exist = db_query_one(exist_sql, {"shopId": item['shopId'], "planId": item['planId']})
                    if exist.empty:
                        # 插入
                        db_insert("inbound_plans", item)
                    else:
                        db_update("inbound_plans", item, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])
        return

    def request_handle(self):
        """ 监听任务列表 """
        watch_key = 'fordeal:inbound_plan:jobs'
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=5)

            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                try:
                    search_params = {
                        "page": 1,
                        "pageSize": self._page_size,
                        "fulfillmentCenterId": "",
                        "planId": "",
                        "planStatus": "",
                        "skuId": ""
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
                    self._parse_and_save(resp)
                    # 处理分页
                    if self._total > self._page_size:
                        page_num = math.ceil(self._total / self._page_size)
                        for page_index in range(1, page_num):
                            p_time_stamp = int(round(time.time() * 1000))
                            search_params['page'] = page_index + 1
                            request_data['data'] = json.dumps(search_params, ensure_ascii=False)
                            request_data['ct'] = p_time_stamp
                            request_data['sign'] = hashlib.md5(str(p_time_stamp).encode()).hexdigest()
                            resp = request_get(job_dict, self._url, self._headers, request_data)
                            self._parse_and_save(resp)
                except Exception as e:
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                    traceback.print_exc()
                    continue
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    InboundPlan().request_handle()
