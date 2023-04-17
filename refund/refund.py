# 退货待确认

import hashlib
import json
import math
import random
import time
import traceback
import sys


sys.path.append("/app/scripts/")
from db_helper import db_del, db_insert
from redis_helper import get_redis_conn
from helper import request_get


class Refund:
    def __init__(self):
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listExceptionSkuPool/1"

        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }

        self._page_size = 50
        self._total = 0

        self._warehouse_id = 3

    def _parse_and_save(self, job_dict, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            self._total = resp_json['data']['total']
            rows = resp_json['data']['rows']
            # 删除
            del_cond = f"`shopId`={job_dict['shopId']} and `confirmType`=0"
            db_del("refunds", del_cond)
            for row in rows:
                if "joinReturnDate" in row:
                    row['joinReturnDate'] = time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.localtime(row['joinReturnDate'] / 1000)
                    )
                row['origin_id'] = row['id']
                row['warehouseId'] = self._warehouse_id
                del row['id']
                # 插入
                refund_id = db_insert("refunds", row)

                exception_shoot_img_job = {
                    "username": job_dict['username'],
                    "password": job_dict['password'],
                    # "provider_id": job_dict['provider_id'],
                    # "proxy_ip": job_dict['proxy_ip'],
                    "refund_id": refund_id,
                    "ssuid": row['ssuId'],
                }
                if refund_id > 0 and row['ssuId']:
                    get_redis_conn().lpush("fordeal:refund:es_img:jobs", json.dumps(exception_shoot_img_job, ensure_ascii=False))
        else:
            raise Exception(resp_json['msg'])

    def exec_handle(self):
        """ 监听任务列表 """
        watch_key = 'fordeal:refund:jobs'
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=5)
            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                try:
                    search_params = {"confirmType": 0, "warehouseId": self._warehouse_id, "page": 1, "pageSize": self._page_size}

                    timestamp = int(round(time.time() * 1000))
                    request_data = {
                        "gw_ver": 1,
                        "data": json.dumps(search_params, ensure_ascii=False),
                        "ct": timestamp,
                        "plat": "h5",
                        "appname": "fordeal",
                        "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                    }
                    resp = request_get(job_dict, self._url, self._headers, request_data)
                    self._parse_and_save(job_dict, resp)
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
                            self._parse_and_save(job_dict, resp)
                except Exception as e:
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                    traceback.print_exc()
                    continue
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    Refund().exec_handle()
