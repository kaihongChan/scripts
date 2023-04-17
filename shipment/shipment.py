# 仓库数据采集

import hashlib
import json
import math
import time
import traceback
import sys


sys.path.append("/app/scripts/")
from db_helper import db_query_one, db_insert, db_update
from helper import request_get
from redis_helper import get_redis_conn


class Shipment:
    def __init__(self):
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listShipmentView/1"
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }

        self._page_size = 60
        self._total = 0

    def _parse_and_save(self, shop_id, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            self._total = resp_json['data']['total']
            rows = resp_json['data']['dataList']
            for row in rows:
                row['receiveTags'] = json.dumps(row['receiveTags'], ensure_ascii=False)
                if "disputeOrderList" in row:
                    row['disputeOrderList'] = json.dumps(row['disputeOrderList'], ensure_ascii=False)
                if "createdAt" in row and row['createdAt'] > 0:
                    row['createdAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['createdAt'] / 1000))
                if "shippedAt" in row and row['shippedAt'] > 0:
                    row['shippedAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['shippedAt'] / 1000))
                if "closedAt" in row and row['closedAt'] > 0:
                    row['closedAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['closedAt'] / 1000))

                row['shop_id'] = shop_id
                exist_sql = (
                    f"select `id`, `shipmentId` from `shipments` where `shipmentId`=%(shipmentId)s and `shop_id`=%(shop_id)s limit 1"
                )
                exist = db_query_one(exist_sql, {"shipmentId": row['shipmentId'], "shop_id": shop_id})
                if exist.empty:
                    # 插入
                    db_insert("shipments", row)
                else:
                    # 更新
                    db_update("shipments", row, f"`id`={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def exec_handle(self):
        """ 监听任务列表 """
        watch_key = 'fordeal:shipment:jobs'
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
                        "shipmentId": "",
                        "trackingNo": "",
                        "shipmentStatus": "",
                        "skuId": "",
                        "withDisputeOrder": 1
                    }
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
                    self._parse_and_save(job_dict['shopId'], resp)
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
                            self._parse_and_save(job_dict['shopId'], resp)
                except Exception as e:
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                    traceback.print_exc()
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    Shipment().exec_handle()