# 仓库数据采集

import hashlib
import json
import math
import time
import traceback
import sys

sys.path.append("/app/scripts/")
from app.base import Base


class Shipment(Base):
    def __init__(self):
        super().__init__()
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listShipmentView/1"

        self._page_size = 60
        self._total = 0

        query_sql = (
            f"select `id`, `username`, `password`, `shop_id` from `accounts` where `username`='{self._username}' limit 1"
        )
        self._account = self._db.query_row(query_sql).to_dict()

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
                exist = self._db.query_row(exist_sql, {"shipmentId": row['shipmentId'], "shop_id": shop_id})
                if exist.empty:
                    # 插入
                    self._db.insert("shipments", row)
                else:
                    # 更新
                    self._db.update("shipments", row, f"`id`={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def exec_handle(self):
        """ 监听任务列表 """
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
            resp = self._requests_get(self._account, request_data)
            self._parse_and_save(self._account['shop_id'], resp)
            # 处理分页
            if self._total > self._page_size:
                page_num = math.ceil(self._total / self._page_size)
                for page_index in range(1, page_num):
                    p_time_stamp = int(round(time.time() * 1000))
                    search_params['page'] = page_index + 1
                    request_data['data'] = json.dumps(search_params, ensure_ascii=False)
                    request_data['ct'] = p_time_stamp
                    request_data['sign'] = hashlib.md5(str(p_time_stamp).encode()).hexdigest()
                    resp = self._requests_get(self._account, request_data)
                    self._parse_and_save(self._account['shop_id'], resp)
        except Exception as e:
            print(f"【{self._url}】数据采集异常，err：{e}")
            traceback.print_exc()


if __name__ == '__main__':
    Shipment().exec_handle()
