# 入库计划
import hashlib
import json
import math
import time
import traceback
import sys

sys.path.append("/app/scripts/")
from app.base import Base


class InboundPlan(Base):
    def __init__(self):
        super().__init__()
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.queryInboundPlan/1"

        self._page_size = 60
        self._total = 0

        query_sql = (
            f"select `id`, `username`, `password`, `shop_id` from `accounts` where `username`='{self._username}' limit 1"
        )
        self._account = self._db.query_row(query_sql).to_dict()

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
                    exist = self._db.query_row(exist_sql, {"shopId": item['shopId'], "planId": item['planId']})
                    if exist.empty:
                        # 插入
                        self._db.insert("inbound_plans", item)
                    else:
                        self._db.update("inbound_plans", item, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])
        return

    def request_handle(self):
        """ 监听任务列表 """
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
            resp = self._requests_get(self._account, request_data)
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
                    resp = self._requests_get(self._account, request_data)
                    self._parse_and_save(resp)
        except Exception as e:
            print(f"【{self._url}】数据采集异常，err：{e}")
            traceback.print_exc()


if __name__ == '__main__':
    InboundPlan().request_handle()
