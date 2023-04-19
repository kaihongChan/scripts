# 订单概览
import hashlib
import json
import time
import traceback

from requests import Response
import sys
sys.path.append("/app/scripts/")
from app.base import Base


class OrderItemCurrent(Base):
    def __init__(self):
        super().__init__()
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listSaleOrderItemCurrent/1"

    def _parse_and_save(self, order_sn, record_id, resp: Response):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            detail = resp_json['data']
            detail['orderSn'] = order_sn
            detail['recordId'] = record_id

            # 数据预处理
            if "skus" in detail and detail['skus']:
                detail['skus'] = json.dumps(detail['skus'], ensure_ascii=False)
            if "orderInfo" in detail and detail['orderInfo']:
                detail['orderInfo'] = json.dumps(detail['orderInfo'], ensure_ascii=False)
            if "settleInfo" in detail and detail['settleInfo']:
                detail['settleInfo'] = json.dumps(detail['settleInfo'], ensure_ascii=False)

            exist_sql = (
                f"select `id` from `order_item_current` where `orderSn`=%(orderSn)s and `recordId`=%(recordId)s limit 1"
            )
            exist = self._db.query_row(exist_sql, {"orderSn": order_sn, "recordId": record_id})
            if exist.empty:
                # 插入
                self._db.insert("order_item_current", detail)
            else:
                # 更新
                self._db.update("order_item_current", detail, f"`id`={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """ 监听任务列表 """
        watch_key = f'fordeal:{self._username}:order:item_current:jobs'
        while True:
            json_str = self._redis.brpop([watch_key], timeout=10)

            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                try:
                    # 构造请求参数
                    search_params = {
                        "orderSn": job_dict['orderSn'],
                        "recordId": job_dict['recordId']
                    }
                    timestamp = int(round(time.time() * 1000))
                    request_data = {
                        "data": json.dumps(search_params, ensure_ascii=False),
                        "ct": timestamp,
                        "plat": "h5",
                        "appname": "fordeal",
                        "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                    }
                    # 请求接口
                    resp = self._requests_get(job_dict, request_data)
                    # 数据解析及保存
                    self._parse_and_save(job_dict['orderSn'], job_dict['recordId'], resp)
                except Exception as e:
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                    traceback.print_exc()
                    continue
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    OrderItemCurrent().request_handle()
