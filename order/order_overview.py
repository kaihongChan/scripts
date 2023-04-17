# 订单概览
import hashlib
import json
import time
import traceback
import sys


sys.path.append("/app/scripts/")
from db_helper import db_query_one, db_insert, db_update
from redis_helper import get_redis_conn
from helper import request_get


class OrderOverView:
    def __init__(self):
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.getSaleOrderOverviewInfo/1"

        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }

    def _parse_and_save(self, resp, job_dict):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            detail = resp_json['data']
            detail['origin_id'] = detail['id']
            del detail['id']

            # 数据预处理（格式化）
            if "placedOrderAt" in detail and detail['placedOrderAt'] > 0:
                detail['placedOrderAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(detail['paymentAt'] / 1000))
            if "paymentAt" in detail and detail['paymentAt'] > 0:
                detail['paymentAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(detail['paymentAt'] / 1000))
            if "recoreInfoList" in detail and detail['recoreInfoList']:
                for val in detail['recoreInfoList']:
                    job_dict.update({
                        "recordId": val['recordId']
                    })
                    get_redis_conn().lpush("fordeal:order:item_current:jobs", json.dumps(job_dict, ensure_ascii=False))
                detail['recoreInfoList'] = json.dumps(detail['recoreInfoList'], ensure_ascii=False)
            if "shipmentInfo" in detail and detail['shipmentInfo']:
                detail['shipmentInfo'] = json.dumps(detail['shipmentInfo'], ensure_ascii=False)

            exist_sql = (
                f"select `id` from `order_overview` where `orderSn`=%(orderSn)s limit 1"
            )
            exist = db_query_one(exist_sql, {"orderSn": detail['orderSn']})
            if exist.empty:
                # 插入
                db_insert("order_overview", detail)
            else:
                # 更新
                db_update("order_overview", detail, f"`id`={exist['id']}")
        else:
            raise Exception(resp['msg'])

    def request_handle(self):
        """ 监听任务列表 """
        watch_key = "fordeal:order:overview:jobs"
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=10)

            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                try:
                    search_params = {
                        "orderSn": job_dict['orderSn'],
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
                    self._parse_and_save(resp, job_dict)
                except Exception as e:
                    # 重新投递任务
                    # get_redis_conn().lpush("fordeal:order:overview:jobs", job_str)
                    print(f"【{job_dict['orderSn']}】数据采集异常，err：{e}")
                    traceback.print_exc()
                    continue
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    OrderOverView().request_handle()
