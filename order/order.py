# 订单数据
import datetime
import hashlib
import json
import math
import time
import traceback
import sys

sys.path.append("/app/scripts/")
from helper import request_get
from redis_helper import get_redis_conn
from db_helper import db_query_one, db_insert, db_update


class Order:
    def __init__(self):
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listSaleOrder/1"

        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }

        self._page_size = 100
        self._order_total = 0

    def _parse_and_save(self, resp, job_dict):
        """ 数据解析及保存 """
        shop_id = job_dict['shopId']
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            self._order_total = resp_json['data']['total']
            rows = resp_json['data']['rows']

            sku_list = []
            for row in rows:
                row['origin_id'] = row['id']
                row['shop_id'] = shop_id
                del row['id']
                for sku in row['skus']:
                    sku['order_id'] = row['origin_id']
                    sku_list.append(sku)
                del row['skus']

                # 数据格式化
                row['placedOrderAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['placedOrderAt'] / 1000))
                if "paymentAt" in row and row['paymentAt'] > 0:
                    row['paymentAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['paymentAt'] / 1000))
                if "shippedAt" in row and row['shippedAt'] > 0:
                    row['shippedAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['shippedAt'] / 1000))
                if "signedAt" in row and row['signedAt'] > 0:
                    row['signedAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['signedAt'] / 1000))
                if "closedAt" in row and row['closedAt'] > 0:
                    row['closedAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['closedAt'] / 1000))
                if "canceledAt" in row and row['canceledAt'] > 0:
                    row['canceledAt'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['canceledAt'] / 1000))

                exist_sql = (
                    f"select `id`, `origin_id` from `orders` where `origin_id`=%(origin_id)s and `shop_id`=%(shop_id)s limit 1"
                )
                exist = db_query_one(query_sql=exist_sql, args={"origin_id": row['origin_id'], "shop_id": shop_id})
                if exist.empty:
                    # 插入
                    db_insert('orders', row)
                else:
                    # 更新
                    db_update('orders', row, f"id={exist['id']}")

                self._push_jobs(row['orderSn'], job_dict)

            # save sku
            for sku_item in sku_list:
                sku_exist_sql = (
                    f"select `id` from `order_skus` where `order_id`=%(order_id)s and `skuId`=%(sku_id)s limit 1"
                )
                sku_exist = db_query_one(sku_exist_sql, {"order_id": sku_item['order_id'], "sku_id": sku_item['skuId']})
                if sku_exist.empty:
                    # 插入
                    db_insert('order_skus', sku_item)
                else:
                    # 更新
                    db_update('order_skus', sku_item, f"id={sku_exist['id']}")

        else:
            raise Exception(f"{resp_json['msg']}")

    def _date_pre_handle(self, create_date):
        """ 订单采集日期预处理 """
        create_date = datetime.datetime.strptime(create_date, '%Y-%m-%d').date()
        today = datetime.date.today()
        sub_days = (today - create_date).days
        # 步长90天
        date_list = []
        for i in range(0, sub_days + 1, 90):
            day = create_date + datetime.timedelta(days=i)
            date_list.append(day)

        if today not in date_list:
            date_list.append(today)

        if len(date_list) == 1 and date_list[0] == today:
            date_list.append(today)

        return date_list

    def _push_jobs(self, order_sn, job_dict):
        job_dict.update(
            {
                "orderSn": order_sn,
            }
        )
        redis_conn = get_redis_conn()
        """ 任务投递 """
        redis_conn.lpush("fordeal:order:overview:jobs", json.dumps(job_dict, ensure_ascii=False))
        redis_conn.lpush("fordeal:order:item_overview:jobs", json.dumps(job_dict, ensure_ascii=False))

    def exec_handle(self):
        """ 监听任务列表 """
        watch_key = "fordeal:order:jobs"
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=5)

            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)
                try:
                    # 开始采集
                    date_list = self._date_pre_handle(job_dict['start_date'])
                    len_date = len(date_list)
                    for i in range(len_date):
                        if i == len_date - 1:
                            break

                        start_timestamp = int(round(time.mktime(time.strptime(str(date_list[i]), "%Y-%m-%d")) * 1000))
                        end_timestamp = int(
                            round(time.mktime(time.strptime(str(date_list[i + 1]), "%Y-%m-%d")) * 1000)
                        ) + 86399000
                        search_params = {
                            "page": 1,
                            "pageSize": self._page_size,
                            "status": "-1",
                            "deliverModel": "FAP",
                            "placedOrderAtBegin": start_timestamp,
                            "placedOrderAtEnd": end_timestamp
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

                        # 处理分页
                        if self._order_total > self._page_size:
                            page_num = math.ceil(self._order_total / self._page_size)
                            for page_index in range(2, page_num):
                                p_time_stamp = int(round(time.time() * 1000))
                                search_params['page'] = page_index
                                request_data['data'] = json.dumps(search_params, ensure_ascii=False)
                                request_data['ct'] = p_time_stamp
                                request_data['sign'] = hashlib.md5(str(p_time_stamp).encode()).hexdigest()
                                resp = request_get(job_dict, self._url, self._headers, request_data)
                                self._parse_and_save(resp, job_dict)

                        today = datetime.date.today()
                        get_redis_conn().hset("fordeal:order:last_date", job_dict['shopId'], str(today))
                except Exception as e:
                    print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                    traceback.print_exc()
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    Order().exec_handle()
