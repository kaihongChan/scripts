# 订单数据
import datetime
import hashlib
import json
import math
import time
import traceback
import sys
sys.path.append("/app/scripts/")
from app.base import Base


class Order(Base):
    def __init__(self):
        super().__init__()
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listSaleOrder/1"

        self._page_size = 100
        self._order_total = 0

        query_sql = (
            "select i.`shopId`, i.`shopName`, i.`createAt` as `start_date`, a.`username`, a.`password`"
            " from accounts as a left join shops as i on a.`shop_id`=i.`shopId`"
            f" where a.`username`={self._username}"
        )
        self._account = self._db.query_row(query_sql).to_dict()
        last_date = self._redis.hget("fordeal:order:last_date", str(self._account['shopId']))
        if last_date is not None:
            last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d')
            self._account['start_date'] = (last_date - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        else:
            self._account['start_date'] = self._account['start_date'].strftime('%Y-%m-%d')

    def _parse_and_save(self, resp, job_dict):
        """
        数据解析及保存
        :param resp:
        :param job_dict:
        :return:
        """
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
                exist = self._db.query_row(query_sql=exist_sql, args={"origin_id": row['origin_id'], "shop_id": shop_id})
                if exist.empty:
                    # 插入
                    self._db.insert('orders', row)
                else:
                    # 更新
                    self._db.update('orders', row, f"id={exist['id']}")

                self._push_jobs(row['orderSn'], job_dict)

            # save sku
            for sku_item in sku_list:
                sku_exist_sql = (
                    f"select `id` from `order_skus` where `order_id`=%(order_id)s and `skuId`=%(sku_id)s limit 1"
                )
                sku_exist = self._db.query_row(sku_exist_sql, {"order_id": sku_item['order_id'], "sku_id": sku_item['skuId']})
                if sku_exist.empty:
                    # 插入
                    self._db.insert('order_skus', sku_item)
                else:
                    # 更新
                    self._db.update('order_skus', sku_item, f"id={sku_exist['id']}")
        else:
            raise Exception(f"{resp_json['msg']}")

    def _date_pre_handle(self, create_date):
        """
        订单采集日期预处理
        :param create_date:
        :return:
        """
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
        """
        订单详情任务投递
        :param order_sn:
        :param job_dict:
        :return:
        """
        job_dict.update(
            {
                "orderSn": order_sn,
            }
        )
        """ 任务投递 """
        self._redis.lpush(f"fordeal:{self._username}:order:overview:jobs", json.dumps(job_dict, ensure_ascii=False))
        self._redis.lpush(f"fordeal:{self._username}:order:item_overview:jobs", json.dumps(job_dict, ensure_ascii=False))

    def request_handle(self):
        """
        监听任务列表
        :return:
        """
        try:
            # 开始采集
            date_list = self._date_pre_handle(self._account['start_date'])
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
                resp = self._requests_get(self._account, request_data)
                self._parse_and_save(resp, self._account)

                # 处理分页
                if self._order_total > self._page_size:
                    page_num = math.ceil(self._order_total / self._page_size)
                    for page_index in range(2, page_num):
                        p_time_stamp = int(round(time.time() * 1000))
                        search_params['page'] = page_index
                        request_data['data'] = json.dumps(search_params, ensure_ascii=False)
                        request_data['ct'] = p_time_stamp
                        request_data['sign'] = hashlib.md5(str(p_time_stamp).encode()).hexdigest()
                        resp = self._requests_get(self._account, request_data)
                        self._parse_and_save(resp, self._account)

                today = datetime.date.today()
                self._redis.hset("fordeal:order:last_date", self._account['shopId'], str(today))
        except Exception as e:
            print(f"【{self._url}】数据采集异常，err：{e}")
            traceback.print_exc()


if __name__ == '__main__':
    Order().request_handle()
