# 商品（每天9点）明细
import datetime
import hashlib
import json
import time
import traceback
import sys
sys.path.append("/app/scripts/")
from app.base import Base


class ItemsIndicate(Base):
    def __init__(self):
        super().__init__()
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.itemsIndicate/1"
        self._page_size = 25
        self._continue = False

        query_sql = (
            "select i.`shopId`, i.`shopName`, i.`createAt` as `last_date`, a.`username`, a.`password`"
            " from `accounts` as a left join `shops` as i on a.`shop_id`=i.`shopId`"
            f" where a.`username`='{self._username}' limit 1"
        )
        self._account = self._db.query_row(query_sql)
        last_date = self._redis.hget("fordeal:item:indicate:last_date", str(self._account['shopId']))
        if last_date is not None:
            self._account['last_date'] = last_date
        else:
            self._account['last_date'] = self._account['last_date'].strftime('%Y-%m-%d')

    def _parse_and_save(self, job_dict, resp):
        """
        数据解析及保存
        :param job_dict:
        :param resp:
        :return:
        """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            if "data" not in resp_json:
                self._continue = False
                return

            rows = resp_json['data']
            if len(rows) == self._page_size:
                self._continue = True
            else:
                self._continue = False

            for row in rows:
                row['ctm_cart_view_rate'] = float(row['ctm_cart_view_rate'].replace("%", "")) / 100
                row['ctm_item_view_rate_new'] = float(row['ctm_item_view_rate_new'].replace("%", "")) / 100
                row['ctm_order_view_rate'] = float(row['ctm_order_view_rate'].replace("%", "")) / 100

                exist_sql = (
                    f"select `id`, `item_id` from `items_indicate`"
                    f" where `item_id`=%(item_id)s and `shop_id`=%(shop_id)s"
                    f" and `date`=%(date)s limit 1"
                )
                exist = self._db.query_row(
                    exist_sql, {
                        "item_id": row['item_id'],
                        "shop_id": row['shop_id'],
                        "date": row['date'],
                    }
                )
                if exist.empty:
                    # 插入
                    self._db.insert("items_indicate", row)
                else:
                    # 更新
                    self._db.update("items_indicate", row, f"`id`={exist['id']}")

                # 写入
                job = {
                    "username": job_dict['username'],
                    "password": job_dict['password'],
                    "shop_id": row['shop_id'],
                    "date": datetime.datetime.strptime(row['date'], '%Y%m%d').strftime('%Y-%m-%d'),
                    "item_id": row['item_id'],
                }
                self._redis.lpush(f"fordeal:{self._username}:item:overview:jobs", json.dumps(job, ensure_ascii=False))
                self._redis.lpush(f"fordeal:{self._username}:item:trend:jobs", json.dumps(job, ensure_ascii=False))
                self._redis.lpush(f"fordeal:{self._username}:item:traffic:jobs", json.dumps(job, ensure_ascii=False))
        else:
            raise Exception(f"请求响应：{resp.text}")

    def request_handle(self):
        """
        请求预处理（监听任务列表）
        :return:
        """
        # 开始采集
        today = datetime.date.today()
        last_date = datetime.datetime.strptime(self._account['last_date'], "%Y-%m-%d").date()
        while last_date < today:
            print(last_date)
            try:
                # 构造表单数据
                search_params = {
                    "pageNo": 1,
                    "pageSize": 25,
                    "startDate": str(last_date),
                    "endDate": str(last_date),
                    "dateType": 1,
                    "selectType": 4,
                    "order": "desc",
                    "orderField": "ctm_item_view_cnt"
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
                self._parse_and_save(self._account, resp)
                # 处理分页
                page_index = 2
                while self._continue:
                    print(page_index)
                    p_time_stamp = int(round(time.time() * 1000))
                    search_params['pageNo'] = page_index
                    request_data['data'] = json.dumps(search_params, ensure_ascii=False)
                    request_data['ct'] = p_time_stamp
                    request_data['sign'] = hashlib.md5(str(p_time_stamp).encode()).hexdigest()
                    resp = self._requests_get(self._account, request_data)
                    self._parse_and_save(self._account, resp)
                    page_index += 1
            except Exception as e:
                print(f"【{self._url}】数据采集异常，err：{e}")
                traceback.print_exc()

            last_date += datetime.timedelta(days=1)

        # 记录最后采集日期
        last_date -= datetime.timedelta(days=1)
        self._redis.hset("fordeal:item:indicate:last_date", str(self._account['shopId']), str(last_date))


if __name__ == '__main__':
    ItemsIndicate().request_handle()
