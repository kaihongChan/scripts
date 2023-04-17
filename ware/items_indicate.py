# 商品（日频）明细
import datetime
import hashlib
import json
import time
import traceback
import sys
sys.path.append("/app/scripts/")
from db_helper import db_query_one, db_insert, db_update
from helper import request_get
from redis_helper import get_redis_conn


class ItemsIndicate:
    def __init__(self):
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.itemsIndicate/1"
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }
        self._page_size = 25
        self._continue = False

    def _parse_and_save(self, job_dict, resp):
        """ 数据解析及保存 """
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
                exist = db_query_one(exist_sql, {
                    "item_id": row['item_id'],
                    "shop_id": row['shop_id'],
                    "date": row['date'],
                })
                if exist.empty:
                    # 插入
                    db_insert("items_indicate", row)
                else:
                    # 更新
                    db_update("items_indicate", row, f"`id`={exist['id']}")

                # 写入
                job = {
                    "username": job_dict['username'],
                    "password": job_dict['password'],
                    "shop_id": row['shop_id'],
                    "date": datetime.datetime.strptime(row['date'], '%Y%m%d').strftime('%Y-%m-%d'),
                    "item_id": row['item_id'],
                    # "provider_id": job_dict['provider_id'],
                    # "proxy_ip": job_dict['proxy_ip'],
                }
                get_redis_conn().lpush("fordeal:item:overview:jobs", json.dumps(job, ensure_ascii=False))
                get_redis_conn().lpush("fordeal:item:trend:jobs", json.dumps(job, ensure_ascii=False))
                get_redis_conn().lpush("fordeal:item:traffic:jobs", json.dumps(job, ensure_ascii=False))
        else:
            raise Exception(f"请求响应：{resp.text}")

    def request_handle(self):
        """ 监听任务列表 """
        watch_key = 'fordeal:item:indicate:jobs'
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=5)
            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)

                # 开始采集
                today = datetime.date.today()
                start_date = datetime.datetime.strptime(job_dict['start_date'], "%Y-%m-%d").date()
                while start_date < today:
                    print(start_date)
                    try:
                        # 构造表单数据
                        search_params = {
                            "pageNo": 1,
                            "pageSize": 25,
                            "startDate": str(start_date),
                            "endDate": str(start_date),
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

                        resp = request_get(job_dict, self._url, self._headers, request_data)
                        self._parse_and_save(job_dict, resp)
                        # 处理分页
                        page_index = 2
                        while self._continue:
                            p_time_stamp = int(round(time.time() * 1000))
                            search_params['pageNo'] = page_index
                            request_data['data'] = json.dumps(search_params, ensure_ascii=False)
                            request_data['ct'] = p_time_stamp
                            request_data['sign'] = hashlib.md5(str(p_time_stamp).encode()).hexdigest()
                            resp = request_get(job_dict, self._url, self._headers, request_data)
                            self._parse_and_save(job_dict, resp)
                            page_index += 1
                    except Exception as e:
                        print(f"【{job_dict['username']}】数据采集异常，err：{e}")
                        traceback.print_exc()

                    start_date += datetime.timedelta(days=1)

                # 记录最后采集日期
                start_date -= datetime.timedelta(days=1)
                get_redis_conn().hset("fordeal:item:indicate:last_date", job_dict['shopId'], str(start_date))
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    ItemsIndicate().request_handle()
