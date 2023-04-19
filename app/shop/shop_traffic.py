# 店铺概览数据（需二次清洗）
import datetime
import hashlib
import json
import time
import traceback
import sys
sys.path.append("/app/scripts/")
from app.base import Base
from utils.np_encoder import NpEncoder


class ShopTraffic(Base):
    def __init__(self):
        super().__init__()
        self._url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.shopTraffic/1'

        query_sql = (
            "select i.`shopId`, i.`shopName`, a.`username`, a.`password`, i.`createAt` as `last_date`"
            " from accounts as a left join shops as i on a.`shop_id`=i.`shopId`"
            f" where a.`username`='{self._username}' limit 1"
        )
        self._account = self._db.query_row(query_sql).to_dict()
        last_date = self._redis.hget("fordeal:shop:traffic:last_date", str(self._account['shopId']))
        if last_date is not None:
            self._account['last_date'] = last_date
        else:
            self._account['last_date'] = self._account['last_date'].strftime('%Y-%m-%d')

    def _parse_and_save(self, job_dict, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = resp_json['data']
            if "meta_list" in data:
                data['meta_list'] = json.dumps(data['meta_list'], ensure_ascii=False)
            if "first_traffic_trend" in data:
                for ftt in data['first_traffic_trend']:
                    if ftt['date'] != job_dict['date']:
                        continue
                    query_sql = (
                        f"select `id` from `shop_first_traffic_trend` where `shop_id`=%(shop_id)s"
                        f" and `date`=%(date)s and `source_id`=%(source_id)s limit 1"
                    )
                    exist = self._db.query_row(
                        query_sql, {
                            "shop_id": ftt['shop_id'],
                            "date": ftt['date'],
                            "source_id": ftt['source_id'],
                        }
                        )
                    if exist.empty:
                        self._db.insert("shop_first_traffic_trend", ftt)
                    else:
                        self._db.update("shop_first_traffic_trend", ftt, f"id={exist['id']}")
                data['first_traffic_trend'] = json.dumps(data['first_traffic_trend'], ensure_ascii=False)
            if "second_traffic_rank" in data:
                for sstr in data['second_traffic_rank']:
                    sstr['date'] = job_dict['date']
                    sstr['shop_id'] = job_dict['shopId']
                    if "ctm_order_view_rate" in sstr:
                        sstr['ctm_order_view_rate'] = float(sstr['ctm_order_view_rate'].replace('%', '')) / 100
                    query_sql = (
                        f"select `id` from `shop_second_traffic_rank` where `shop_id`=%(shop_id)s"
                        f" and `date`=%(date)s and `source_l2_id`=%(source_l2_id)s"
                    )
                    exist = self._db.query_row(
                        query_sql, {
                            "shop_id": sstr['shop_id'],
                            "date": sstr['date'],
                            "source_l2_id": sstr['source_l2_id'],
                        }
                        )
                    if exist.empty:
                        self._db.insert("shop_second_traffic_rank", sstr)
                    else:
                        self._db.update("shop_second_traffic_rank", sstr, f"id={exist['id']}")
                data['second_traffic_rank'] = json.dumps(data['second_traffic_rank'], ensure_ascii=False, cls=NpEncoder)
            data['shop_id'] = job_dict['shopId']
            data['request_date'] = job_dict['date']

            query_sql = (
                f"select `id` from `shop_traffic` where `shop_id`=%(shop_id)s and `request_date`=%(request_date)s"
            )
            exist = self._db.query_row(
                query_sql, {
                    "shop_id": job_dict['shopId'],
                    "request_date": job_dict['date'],
                }
                )
            if exist.empty:
                self._db.insert("shop_traffic", data)
            else:
                self._db.update("shop_traffic", data, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """
        请求预处理
        :return:
        """
        try:
            today = datetime.date.today()
            last_date = datetime.datetime.strptime(self._account['last_date'], "%Y-%m-%d").date()
            while last_date < today:
                print(last_date)
                timestamp = int(round(time.time() * 1000))
                search_params = {
                    "data": json.dumps(
                        {
                            "startDate": str(last_date),
                            "endDate": str(last_date),
                            "dateType": 1,
                            "selectType": 4
                        }, ensure_ascii=False
                    ),
                    "gw_ver": 1,
                    "ct": timestamp,
                    "plat": "h5",
                    "appname": "fordeal",
                    "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                }
                resp = self._requests_get(self._account, search_params)
                self._account['date'] = str(last_date)
                self._parse_and_save(self._account, resp)
                last_date += datetime.timedelta(days=1)

            last_date -= datetime.timedelta(days=1)
            self._redis.hset("fordeal:shop:traffic:last_date", str(self._account['shopId']), str(last_date))
        except Exception as e:
            traceback.print_exc()
            print(f"【{self._url}】数据采集异常，err：{e}")


if __name__ == '__main__':
    ShopTraffic().request_handle()
