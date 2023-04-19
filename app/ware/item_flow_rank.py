import datetime
import hashlib
import json
import time
import traceback
import sys

sys.path.append("/app/scripts/")
from app.base import Base


class ItemFlowRank(Base):
    def __init__(self):
        super().__init__()
        self._url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.itemFlowRank/1'

        query_sql = (
            "select i.`shopId`, i.`shopName`, i.`createAt` as `last_date`, a.`username`, a.`password`"
            " from `accounts` as a left join `shops` as i on a.`shop_id`=i.`shopId`"
            f" where a.`username`='{self._username}'"
        )
        self._account = self._db.query_row(query_sql).to_dict()
        last_date = self._redis.hget("fordeal:item:flow_rank:last_date", str(self._account['shopId']))
        if last_date is not None:
            self._account['last_date'] = last_date
        else:
            self._account['last_date'] = self._account['last_date'].strftime('%Y-%m-%d')

    def _parse_and_save(self, shop_id, date, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            if "data" in resp_json:
                data = resp_json['data']
                if "item_list" in data:
                    for item in data['item_list']:
                        if "ctm_item_view_rate_new" in item:
                            item['ctm_item_view_rate_new'] = float(item['ctm_item_view_rate_new'].replace('%', '')) / 100
                        if "ctm_cart_view_rate" in item:
                            item['ctm_cart_view_rate'] = float(item['ctm_cart_view_rate'].replace('%', '')) / 100
                        if "ctm_order_view_rate" in item:
                            item['ctm_order_view_rate'] = float(item['ctm_order_view_rate'].replace('%', '')) / 100
                        query_sql = (
                            f"select `id` from `item_flow_rank_list` where `shop_id`=%(shop_id)s"
                            f" and `date`=%(date)s and item_id=%(item_id)s limit 1"
                        )
                        exist = self._db.query_row(
                            query_sql, {
                                "shop_id": shop_id,
                                "date": item['date'],
                                "item_id": item['item_id']
                            }
                            )
                        if exist.empty:
                            self._db.insert("item_flow_rank_list", item)
                        else:
                            self._db.update("item_flow_rank_list", item, f"id={exist['id']}")
                    data['item_list'] = json.dumps(data['item_list'], ensure_ascii=False)
                if "meta_list" in data:
                    data['meta_list'] = json.dumps(data['meta_list'], ensure_ascii=False)
                data['date'] = date
                data['shop_id'] = shop_id
                exist_sql = (
                    f"select `id` from `item_flow_rank` where shop_id=%(shop_id)s and `date`=%(date)s limit 1"
                )
                exist = self._db.query_row(
                    exist_sql, {
                        "shop_id": shop_id,
                        "date": date
                    }
                    )
                if exist.empty:
                    # 插入
                    self._db.insert("item_flow_rank", data)
                else:
                    self._db.update("item_flow_rank", data, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """ 发送请求 """
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
                            "selectType": 1
                        }, ensure_ascii=False
                    ),
                    "gw_ver": 1,
                    "ct": timestamp,
                    "plat": "h5",
                    "appname": "fordeal",
                    "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                }
                resp = self._requests_get(self._account, search_params)
                self._parse_and_save(self._account['shopId'], str(last_date), resp)
                last_date += datetime.timedelta(days=1)

            last_date -= datetime.timedelta(days=1)
            self._redis.hset("fordeal:item:flow_rank:last_date", str(self._account['shopId']), str(last_date))
        except Exception as e:
            traceback.print_exc()
            print(f"【{self._url}】数据采集异常，err：{e}")


if __name__ == '__main__':
    ItemFlowRank().request_handle()
