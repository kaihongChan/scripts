# 店铺信息（更新频率：周）
import datetime
import hashlib
import json
import time
import traceback
import sys
sys.path.append("/app/scripts/")
from app.base import Base


class ShopTrans(Base):
    def __init__(self):
        super().__init__()
        self._url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.shopTrans/1'

        query_sql = (
            "select i.`shopId`, i.`shopName`, a.`username`, a.`password`, i.`createAt` as `last_date`"
            " from accounts as a left join shops as i on a.`shop_id`=i.`shopId`"
            f" where a.`username`='{self._username}' limit 1"
        )
        self._account = self._db.query_row(query_sql).to_dict()
        last_date = self._redis.hget("fordeal:shop:trans:last_date", str(self._account['shopId']))
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
                resp_dict = {}
                for key, val in data.items():
                    if type(val) == dict or type(val) == list:
                        resp_dict[key] = json.dumps(val, ensure_ascii=False)

                resp_dict['date'] = date
                resp_dict['shop_id'] = shop_id
                exist_sql = (
                    f"select `id` from `shop_trans` where shop_id=%(shop_id)s and `date`=%(date)s limit 1"
                )
                exist = self._db.query_row(exist_sql, {"shop_id": shop_id, "date": date})
                if exist.empty:
                    # 插入
                    self._db.insert("shop_trans", resp_dict)
                else:
                    self._db.update("shop_trans", resp_dict, f"id={exist['id']}")
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
                    "data": json.dumps({
                        "startDate": str(last_date),
                        "endDate": str(last_date),
                        "dateType": 1,
                        "selectType": 1
                    }, ensure_ascii=False),
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
            self._redis.hset("fordeal:shop:trans:last_date", str(self._account['shopId']), str(last_date))
        except Exception as e:
            traceback.print_exc()
            print(f"【{self._url}】数据采集异常，err：{e}")


if __name__ == '__main__':
    ShopTrans().request_handle()
