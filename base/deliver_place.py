# 发货地信息（更新频率：周）
import hashlib
import time
import traceback
import sys
sys.path.append("/app/scripts/")
from db_helper import db_query, db_insert, db_update, db_query_one
from helper import request_get


class DeliverPlace:
    def __init__(self):
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.getDeliverPlaceInfo/1"

    def request_handle(self):
        query_sql = (
            "select i.`shopId`, i.`shopName`, i.`createAt` as `start_date`"
            ", a.`username`, a.`password`, a.`provider_id`, a.`proxy_ip`"
            " from accounts as a left join shops as i on a.`shop_id`=i.`shopId`"
            " where a.`status`=1"
        )
        accounts = db_query(query_sql)
        for _, account in accounts.iterrows():
            try:
                # 构造请求参数
                timestamp = int(round(time.time() * 1000))
                request_params = {
                    "data": "{}",
                    "gw_ver": 1,
                    "ct": timestamp,
                    "plat": "h5",
                    "appname": "fordeal",
                    "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                }
                # 请求接口
                resp = request_get(account, self._url, self._headers, request_params)
                # 数据存储
                self._parse_and_save(resp, account['shopId'])
            except Exception as e:
                print(e)
                traceback.print_exc()
                continue

    def _parse_and_save(self, resp, shop_id):
        """ 数据入库 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = resp_json['data']
            for val in data:
                val['shopId'] = shop_id
                exist_sql = (
                    f"select * from `deliver_places` where `shopId`=%(shopId)s and `deliverPlaceCode`=%(deliverPlaceCode)s limit 1"
                )
                exist = db_query_one(exist_sql, {"shopId": shop_id, "deliverPlaceCode": val['deliverPlaceCode']})
                if exist.empty:
                    db_insert("deliver_places", val)
                else:
                    db_update("deliver_places", val, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])


if __name__ == '__main__':
    DeliverPlace().request_handle()
