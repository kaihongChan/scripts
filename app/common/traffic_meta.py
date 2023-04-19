import hashlib
import time
import traceback
import sys
from requests import Response
sys.path.append("/app/scripts/")
from app.base import Base


class TrafficMeta(Base):
    def __init__(self):
        super().__init__()
        self._url = 'https://cn-ali-gw.fordeal.com/merchant/dwp.galio.trafficMeta/1'
        query_sql = (
            "select i.`shopId`, i.`shopName`, a.`username`, a.`password`"
            " from accounts as a left join shops as i on a.`shop_id`=i.`shopId`"
            f" where a.`username`={self._username}"
        )
        self._account = self._db.query_row(query_sql)

    def _parse_and_save(self, resp: Response):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = resp_json['data']
            for val in data:
                query_sql = (
                    f"select `id` from `traffic_meta` where `sourceId`=%(sourceId)s"
                )
                exist = self._db.query_row(query_sql=query_sql, args={"sourceId": val['sourceId']})
                if exist.empty:
                    self._db.insert("traffic_meta", val)
                else:
                    self._db.update("traffic_meta", val, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """ 发送请求 """
        try:
            timestamp = int(round(time.time() * 1000))
            search_params = {
                "data": "{}",
                "gw_ver": 1,
                "ct": timestamp,
                "plat": "h5",
                "appname": "fordeal",
                "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
            }
            resp = self._requests_get(self._account, search_params)
            self._parse_and_save(resp)
        except Exception as e:
            traceback.print_exc()
            print(f"【{self._url}】数据采集异常，err：{e}")


if __name__ == '__main__':
    TrafficMeta().request_handle()
