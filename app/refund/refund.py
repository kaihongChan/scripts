# 退货待确认

import hashlib
import json
import math
import time
import traceback
import sys
sys.path.append("/app/scripts/")
from app.base import Base


class Refund(Base):
    def __init__(self):
        super().__init__()
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listExceptionSkuPool/1"

        self._page_size = 50
        self._total = 0

        self._warehouse_id = 3

        query_sql = (
            "select i.`shopId`, i.`shopName`, a.`username`, a.`password`"
            " from `accounts` as a left join shops as i on a.`shop_id`=i.`shopId`"
            f" where a.`username`={self._username}"
        )
        self._account = self._db.query_row(query_sql)

    def _parse_and_save(self, job_dict, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            self._total = resp_json['data']['total']
            rows = resp_json['data']['rows']
            # 删除
            del_cond = f"`shopId`={job_dict['shopId']} and `confirmType`=0"
            self._db.db_del("refunds", del_cond)
            for row in rows:
                if "joinReturnDate" in row:
                    row['joinReturnDate'] = time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.localtime(row['joinReturnDate'] / 1000)
                    )
                row['origin_id'] = row['id']
                row['warehouseId'] = self._warehouse_id
                del row['id']
                # 插入
                refund_id = self._db.insert("refunds", row)

                exception_shoot_img_job = {
                    "username": job_dict['username'],
                    "password": job_dict['password'],
                    "refund_id": refund_id,
                    "ssuid": row['ssuId'],
                }
                if refund_id > 0 and row['ssuId']:
                    self._redis.lpush(f"fordeal:{self._username}:es_img:jobs", json.dumps(exception_shoot_img_job, ensure_ascii=False))
        else:
            raise Exception(resp_json['msg'])

    def request_handle(self):
        """
        请求预处理
        :return:
        """
        try:
            search_params = {"confirmType": 0, "warehouseId": self._warehouse_id, "page": 1, "pageSize": self._page_size}

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
            if self._total > self._page_size:
                page_num = math.ceil(self._total / self._page_size)
                for page_index in range(1, page_num):
                    p_time_stamp = int(round(time.time() * 1000))
                    search_params['page'] = page_index + 1
                    request_data['data'] = json.dumps(search_params, ensure_ascii=False)
                    request_data['ct'] = p_time_stamp
                    request_data['sign'] = hashlib.md5(str(p_time_stamp).encode()).hexdigest()
                    resp = self._requests_get(self._account, request_data)
                    self._parse_and_save(self._account, resp)
        except Exception as e:
            print(f"【{self._url}】数据采集异常，err：{e}")
            traceback.print_exc()


if __name__ == '__main__':
    Refund().request_handle()
