# 退货待确认

import hashlib
import json
import time
import traceback
import sys


sys.path.append("/app/scripts/")
from app.base import Base


class ExceptionImg(Base):
    def __init__(self):
        super().__init__()
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listExceptionShootImg/1"

    def _parse_and_save(self, refund_id, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = json.dumps(resp_json['data'], ensure_ascii=False)
            exist_sql = (
                f"select `id` from `list_exception_shoot_imgs` where `refund_id`=%(refund_id)s limit 1"
            )
            exist = self._db.query_row(exist_sql, {"refund_id": refund_id})
            if exist.empty:
                # 插入
                self._db.insert("list_exception_shoot_imgs", {
                    "refund_id": refund_id,
                    "data": data,
                })
            else:
                self._db.update("list_exception_shoot_imgs", {"data": data}, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def exec_handle(self):
        """
        请求预处理（监听任务列表）
        :return:
        """
        watch_key = f'fordeal:{self._username}:es_img:jobs'
        while True:
            json_str = self._redis.brpop([watch_key], timeout=10)
            if json_str is not None:
                _, job_str = json_str
                job_dict = json.loads(job_str)

                try:
                    search_params = {"ssuId": job_dict['ssuid']}
                    timestamp = int(round(time.time() * 1000))
                    request_data = {
                        "gw_ver": 1,
                        "data": json.dumps(search_params, ensure_ascii=False),
                        "ct": timestamp,
                        "plat": "h5",
                        "appname": "fordeal",
                        "sign: ": hashlib.md5(str(timestamp).encode()).hexdigest(),
                    }
                    resp = self._requests_get(job_dict, request_data)
                    self._parse_and_save(job_dict['refund_id'], resp)
                except Exception as e:
                    print(f"【{job_dict['ssuid']}】数据采集异常，err：{e}")
                    traceback.print_exc()
                    continue
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    ExceptionImg().exec_handle()
