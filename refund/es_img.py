# 退货待确认

import hashlib
import json
import time
import traceback
import sys


sys.path.append("/app/scripts/")
from db_helper import db_insert, db_update, db_query_one
from redis_helper import get_redis_conn
from helper import request_get


class ExceptionImg:
    def __init__(self):
        self._url = "https://cn-ali-gw.fordeal.com/merchant/dwp.galio.listExceptionShootImg/1"

        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
            'referer': 'https://seller.fordeal.com/',
            'origin': 'https: // seller.fordeal.com',
            'accept': 'application/json, text/plain, */*',
        }

    def test(self):
        print({"refund_id": 1, "ssuid": 137984871})

    def _parse_and_save(self, refund_id, resp):
        """ 数据解析及保存 """
        resp_json = resp.json()
        if resp.status_code == 200 and resp_json['code'] == 1001:
            data = json.dumps(resp_json['data'], ensure_ascii=False)
            exist_sql = (
                f"select `id` from `list_exception_shoot_imgs` where `refund_id`=%(refund_id)s limit 1"
            )
            exist = db_query_one(exist_sql, {"refund_id": refund_id})
            if exist.empty:
                # 插入
                db_insert("list_exception_shoot_imgs", {
                    "refund_id": refund_id,
                    "data": data,
                })
            else:
                db_update("list_exception_shoot_imgs", {"data": data}, f"id={exist['id']}")
        else:
            raise Exception(resp_json['msg'])

    def exec_handle(self):
        """ 监听任务列表 """
        watch_key = 'fordeal:refund:es_img:jobs'
        while True:
            json_str = get_redis_conn().brpop([watch_key], timeout=10)
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
                    resp = request_get(job_dict, self._url, self._headers, request_data)
                    self._parse_and_save(job_dict['refund_id'], resp)
                except Exception as e:
                    print(f"【{job_dict['ssuid']}】数据采集异常，err：{e}")
                    traceback.print_exc()
                    continue
            else:
                print(f"{watch_key}监听结束。。。")
                break


if __name__ == '__main__':
    # ExceptionImg().test()
    ExceptionImg().exec_handle()
