import os
import sys
import time

from conf import APP_PATH

sys.path.append("/app/scripts/")
from utils.db_helper import get_db
from utils.redis_helper import get_redis


class Main:
    def __init__(self):
        """
        初始化
        """
        self._db = get_db()
        self._redis = get_redis()

        self._username = self._redis.get("fordeal:spider:account")

    def __del__(self):
        """
        资源释放
        """
        self._db.close()
        self._redis.close()

    def run(self):
        """
        开始执行
        """
        if self._username is not None and self._username != "":
            # 开始执行
            os.system(f"sh {APP_PATH}/order.sh")
            os.system(f"sh {APP_PATH}/refund.sh")
            os.system(f"sh {APP_PATH}/shipment.sh")
            os.system(f"sh {APP_PATH}/shop.sh")
            os.system(f"sh {APP_PATH}/ware.sh")

            self._finish()

    def _finish(self):
        """
        标记完成并更新数据
        """
        try:
            query_sql = (
                f"select `id`, `username` from accouts where `username`='{self._username}' limit 1"
            )
            account = self._db.query_row(query_sql).to_dict()
            if account:
                self._db.update(
                    table_name="accounts",
                    update_dict={
                        "sync_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "sync_status": 1
                    },
                    cond_str=f"username={self._username}"
                )
                self._redis.hset("fordeal:shop:last_sync", account['id'], time.strftime("%Y-%m-%d %H:%M:%S"))
            self._redis.delete("fordeal:spider:account")
        except Exception as err:
            print(f"更新失败：{err}")


if __name__ == '__main__':
    Main().run()
