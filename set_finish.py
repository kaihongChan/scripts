import sys
import time

sys.path.append("/app/scripts/")
from db_helper import db_update
from redis_helper import get_redis_conn

if __name__ == '__main__':
    print('finish...')
    try:
        redis_conn = get_redis_conn()
        shop_id = redis_conn.get("fordeal:spider:shop")
        redis_conn.delete("fordeal:spider:shop")
        if shop_id is not None and int(shop_id) > 0:
            db_update(
                table_name="accounts",
                update_dict={
                    "sync_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "sync_status": 1
                },
                cond_str=f"shop_id={shop_id}"
            )
            redis_conn.hset("fordeal:shop:last_sync", shop_id, time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as err:
        print(f"更新失败：{err}")
