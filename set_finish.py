import sys
import time

from utils.db_helper import get_db
from utils.redis_helper import get_redis

sys.path.append("/app/scripts/")

if __name__ == '__main__':
    print('finish...')

    redis_obj = get_redis()
    db_obj = get_db()

    try:
        username = redis_obj.get("fordeal:spider:account")
        query_sql = (
            f"select `id`, `username` from accouts where `username`='{username}' limit 1"
        )
        account = db_obj.query_row("").to_dict()
        if username is not None and username != "":
            db_obj.update(
                table_name="accounts",
                update_dict={
                    "sync_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "sync_status": 1
                },
                cond_str=f"username={username}"
            )
            redis_obj.hset("fordeal:shop:last_sync", account['id'], time.strftime("%Y-%m-%d %H:%M:%S"))
        redis_obj.delete("fordeal:spider:account")
    except Exception as err:
        print(f"更新失败：{err}")

    redis_obj.close()
    db_obj.close()
