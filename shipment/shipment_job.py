import json
import sys


sys.path.append("/app/scripts/")
from db_helper import db_query
from redis_helper import get_redis_conn

if __name__ == '__main__':
    query_sql = (
        "select `id`, `username`, `password`, `shop_id`, `provider_id`, `proxy_ip` from `accounts` where `status`=1"
    )
    accounts = db_query(query_sql)
    for _, account in accounts.iterrows():
        # 写入任务列表
        get_redis_conn().lpush("fordeal:shipment:jobs", json.dumps(account.to_dict(), ensure_ascii=False))
        get_redis_conn().lpush("fordeal:inbound_plan:jobs", json.dumps(account.to_dict(), ensure_ascii=False))
