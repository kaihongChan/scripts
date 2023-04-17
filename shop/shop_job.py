import json
import sys


sys.path.append("/app/scripts/")
from db_helper import db_query
from redis_helper import get_redis_conn

if __name__ == '__main__':
    query_sql = (
        "select i.`shopId`, i.`shopName`, a.`username`, a.`password`,"
        " i.`createAt` as `last_date`, a.`provider_id`, a.`proxy_ip`"
        " from accounts as a left join shops as i on a.`shop_id`=i.`shopId`"
        " where a.`status`=1"
    )
    accounts = db_query(query_sql)

    for _, account in accounts.iterrows():
        account['last_date'] = account['last_date'].strftime('%Y-%m-%d')
        # shop_trend
        last_date = get_redis_conn().hget("fordeal:shop:trend:last_date", str(account['shopId']))
        if last_date is not None:
            account['last_date'] = last_date
        get_redis_conn().lpush("fordeal:shop:trend:jobs", json.dumps(account.to_dict(), ensure_ascii=False))

        # shop_flow_source
        last_date = get_redis_conn().hget("fordeal:shop:flow_source:last_date", str(account['shopId']))
        if last_date is not None:
            account['last_date'] = last_date
        get_redis_conn().lpush("fordeal:shop:flow_source:job", json.dumps(account.to_dict(), ensure_ascii=False))

        # shop_traffic
        last_date = get_redis_conn().hget("fordeal:shop:traffic:last_date", str(account['shopId']))
        if last_date is not None:
            account['last_date'] = last_date
        get_redis_conn().lpush("fordeal:shop:traffic:jobs", json.dumps(account.to_dict(), ensure_ascii=False))

        # shop_trans
        last_date = get_redis_conn().hget("fordeal:shop:trans:last_date", str(account['shopId']))
        if last_date is not None:
            account['last_date'] = last_date
        get_redis_conn().lpush("fordeal:shop:trans:jobs", json.dumps(account.to_dict(), ensure_ascii=False))
