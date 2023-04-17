import json
import sys

sys.path.append("/app/scripts/")
from db_helper import db_query
from redis_helper import get_redis_conn

if __name__ == '__main__':
    query_sql = (
        "select i.`shopId`, i.`shopName`, i.`createAt` as `start_date`"
        ", a.`username`, a.`password`, a.`provider_id`, a.`proxy_ip`"
        " from `accounts` as a left join `shops` as i on a.`shop_id`=i.`shopId`"
        " where a.`status`=1"
    )
    accounts = db_query(query_sql)
    for _, account in accounts.iterrows():
        account['start_date'] = account['start_date'].strftime('%Y-%m-%d')
        # 商品明细
        last_date = get_redis_conn().hget("fordeal:item:indicate:last_date", account['shopId'])
        if last_date is not None:
            account['start_date'] = last_date
        get_redis_conn().lpush("fordeal:item:indicate:jobs", json.dumps(account.to_dict(), ensure_ascii=False))

        # 商品流量排行
        last_date = get_redis_conn().hget("fordeal:item:flow_rank:last_date", account['shopId'])
        if last_date is not None:
            account['start_date'] = last_date
        get_redis_conn().lpush("fordeal:item:flow_rank:jobs", json.dumps(account.to_dict(), ensure_ascii=False))
