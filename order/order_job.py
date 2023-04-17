import datetime
import json
import sys

sys.path.append("/app/scripts/")
from redis_helper import get_redis_conn
from db_helper import db_query

if __name__ == '__main__':
    query_sql = (
        "select i.`shopId`, i.`shopName`, i.`createAt` as `start_date`"
        ", a.`username`, a.`password`, a.`provider_id`, a.`proxy_ip`"
        " from accounts as a left join shops as i on a.`shop_id`=i.`shopId`"
        " where a.`status`=1"
    )
    accounts = db_query(query_sql)
    for _, account in accounts.iterrows():
        # 获取最新采集日期
        last_date = get_redis_conn().hget("fordeal:order:last_date", account['shopId'])
        if last_date is not None:
            last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d')
            account['start_date'] = (last_date - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        else:
            account['start_date'] = account['start_date'].strftime('%Y-%m-%d')

        get_redis_conn().lpush("fordeal:order:jobs", json.dumps(account.to_dict(), ensure_ascii=False))
