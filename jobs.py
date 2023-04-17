import datetime
import json
import os
import traceback
import sys
import copy

sys.path.append("/app/scripts/")
from redis_helper import get_redis_conn
from db_helper import db_query_one
from shop.shop_info import ShopInfo

if __name__ == '__main__':
    while True:
        # 检测是否有店铺正在爬取数据
        current_job = get_redis_conn().get("fordeal:spider:shop")
        if current_job is not None:
            print("爬取任务正在进行中。。。")
            exit()

        # 临时文件
        filename = "/app/scripts/tmp/shop_handlers.lock"

        watch_key = "fordeal:account_new:jobs"
        json_str = get_redis_conn().brpop([watch_key], timeout=50)
        if json_str is not None:
            _, job_str = json_str
            job_dict = json.loads(job_str)
            # 执行命令行
            try:
                # 执行shop_info
                get_redis_conn().lpush("fordeal:shop:info:jobs", json.dumps(job_dict, ensure_ascii=False))
                ShopInfo().request_handle()

                query_sql = (
                    "select i.`shopId`, i.`shopName`, i.`createAt` as `start_date`, a.`username`, a.`password`"
                    " from accounts as a left join shops as i on a.`shop_id`=i.`shopId`"
                    " where a.`status`=1 and a.id=%(id)s"
                )
                account = db_query_one(query_sql, {"id": job_dict['id']})
                account['shopId'] = int(account['shopId'])
                account['start_date'] = account['start_date'].strftime('%Y-%m-%d')
                account['last_date'] = account['start_date']
                account = account.to_dict()

                # 设置当前采集店铺ID
                get_redis_conn().set("fordeal:spider:shop", account['shopId'])
                # 删除临时文件
                os.remove(filename)
                # 投入采集任务
                get_redis_conn().lpush(
                    "fordeal:deliver_place:jobs",
                    json.dumps(account, ensure_ascii=False)
                )

                get_redis_conn().lpush(
                    "fordeal:traffic_meta:jobs",
                    json.dumps(account, ensure_ascii=False)
                )

                order_job = copy.copy(account)
                order_last_date = get_redis_conn().hget("fordeal:order:last_date", str(account['shopId']))
                if order_last_date is not None:
                    last_date = datetime.datetime.strptime(order_last_date, '%Y-%m-%d')
                    order_job['start_date'] = (last_date - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
                    order_job['last_date'] = order_job['start_date']
                get_redis_conn().lpush(
                    "fordeal:order:jobs",
                    json.dumps(order_job, ensure_ascii=False)
                )

                get_redis_conn().lpush(
                    "fordeal:refund:jobs",
                    json.dumps(account, ensure_ascii=False)
                )

                get_redis_conn().lpush(
                    "fordeal:shipment:jobs",
                    json.dumps(account, ensure_ascii=False)
                )

                get_redis_conn().lpush(
                    "fordeal:inbound_plan:jobs",
                    json.dumps(account, ensure_ascii=False)
                )

                shop_trend_job = copy.copy(account)
                shop_trend_last_date = get_redis_conn().hget("fordeal:shop:trend:last_date", str(account['shopId']))
                if shop_trend_last_date is not None:
                    shop_trend_job['start_date'] = shop_trend_last_date
                    shop_trend_job['last_date'] = shop_trend_job['start_date']
                get_redis_conn().lpush(
                    "fordeal:shop:trend:jobs",
                    json.dumps(shop_trend_job, ensure_ascii=False)
                )

                shop_flow_source_job = copy.copy(account)
                shop_flow_source_last_date = get_redis_conn().hget("fordeal:shop:flow_source:last_date", str(account['shopId']))
                if shop_flow_source_last_date is not None:
                    shop_flow_source_job['start_date'] = shop_flow_source_last_date
                    shop_flow_source_job['last_date'] = shop_flow_source_job['start_date']
                get_redis_conn().lpush(
                    "fordeal:shop:flow_source:job",
                    json.dumps(shop_flow_source_job, ensure_ascii=False)
                )

                shop_traffic_job = copy.copy(account)
                shop_traffic_last_date = get_redis_conn().hget("fordeal:shop:traffic:last_date", str(account['shopId']))
                if shop_traffic_last_date is not None:
                    shop_traffic_job['start_date'] = shop_traffic_last_date
                    shop_traffic_job['last_date'] = shop_traffic_job['start_date']
                get_redis_conn().lpush(
                    "fordeal:shop:traffic:jobs",
                    json.dumps(shop_traffic_job, ensure_ascii=False)
                )

                shop_trans_job = copy.copy(account)
                shop_trans_last_date = get_redis_conn().hget("fordeal:shop:trans:last_date", str(account['shopId']))
                if shop_trans_last_date is not None:
                    shop_trans_job['start_date'] = shop_trans_last_date
                    shop_trans_job['last_date'] = shop_trans_job['start_date']
                get_redis_conn().lpush(
                    "fordeal:shop:trans:jobs",
                    json.dumps(shop_trans_job, ensure_ascii=False)
                )

                item_flow_rank_job = copy.copy(account)
                item_flow_rank_last_date = get_redis_conn().hget("fordeal:item:flow_rank:last_date", str(account['shopId']))
                if item_flow_rank_last_date is not None:
                    item_flow_rank_job['start_date'] = item_flow_rank_last_date
                    item_flow_rank_job['last_date'] = item_flow_rank_job['start_date']
                get_redis_conn().lpush(
                    "fordeal:item:flow_rank:jobs",
                    json.dumps(item_flow_rank_job, ensure_ascii=False)
                )

                item_indicate_job = copy.copy(account)
                item_indicate_last_date = get_redis_conn().hget("fordeal:item:indicate:last_date", str(account['shopId']))
                if item_indicate_last_date is not None:
                    item_indicate_job['start_date'] = item_indicate_last_date
                    item_indicate_job['last_date'] = item_indicate_job['start_date']
                get_redis_conn().lpush(
                    "fordeal:item:indicate:jobs",
                    json.dumps(item_indicate_job, ensure_ascii=False)
                )

            except Exception as e:
                print(f"err：{e}")
                traceback.print_exc()
                continue
        else:
            print(f"{watch_key}监听结束。。。")
            break
