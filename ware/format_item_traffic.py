import json
import sys
sys.path.append("/app/scripts/")
from db_helper import db_query, db_insert

if __name__ == '__main__':
    # 清洗商品流量来源
    query_sql = (
        "select * from `item_traffic`"
    )
    res = db_query(query_sql)

    for _, val in res.iterrows():
        print(f"【{val['item_id']}】处理中。。。")
        # 构造插入数据
        insert_data = {
            "shop_id": val['shop_id'],
            "item_id": val['item_id'],
            "date": str(val['date']),
        }
        for traffic in json.loads(val['traffic_list']):
            insert_data['source_id'] = traffic['sourceId']
            insert_data['parent_source_id'] = traffic['parentSourceId']
            for k, indicate in traffic['indicate'].items():
                if k == "ctm_order_view_rate":
                    indicate['content'] = indicate['content'].replace('%', '')

                insert_data[k] = indicate['content']

            db_insert("item_traffic_indicate", insert_data)
