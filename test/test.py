import json
import os
import sys


sys.path.append("/app/scripts/")
from conf import COOKIE_PATH
from db_helper import db_update
from redis_helper import get_redis_conn

if __name__ == '__main__':
    # db_update('accounts', {
    #     "sync_status": 0,
    #     "sync_at": None
    # }, "id=3")
    # get_redis_conn().lpush("fordeal:account_new:jobs", json.dumps({
    #     'id': 3,
    #     'username': '17750697329',
    #     'password': 'e72cYQZ5gaU5ED/FMLYQFg=='
    # }))

    print(COOKIE_PATH)