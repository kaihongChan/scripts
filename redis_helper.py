import sys

import redis
sys.path.append("/app/scripts/")
from conf import redis_cfg


def get_redis_conn():
    """ 获取redis连接实例 """
    conf = redis_cfg
    pool = redis.ConnectionPool(
        host=conf['host'],
        port=conf['port'],
        password=conf['password'],
        encoding='utf-8',
        decode_responses=conf['decode_responses'],
        db=conf['db'],
        max_connections=1000
    )
    # 去连接池中获取一个连接
    redis_conn = redis.Redis(connection_pool=pool)

    return redis_conn
