import time

import pandas as pd
from sqlalchemy import text, create_engine
import sys
sys.path.append("/app/scripts/")
from conf import db_cfg


def get_db_conn():
    """ 获取db连接实例 """
    conn = (
        f"mysql+pymysql://{db_cfg['user']}:{db_cfg['passwd']}"
        f"@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['db']}?charset={db_cfg['charset']}"
    )
    db_engine = create_engine(conn)
    db_conn = db_engine.connect()

    return db_conn


def db_insert(table_name, insert_dict):
    """ 数据库插入操作 """
    db_conn = get_db_conn()
    columns = ",".join(insert_dict.keys())
    values = tuple(insert_dict.values())

    insert_sql = (
        f"insert into {table_name} ({columns}) values {values}"
    )
    result = db_conn.execute(text(insert_sql)).lastrowid
    db_conn.close()
    return result


def db_update(table_name, update_dict, cond_str):
    """ 数据库更新操作 """
    db_conn = get_db_conn()
    k_v_map = ""
    val_list = []
    for column, val in update_dict.items():
        if type(val) == bool:
            val = int(val)
        k_v_map += f"{column}=%({column})s,"
        val_list.append(val)
    k_v_map = k_v_map[0:-1]
    update_sql = (
        f"update {table_name} set {k_v_map} where {cond_str}"
    )
    result = db_conn.execute(update_sql, update_dict)
    db_conn.close()
    return result


def db_query(query_sql, args=None):
    """ 查询多条记录 """
    db_conn = get_db_conn()
    result = pd.read_sql(query_sql, con=db_conn, params=args)
    db_conn.close()
    return result


def db_query_one(query_sql, args=None):
    """ 查询单条记录 """
    db_conn = get_db_conn()
    result = pd.read_sql(query_sql, con=db_conn, params=args)
    db_conn.close()
    if result.empty:
        return pd.Series([], dtype='object')
    return result.iloc[0]


def db_del(table_name, cond_str):
    db_conn = get_db_conn()
    del_sql = (
        f"delete from {table_name} where {cond_str}"
    )
    result = db_conn.execute(text(del_sql))
    db_conn.close()
    return result
