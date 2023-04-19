import pandas as pd
from sqlalchemy import text, create_engine
import sys

sys.path.append("/app/scripts/")
from conf import db_cfg


class DBHelper:
    def __init__(self):
        conn = (
            f"mysql+pymysql://{db_cfg['user']}:{db_cfg['passwd']}"
            f"@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['db']}?charset={db_cfg['charset']}"
        )
        db_engine = create_engine(conn)
        self._db_conn = db_engine.connect()

    def insert(self, table_name, insert_dict):
        """
        插入操作
        :param table_name:
        :param insert_dict:
        :return:
        """
        columns = ",".join(insert_dict.keys())
        values = tuple(insert_dict.values())

        insert_sql = (
            f"insert into {table_name} ({columns}) values {values}"
        )
        result = self._db_conn.execute(text(insert_sql)).lastrowid
        return result

    def update(self, table_name, update_dict, cond_str):
        """
        更新操作
        :param table_name: 表名
        :param update_dict: 更新字典
        :param cond_str: 条件语句
        :return:
        """
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
        result = self._db_conn.execute(update_sql, update_dict)
        return result

    def query_rows(self, query_sql, args=None):
        """

        :param query_sql:
        :param args:
        :return:
        """
        result = pd.read_sql(query_sql, con=self._db_conn, params=args)
        return result

    def query_row(self, query_sql, args=None):
        """

        :param query_sql:
        :param args:
        :return:
        """
        result = pd.read_sql(query_sql, con=self._db_conn, params=args)
        if result.empty:
            return pd.Series([], dtype='object')
        return result.iloc[0]

    def db_del(self, table_name, cond_str):
        """

        :param table_name:
        :param cond_str:
        :return:
        """
        del_sql = (
            f"delete from {table_name} where {cond_str}"
        )
        result = self._db_conn.execute(text(del_sql))
        return result

    def close(self):
        self._db_conn.close()


def get_db() -> DBHelper:
    """
    获取数据库连接实例
    """
    return DBHelper()
