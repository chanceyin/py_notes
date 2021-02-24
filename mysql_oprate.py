#!/usr/bin/env python
# encoding: utf-8
"""
@author: rcyin
@license: (C) Copyright 2013-2017, Node Supply Chain Manager Corporation Limited.
@software: pycharm
@file: mysql_oprate.py
@time: 2020/10/18 16:38
@desc: 操作mysql库
"""

import MySQLdb
from MySQLdb._exceptions import OperationalError

MyOperationalError = OperationalError


class MysqlControl(object):
    def __init__(self, host, user, pwd, db='', port=3306, charset='utf8'):
        self.conn = MySQLdb.connect(host=host, port=int(port), user=user, db=db, password=pwd, charset=charset)

    def create_db(self, db_name):
        cursor = self.conn.cursor()
        cursor.execute(f'CREATE DATABASE {db_name} CHARACTER SET utf8 COLLATE utf8_general_ci;')
        self.conn.commit()

    def select_db(self, database):
        self.conn.select_db(database)

    def ddl_exec(self, sql_info):
        """

        :param sql_info:
        :return:
        """
        cursor = self.conn.cursor()
        cursor.execute(sql_info)
        cursor.close()

    def insert_data(self, table_name, data_list, fields=''):
        """
        批量数据插入
        :param sql_info:
        :param data_list:
        :return:
        """
        cursor = self.conn.cursor()
        try:
            fields_num = len(data_list[0])
            fields_str = ",".join(['%s'] * fields_num)
            sql_info = f'INSERT INTO {table_name}({fields}) VALUES({fields_str});' if fields else \
                f'INSERT INTO {table_name} VALUES({fields_str});'
            cursor.executemany(sql_info, data_list)
            self.conn.commit()
        except Exception as e:
            print(f'executemany sql error {str(e)}')
            cursor.close()
            self.conn.rollback()

    def replace_insert(self, table_name, uniq_fields, other_fields, data_list):
        """

        :param sql_info:
        :return:
        """
        cursor = self.conn.cursor()
        try:
            all_fields = ','.join(uniq_fields + other_fields)
            fields_str = ",".join(['%s'] * (len(uniq_fields) + len(other_fields)))
            names_fields_str = ','.join([f'{field}=values({field})' for field in other_fields])
            sql_info = f'INSERT INTO {table_name} ({all_fields}) VALUES ({fields_str}) ON DUPLICATE KEY UPDATE {names_fields_str};'
            cursor.executemany(sql_info, data_list)
            self.conn.commit()
        except Exception as e:
            print(f'run error {str(e)}')
            cursor.close()
            self.conn.rollback()

    def do_query(self, sql_info):
        """

        :param sql_info:
        :return:
        """
        cursor = self.conn.cursor()
        cursor.execute(sql_info)
        yield from cursor.fetchall()

    def close(self):
        if self.conn:
            self.conn.close()


if __name__ == '__main__':
    pass
