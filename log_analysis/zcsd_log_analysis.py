# -*- coding: utf-8 -*-
import sys
import time
from datetime import datetime

import configobj
import pymysql
import os

import pandas as pd
from sqlalchemy import create_engine


# def get_user_info():
#     sql_select = """
#         select open_id,nickname,phone,age,sex,duty from app.sys_user where enabled=1 and is_deleted=0 and open_id is not null and length(open_id)>1
#     """
#
#     # with psycopg2.connect(host=ZCSD_DB_HOST, port=CO['ZCSD_DB'].as_int('port'), database=CO['ZCSD_DB']['db'],
#     #                       user=CO['ZCSD_DB']['user'], password=CO['ZCSD_DB']['passwd']) as pg_conn:
#     #     pg_cur = pg_conn.cursor()
#     #     pg_cur.execute(sql_select)
#     #     pg_rows = pg_cur.fetchall()
#     #
#     #     # for r in pg_rows:
#     #     #     print(r)
#
#     df = pd.read_sql_query(sql_select, pg_engine)
#     return df


# def get_cont_info():
#     sql_select = """
#         select id, header, label_industry_ids, label_place_ids from app.policy where enabled=1 and id is not null and header is not null
#     """
#
#     engine = create_engine(
#         "postgresql://%s:%s@%s:%s/%s" % (ZCSD_DB_USER, ZCSD_DB_PASSWD, ZCSD_DB_HOST, ZCSD_DB_PORT, ZCSD_DB_DB))
#     df = pd.read_sql_query(sql_select, engine)
#     return df


def analyze_log(log_file, mysql_table):
    # user_df = get_user_info()
    # user_df['uid'] = user_df['open_id']
    # print(user_df.head(5))
    # print("-" * 160)

    # cont_df = get_cont_info()
    # print(cont_df)
    # print("-" * 160)

    log_df = pd.read_csv(log_file, header=None, sep="|", names=["cid", "uid", "pid", "ts", "lat", "lon", "op", "cont"], encoding='utf-8')
    # log_df = log_df.dropna(axis=0)
    log_df['cid'] = log_df['cid'].map(lambda x: "_NULL" if str(x).__len__() < 4 else str(x))
    log_df['uid'] = log_df['uid'].map(lambda x: "_NULL" if str(x).__len__() < 4 else str(x))
    log_df['pid'] = log_df['pid'].map(lambda x: "_NULL" if str(x).__len__() < 4 else str(x))
    log_df['header'] = log_df['cont'].map(lambda x: "_NULL" if str(x).__len__() < 4 else str(x))
    log_df['ts_str'] = log_df['ts'].map(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:00:00'))
    log_df['ts'] = log_df['ts'].map(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    log_df['lat_str'] = log_df['lat'].map(lambda x: round(x, 1))
    log_df['lon_str'] = log_df['lon'].map(lambda x: round(x, 1))
    print(log_df.tail(5))
    print("-" * 160)
    print(log_df.describe())

    log_df.to_csv("/home/james/workspace4py/JamesYqc/_data/t_zcsd_user_log_detail.csv")
    log_df.to_sql('t_zcsd_user_log_detail', zcsd_huawei_mysql_engine, index=False, if_exists="replace")


def main():
    log_file = '../_data/yqc_merge_20200806.log'
    mysql_table = 'sys_user'

    analyze_log(log_file, mysql_table)


if __name__ == '__main__':
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 1000)
    pd.set_option('display.max_rows', None)

    config_path = os.getcwd() + '/../james_config/zcsd.conf'
    CO = configobj.ConfigObj(config_path)

    DB_HOST = CO['LOCAL_DB']['host']
    DB_USER = CO['LOCAL_DB']['user']
    DB_PASSWD = CO['LOCAL_DB']['passwd']
    DB_DB = CO['LOCAL_DB']['db']
    DB_PORT = CO['LOCAL_DB'].as_int('port')

    DB_CONN = pymysql.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD,
                              db=DB_DB,
                              port=DB_PORT,
                              charset='utf8mb4')

    DB_COR = DB_CONN.cursor()

    my_engine = create_engine(
        "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8mb4" % (DB_USER, DB_PASSWD, DB_HOST, DB_PORT, DB_DB))

    ZCSD_HUAWEI_MYSQL_HOST = CO['ZCSD_HUAWEI_MYSQL_DB']['host']
    ZCSD_HUAWEI_MYSQL_USER = CO['ZCSD_HUAWEI_MYSQL_DB']['user']
    ZCSD_HUAWEI_MYSQL_PASSWD = CO['ZCSD_HUAWEI_MYSQL_DB']['passwd']
    ZCSD_HUAWEI_MYSQL_DB = CO['ZCSD_HUAWEI_MYSQL_DB']['db']
    ZCSD_HUAWEI_MYSQL_PORT = CO['ZCSD_HUAWEI_MYSQL_DB'].as_int('port')

    zcsd_huawei_mysql_engine = create_engine(
        "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8mb4" % (ZCSD_HUAWEI_MYSQL_USER, ZCSD_HUAWEI_MYSQL_PASSWD, ZCSD_HUAWEI_MYSQL_HOST, ZCSD_HUAWEI_MYSQL_PORT, ZCSD_HUAWEI_MYSQL_DB))


    main()

    sys.exit(0)
