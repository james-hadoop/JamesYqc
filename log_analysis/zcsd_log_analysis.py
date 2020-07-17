# -*- coding: utf-8 -*-
import configobj
import pymysql
import os

import pandas as pd
from sqlalchemy import create_engine


def get_user_info():
    sql_select = """
        select open_id,nickname,phone,age,sex,duty from app.sys_user where enabled=1 and is_deleted=0 and open_id is not null and length(open_id)>1
    """

    # with psycopg2.connect(host=ZCSD_DB_HOST, port=CO['ZCSD_DB'].as_int('port'), database=CO['ZCSD_DB']['db'],
    #                       user=CO['ZCSD_DB']['user'], password=CO['ZCSD_DB']['passwd']) as pg_conn:
    #     pg_cur = pg_conn.cursor()
    #     pg_cur.execute(sql_select)
    #     pg_rows = pg_cur.fetchall()
    #
    #     # for r in pg_rows:
    #     #     print(r)

    engine = create_engine(
        "postgresql://%s:%s@%s:%s/%s" % (ZCSD_DB_USER, ZCSD_DB_PASSWD, ZCSD_DB_HOST, ZCSD_DB_PORT, ZCSD_DB_DB))
    df = pd.read_sql_query(sql_select, engine)
    return df


def get_cont_info():
    sql_select = """
        select id, header, label_industry_ids, label_place_ids from app.policy where enabled=1 and id is not null and header is not null
    """

    engine = create_engine(
        "postgresql://%s:%s@%s:%s/%s" % (ZCSD_DB_USER, ZCSD_DB_PASSWD, ZCSD_DB_HOST, ZCSD_DB_PORT, ZCSD_DB_DB))
    df = pd.read_sql_query(sql_select, engine)
    return df


def analyze_log(log_file, mysql_table):
    pg_df = get_user_info()
    cont_df = get_cont_info()
    print(cont_df)
    print("-" * 160)

    log_df = pd.read_csv(log_file, header=None, sep="|", names=["cid", "uid", "pid", "ts", "lat", "lon", "op", "cont"])
    log_df = log_df.dropna(axis=0)
    log_df['cid'] = log_df['cid'].map(lambda x: str(x))
    print(log_df)
    print("-" * 160)

    user_df = log_df.groupby(['uid']).count()
    # print(log_df['pid']['o_y51wX5ZtN7xF8HH5G7VUQxE_rw'])

    # log_df.uid left join pg_df.open_id
    user_dim_df = user_df.join(pg_df.set_index('open_id'), on='uid', how='left', lsuffix='_l', rsuffix='_r')
    print(user_dim_df)
    print("-" * 160)

    page_dim_df = log_df.groupby(['pid']).count()
    print(page_dim_df)
    print("-" * 160)

    log_df = log_df.groupby(['cid']).count()
    cont_dim_df = log_df.join(cont_df.set_index('id'), on='uid', how='left', lsuffix='_l', rsuffix='_r')
    print(cont_dim_df)
    print("-" * 160)


def main():
    log_file = '../_data/yqc_merge_20200716.log'
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
                              charset='utf8')

    DB_COR = DB_CONN.cursor()

    ZCSD_DB_HOST = CO['ZCSD_DB']['host']
    ZCSD_DB_PORT = CO['ZCSD_DB'].as_int('port')
    ZCSD_DB_DB = CO['ZCSD_DB']['db']
    ZCSD_DB_USER = CO['ZCSD_DB']['user']
    ZCSD_DB_PASSWD = CO['ZCSD_DB']['passwd']

    # ZCSD_DB_CONN = psycopg2.connect(host=ZCSD_DB_HOST, port=ZCSD_DB_PORT, database=ZCSD_DB_DB,
    #                                 user=ZCSD_DB_USER, password=ZCSD_DB_PASSWD)
    #
    # ZCSD_DB_COR = DB_CONN.cursor()

    main()
