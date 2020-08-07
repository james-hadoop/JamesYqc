# -*- coding: utf-8 -*-
import sys
from datetime import datetime

import configobj
import pymysql
import os

import pandas as pd
from sqlalchemy import create_engine


def sync_policy_elucidation():
    sql_select = """
        SELECT ordinal, enabled, created_time, created_by, updated_time, updated_by, deleted_time, deleted_by, is_deleted, id, code, "name", "header", "content", hits, picture, policy_id, handpick, is_headlines, label_industry_ids, label_place_ids, "source"
FROM app.policy_elucidation
    """

    user_info_df = pd.read_sql(sql_select, pg_engine)

    user_info_df.to_sql('policy_elucidation', zcsd_huawei_mysql_engine, index=False, if_exists="append")


def main():
    sync_policy_elucidation()


if __name__ == '__main__':
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 1000)
    pd.set_option('display.max_rows', None)

    config_path = os.getcwd() + '/../james_config/zcsd.conf'
    CO = configobj.ConfigObj(config_path)

    ZCSD_HUAWEI_MYSQL_HOST = CO['ZCSD_HUAWEI_MYSQL_DB']['host']
    ZCSD_HUAWEI_MYSQL_USER = CO['ZCSD_HUAWEI_MYSQL_DB']['user']
    ZCSD_HUAWEI_MYSQL_PASSWD = CO['ZCSD_HUAWEI_MYSQL_DB']['passwd']
    ZCSD_HUAWEI_MYSQL_DB = CO['ZCSD_HUAWEI_MYSQL_DB']['db']
    ZCSD_HUAWEI_MYSQL_PORT = CO['ZCSD_HUAWEI_MYSQL_DB'].as_int('port')

    zcsd_huawei_mysql_engine = create_engine(
        "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8mb4" % (ZCSD_HUAWEI_MYSQL_USER, ZCSD_HUAWEI_MYSQL_PASSWD, ZCSD_HUAWEI_MYSQL_HOST, ZCSD_HUAWEI_MYSQL_PORT, ZCSD_HUAWEI_MYSQL_DB))


    ZCSD_DB_HOST = CO['ZCSD_DB']['host']
    ZCSD_DB_PORT = CO['ZCSD_DB'].as_int('port')
    ZCSD_DB_DB = CO['ZCSD_DB']['db']
    ZCSD_DB_USER = CO['ZCSD_DB']['user']
    ZCSD_DB_PASSWD = CO['ZCSD_DB']['passwd']

    pg_engine = create_engine(
        "postgresql://%s:%s@%s:%s/%s" % (ZCSD_DB_USER, ZCSD_DB_PASSWD, ZCSD_DB_HOST, ZCSD_DB_PORT, ZCSD_DB_DB))

    main()

    sys.exit(0)
