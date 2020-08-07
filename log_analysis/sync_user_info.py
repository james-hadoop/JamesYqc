# -*- coding: utf-8 -*-
import sys
from datetime import datetime

import configobj
import pymysql
import os

import pandas as pd
from sqlalchemy import create_engine


def distract_user_info_from_pg_to_mysql():
    sql_select = """
        select open_id,nickname,phone,age,sex,duty from app.sys_user where enabled=1 and is_deleted=0 and open_id is not null and length(open_id)>1 order by created_time desc
    """

    user_info_df = pd.read_sql(sql_select, pg_engine)
    print(user_info_df)
    user_info_df['nickname'] = user_info_df['nickname'].map(lambda x: str(x).encode("utf8"))
    print(user_info_df)
    user_info_df.to_sql('sys_user', zcsd_huawei_mysql_engine, index=False, if_exists="replace")


def main():
    distract_user_info_from_pg_to_mysql()


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
