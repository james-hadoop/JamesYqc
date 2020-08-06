# -*- coding: utf-8 -*-
import sys
from datetime import datetime

import configobj
import pymysql
import os

import pandas as pd
from sqlalchemy import create_engine


def generate_user_behavior_summary():
    sql_select = """
        SELECT t_log.*, nickname, phone, age, sex , duty FROM ( SELECT uid, cid, pid, ts, ts_str, lat_str, lon_str, op, COUNT(op) AS cnt FROM zcsd.t_zcsd_user_log_detail GROUP BY uid, cid, pid, ts, ts_str, lat_str, lon_str, op ) t_log LEFT JOIN ( SELECT open_id, nickname, phone, age, sex , duty FROM zcsd.t_zcsd_user_info WHERE length(open_id) > 1 ) t_u ON t_log.uid = t_u.open_id;
        """

    log_df = pd.read_sql(sql_select, my_engine)
    log_df.to_sql('t_zcsd_user_behavior_summay', zcsd_huawei_mysql_engine, index=False, if_exists="replace")


def main():
    generate_user_behavior_summary()


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
