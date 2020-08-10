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
        SELECT t_log_u.*, header, label_industry_ids, label_place_ids FROM ( SELECT t_log.*, nickname, phone, age, sex , duty FROM ( SELECT uid, cid, pid, timestamp(ts) AS ts, ts_str , lat_str, lon_str, op, COUNT(op) AS cnt FROM t_zcsd_user_log_detail GROUP BY uid, cid, pid, ts, ts_str, lat_str, lon_str, op ) t_log LEFT JOIN ( SELECT open_id, nickname, phone, age, sex , duty FROM sys_user ) t_u ON t_log.uid = t_u.open_id ) t_log_u LEFT JOIN ( SELECT id, header, label_industry_ids, label_place_ids FROM policy WHERE (enabled = 1 AND id IS NOT NULL AND header IS NOT NULL) ) t_c ON t_log_u.cid = t_c.id
        """

    log_df = pd.read_sql(sql_select, zcsd_huawei_mysql_engine)
    print(log_df.head(5))

    log_df.to_sql('t_zcsd_user_behavior_summay', zcsd_huawei_mysql_engine, index=False, if_exists="replace")


def main():
    generate_user_behavior_summary()


if __name__ == '__main__':
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 1000)
    pd.set_option('display.max_rows', None)

    config_path = os.getcwd() + '/james_config/zcsd.conf'
    CO = configobj.ConfigObj(config_path)

    ZCSD_HUAWEI_MYSQL_HOST = CO['ZCSD_HUAWEI_MYSQL_DB']['host']
    ZCSD_HUAWEI_MYSQL_USER = CO['ZCSD_HUAWEI_MYSQL_DB']['user']
    ZCSD_HUAWEI_MYSQL_PASSWD = CO['ZCSD_HUAWEI_MYSQL_DB']['passwd']
    ZCSD_HUAWEI_MYSQL_DB = CO['ZCSD_HUAWEI_MYSQL_DB']['db']
    ZCSD_HUAWEI_MYSQL_PORT = CO['ZCSD_HUAWEI_MYSQL_DB'].as_int('port')

    zcsd_huawei_mysql_engine = create_engine(
        "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8mb4" % (ZCSD_HUAWEI_MYSQL_USER, ZCSD_HUAWEI_MYSQL_PASSWD, ZCSD_HUAWEI_MYSQL_HOST, ZCSD_HUAWEI_MYSQL_PORT, ZCSD_HUAWEI_MYSQL_DB))

    main()

    sys.exit(0)
