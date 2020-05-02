from james_config.yqc_config import g
import logging as log
import datetime
import os

from sqlalchemy import create_engine
import pandas as pd

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def init_log_config():
    log_dir = g['log_dir']
    log_file = os.path.basename(__file__).split('.')[0]
    log_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_full_path = log_dir + log_file + log_time + ".log"

    print("log_full_path=%s" % log_full_path)
    log.basicConfig(filename=log_full_path,
                    level=log.INFO, format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)8s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

    return log_full_path


def fetch_yqc_spider_results(db_host, db_port, db_db, db_user, db_passwd, csv_file_path, work_date):
    sql_select = f"""
SELECT *
FROM
  (SELECT t_cont.*
   FROM
     (SELECT min(id) id,
             title
      FROM developer.yqc_spider
      GROUP BY title) t_id
   LEFT JOIN
     (SELECT id,
             title,
             url,
             pub_time,
             pub_org,
             doc_id,
             index_id,
             key_cnt,
             region,
             update_time
      FROM developer.yqc_spider
      WHERE update_time>'%(work_date)s 00:00:00') t_cont ON t_id.id=t_cont.id) tt
WHERE title IS NOT NULL
ORDER BY region, key_cnt DESC;""" % {'work_date': work_date}
    log.info(f"sql_select={sql_select}")

    engine = create_engine("mysql+pymysql://%s:%s@%s:%s/%s" % (db_user, db_passwd, db_host, db_port, db_db))

    df = pd.read_sql_query(sql_select, engine)
    df = df.applymap(lambda x: str(x).strip())

    log.info(df)

    df.to_csv(csv_file_path)
    log.info("csv file has been saved.")


def get_now(pattern="%Y-%m-%d"):
    now = datetime.datetime.now()
    return now.strftime(pattern)


def send_email_with_excel(csv_file_path, work_date):
    mail_host = "smtp.163.com"
    mail_user = "jamesqjiang"
    mail_pass = "jamesqjiang"

    sender = 'jamesqjiang@163.com'
    receivers = ['sysinfo@yuanqucha.com']
    title = '园区政策信息_' + work_date

    msg = MIMEMultipart()
    msg.attach(MIMEText('尊敬的先生/女士：  请查收最新的园区政策信息，感谢您的订阅！'))
    msg['Subject'] = title  # subject
    msg['From'] = 'jamesqjiang@163.com'
    msg['To'] = 'sysinfo@yuanqucha.com'

    xlsx = MIMEText(open(csv_file_path, 'rb').read(), 'base64', 'gb2312')
    xlsx["Content-Type"] = 'application/octet-stream'
    xlsx.add_header('Content-Disposition', 'attachment', filename=csv_file_path)
    msg.attach(xlsx)

    smtpObj = smtplib.SMTP_SSL(mail_host, 465)  # 启用SSL发信, 端口一般是465
    smtpObj.login(mail_user, mail_pass)  # 登录验证
    smtpObj.sendmail(sender, receivers, msg.as_string())  # 发送
    log.info(f"{csv_file_path} 发送成功!")


def main():
    # 1. 获取配置信息，配置日志打点
    log_full_path = init_log_config()

    excel_dir = g['excel_dir']
    chrome_driver_dir = g['chrome_driver_dir']
    scrapy_dir = g['scrapy_dir']
    db_host = g['db_host']
    db_port = int(g['db_port'])
    db_db = g['db_db']
    db_user = g['db_user']
    db_passwd = g['db_passwd']

    log.info(">>> 1. 获取配置信息，配置打点日志:")
    log.info(f"log_full_path={log_full_path}")
    log.info(f"excel_dir={excel_dir}")
    log.info(f"chrome_driver_dir={chrome_driver_dir}")
    log.info(f"scrapy_dir={scrapy_dir}")
    log.info(f"db_host={db_host}")
    log.info(f"db_port={db_port}")
    log.info(f"db_db={db_db}")

    # # 2. 调用sehll，执行数据爬取
    # log.info(">>> 2. 调用sehll，执行数据爬取:")
    # shell_cmd = "cd " + scrapy_dir + " && sh start_spider.sh"
    #
    # log.info(f"shell_cmd={shell_cmd}")
    # shell_ret = os.system(shell_cmd)
    # log.info(f"shell_ret={shell_ret}")

    # 3. 将MySQL中的数据导出到csv文件
    log.info(">>> 3. 将MySQL中的数据导出到csv文件:")
    work_date = get_now()
    csv_file_path = excel_dir + "yqc_spider_" + work_date + ".csv"
    log.info(f"csv_file_path={csv_file_path}")
    fetch_yqc_spider_results(db_host, db_port, db_db, db_user, db_passwd, csv_file_path, work_date)

    # 4. 发送邮件
    log.info(">>> 4. 发送邮件:")
    send_email_with_excel(csv_file_path, work_date)


if __name__ == '__main__':
    main()
