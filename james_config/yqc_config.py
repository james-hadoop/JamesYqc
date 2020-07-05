import os
import configparser

global g
g = {}

os_info = os.uname()

config_module = ''
if os_info.sysname == 'Darwin':
    config_module = 'os_mac'
else:
    config_module = 'os_ubuntu'

config_path = os.getcwd()
config = configparser.ConfigParser()
config.read(config_path + '/james_config/yqc_config.ini')

# for key in config['os_ubuntu']:
#     print(key)

g['excel_dir'] = config[config_module]['excel_dir']
g['chrome_driver_dir'] = config[config_module]['chrome_driver_dir']
g['scrapy_dir'] = config[config_module]['scrapy_dir']
g['log_dir'] = config['log']['log_dir']

g['db_host'] = config['db']['db_host']
g['db_port'] = config['db']['db_port']
g['db_db'] = config['db']['db_db']
g['db_user'] = config['db']['db_user']
g['db_passwd'] = config['db']['db_passwd']

g_city_map = {'北京': '北京市政府网站', '重庆': '重庆市政府网站', '海口': '海口市政府网站',
              '宁波': '宁波市政府网站', '上海': '上海市政府网站', '广州': '广州市政府网站', '南宁': '南宁市政府网站', '长沙': '长沙市政府网站',
              '南昌': '南昌市政府网站', '郑州': '郑州市政府网站', '石家庄': '石家庄市政府网站', '哈尔滨': '哈尔滨市政府网站', '深圳': '深圳市政府网站', '苏州': '苏州市政府网站',
              '厦门': '厦门市政府网站', '长春': '长春市政府网站', '沈阳': '沈阳市政府网站', '昆明': '昆明市政府网站'}
