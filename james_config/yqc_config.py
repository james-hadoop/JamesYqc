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

config_path=os.getcwd()
config = configparser.ConfigParser()
config.read(config_path+'/james_config/yqc_config.ini')

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
