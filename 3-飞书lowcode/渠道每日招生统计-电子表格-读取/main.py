import os
import sys
import yaml

#os.environ['myconf'] = "/Users/xujunhao/PycharmProjects/Learn-Python/my_packages" #设置一个自定义模块路径，并通过环境变量存储路径
sys.path.append(os.getenv('myconf')) #动态将该路径添加到 Python 的模块搜索路径中
import insert_db
from fs_spreadsheet import *

conf = yaml.full_load(open('conf.yml', 'r', encoding='utf-8'))
app_id = conf['feishu_app']['app_id']
app_secret = conf['feishu_app']['app_secret']
webhook_url=conf['feishu_robot']['webhook']

# 配置参数
app_token = "CkDBsZzdYhXtPJtOTxDcR6yynSd"
table_id = "1aa0ad"

#获取tenant_access_token
tenant_access_token = get_tenant_access_token(app_id,app_secret)

range="A1:B10"

data=get_data(tenant_access_token,app_token,table_id,range)

#print(data)