import os
import sys
import yaml
import datetime as dt
import pandas as pd

#os.environ['myconf'] = "/Users/xujunhao/PycharmProjects/Learn-Python/my_packages" #设置一个自定义模块路径，并通过环境变量存储路径
sys.path.append(os.getenv('myconf')) #动态将该路径添加到 Python 的模块搜索路径中
import insert_db
from fs_spreadsheet import *

conf = yaml.full_load(open('conf.yml', 'r', encoding='utf-8'))
app_id = conf['feishu_app']['app_id']
app_secret = conf['feishu_app']['app_secret']
webhook_url=conf['feishu_robot']['webhook']
sql = conf['sql']
file = conf['file']

#连接hive
hive_cursor = insert_db.connect_to_hive()

#执行sql
print('execute_hive_query_start_time：', dt.datetime.now())
df = insert_db.execute_hive_query(hive_cursor,sql)
print('execute_hive_query_end_time：', dt.datetime.now())

#创建data文件夹
data_path = file.split('/')[0]
if not os.path.exists(data_path):
    os.mkdir(data_path)

#保存为.feather文件
insert_db.save_to_feather(df,file)

# 配置参数
app_token = "CkDBsZzdYhXtPJtOTxDcR6yynSd"
table_id = "1aa0ad"

# new_data =[
#             [
#                 "Hello", 13
#             ],
#             [
#                 "World", 14
#             ]
# ] * 1500
new_data = pd.read_feather(file)
dat_dict = new_data.fillna('null').values.tolist()

#获取tenant_access_token
tenant_access_token = get_tenant_access_token(app_id,app_secret)

#获取表格行数
row_count = get_rows(tenant_access_token,app_token,table_id)

#删除表格记录(保留第1行）
delete_rows(tenant_access_token,app_token,table_id,2,row_count)

cell_range="A2:B"+str(len(new_data)+1)
insert_data(tenant_access_token,app_token,table_id,cell_range,dat_dict)