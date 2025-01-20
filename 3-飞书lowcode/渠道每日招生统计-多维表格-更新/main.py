import os
import sys
import yaml
import datetime as dt
import lark_oapi as lark
import pandas as pd

#os.environ['myconf'] = "/Users/xujunhao/PycharmProjects/Learn-Python/my_packages" #设置一个自定义模块路径，并通过环境变量存储路径
sys.path.append(os.getenv('myconf')) #动态将该路径添加到 Python 的模块搜索路径中
import insert_db
from fs_multitable import *

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
app_token = "EgZjbgkgpaZo0WsIrvpcG8p6n9f"
table_id = "tblcvVFKjvaVFdiM"
# new_data = [
#     {"单向关联": "recHTLvO7x,recbS8zb2m"}
# ] * 100  # 示例：插入 1000 条数据
new_data = pd.read_feather(file)
dat_dict = new_data.fillna('null').to_dict(orient="records")

try :

    client = create_client(app_id, app_secret)

    # Step 1: 查询表中所有记录 ID
    record_ids = query_all_record_ids(client, app_token, table_id)

    # Step 2: 删除表中所有记录
    delete_records(client, app_token, table_id, record_ids)

    # Step 3: 插入新数据
    print("开始插入数据时间", dt.datetime.now())
    insert_records(client, app_token, table_id, dat_dict)

except Exception as e:
    print(e)
    message = '{}-发送失败'.format(sql.split('.')[0])
    send_feishu_message(webhook_url, message)