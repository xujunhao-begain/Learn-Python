import os
import sys
import yaml
import datetime as dt

os.environ['myconf'] = "/Users/xujunhao/PycharmProjects/Learn-Python/my_packages" #设置一个自定义模块路径，并通过环境变量存储路径
sys.path.append(os.getenv('myconf')) #动态将该路径添加到 Python 的模块搜索路径中
import insert_db
from fs_multitable import *

conf = yaml.full_load(open('conf.yml', 'r', encoding='utf-8'))
app_id = conf['feishu_app']['app_id']
app_secret = conf['feishu_app']['app_secret']
webhook_url=conf['feishu_robot']['webhook']
file = conf['file']
table = conf['table']
comment = conf['comment']
col_comment = conf['col_comment']
table_index = conf['table_index']
app_token = "EgZjbgkgpaZo0WsIrvpcG8p6n9f"
table_id = "tblcvVFKjvaVFdiM"

client = create_client(app_id, app_secret)

print("开始读取数据时间:", dt.datetime.now())
df = get_records(client, app_token, table_id)

#创建data文件夹
data_path = file.split('/')[0]
if not os.path.exists(data_path):
    os.mkdir(data_path)

#写入到feather文件
insert_db.save_to_feather(df,file)

# 连接mysql
mysql_conn, mysql_cursor = insert_db.connect_to_mysql()

# 创建表
insert_db.create_or_truncate_mysql_table(mysql_cursor, table, col_comment)

# 导入数据
print('import_to_mysql_start_time：', dt.datetime.now())
insert_db.import_to_mysql(mysql_conn, mysql_cursor, table, file)
print('import_to_mysql_end_time：', dt.datetime.now())
