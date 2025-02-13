import os
import sys
import yaml
import pandas as pd

os.environ['myconf'] = "/Users/xujunhao/PycharmProjects/Learn-Python/my_packages"
sys.path.append(os.getenv('myconf'))
import auto_email

with open("conf.yml", 'r') as f:
    config = yaml.safe_load(f)

# SQL 查询和输出文件
sql_file = config['sql']
excel_file = config['file']

# 邮件配置
Subject = config['email']['subject']
Content = config['email']['content']
Attachment = config['file']
Receiver = config['email']['receiver']
Cc = config['email']['cc']


try:
    # 连接 Hive
    hive_connection = auto_email.connect_to_hive()
    if hive_connection is None:
        raise Exception("无法连接到 Hive 数据库")

    # 执行 SQL
    with open(sql_file, 'r', encoding='UTF-8') as f:
         query = f.read()

    df_result = auto_email.execute_hive_query(hive_connection, query)
    if df_result is None:
        raise Exception("无法执行 Hive 查询")

    # 写入 Excel
    if not os.path.exists('data'):
        os.mkdir('data')
    auto_email.write_to_excel(df_result, excel_file)

    # 发送邮件
    auto_email.email_send(Subject, Content, Attachment, Receiver, Cc=None, Dt=None)

except Exception as e:
    print(f"程序执行出错: {e}")