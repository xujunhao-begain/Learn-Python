import os
import sys
import yaml
import pandas as pd
import datetime as dt
import pyarrow.feather as feather

#os.environ['myconf'] = "/Users/xujunhao/PycharmProjects/Learn-Python/my_packages"
sys.path.append(os.getenv('myconf'))
import insert_db

with open("conf.yml", 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

    sql = config['sql']
    file = config['file']
    table = config['table']
    comment = config['comment']
    col_comment = config['col_comment']
    table_index = config['table_index']

    #连接hive
    hive_cursor = insert_db.connect_to_hive()

    #创建data文件夹
    data_path = file.split('/')[0]
    if not os.path.exists(data_path):
        os.mkdir(data_path)

    #执行sql
    print('execute_hive_query_start_time：', dt.datetime.now())
    df = insert_db.execute_hive_query(hive_cursor,sql)
    print('execute_hive_query_end_time：', dt.datetime.now())

    #保存为.feather文件
    insert_db.save_to_feather(df,file)

    #df = feather.read_feather(file)

    #连接mysql
    mysql_conn,mysql_cursor = insert_db.connect_to_mysql()

    #创建表
    insert_db.create_or_truncate_mysql_table(mysql_cursor, table, col_comment)

    #导入数据
    print('import_to_mysql_start_time：', dt.datetime.now())
    insert_db.import_to_mysql(mysql_conn, mysql_cursor, table, file)
    print('import_to_mysql_end_time：', dt.datetime.now())

