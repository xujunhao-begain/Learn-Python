import os
import sys
import yaml
import pandas as pd
import datetime as dt

#os.environ['myconf'] = "/Users/xujunhao/PycharmProjects/my_daily_work/my_packages"
sys.path.append(os.getenv('myconf'))
from conn_db import conn_db
from data_to_db import insert_into_db


def fetch_hue_data(sql, file):
    sql = open(sql, 'r', encoding='utf-8').read().replace('${', '{')
    values, columns = conn_db(sql)
    data = pd.DataFrame(values, columns=columns)
    data.to_feather(file)


if __name__ == '__main__':

    with open('conf.yml', 'r', encoding='utf-8') as f:
        conf = yaml.full_load(f.read())
    sql = conf['sql']
    file = conf['file']
    table = conf['table']
    comment = conf['comment']
    col_comment = conf['col_comment']
    table_index = conf['table_index']

    data_path = file.split('/')[0]
    if not os.path.exists(data_path):
        os.mkdir(data_path)

    now = dt.datetime.now()
    print('start_time：', now)

    fetch_hue_data(sql, file)
    print('fetch end_time：', now)
    insert_into_db(file, table=table, key='mysql_superset', table_comment=comment, col_comment=col_comment,
                   normal_index=table_index)
    now = dt.datetime.now()
    print('end_time：', now)
