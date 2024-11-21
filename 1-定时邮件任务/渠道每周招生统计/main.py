import os
import sys
import yaml
import pandas as pd

#os.environ['myconf'] = "/Users/xujunhao/PycharmProjects/my_daily_work/my_packages/"
sys.path.append(os.getenv('myconf'))
from conn_db import conn_db
from dd_robot import dd_robot
from feishu_robot import feishu_robot
import auto_email

def fetch_raw(conf):

    sql = conf['sql']
    file = conf['file']

    sql = open(sql, 'r', encoding = 'UTF-8').read()
    val, col = conn_db(sql)
    data = pd.DataFrame(val, columns = col)

    if not os.path.exists('data'):
        os.mkdir('data')

    data.to_excel(file, index=False)

def send_data(conf):

    email_conf = conf['email']

    Subject  = email_conf['subject']
    Content  = email_conf.get('content')
    Attachment = conf['file']
    Recever  = email_conf['receiver']
    Cc   = email_conf.get('cc')

    auto_email.email_send(Subject, Content, Attachment, Recever, Cc)

with open('conf.yml', 'r', encoding ='utf-8') as f:
    conf = yaml.full_load(f)

try:
    fetch_raw(conf)
    send_data(conf)
except Exception as e:
    print(e)
    robot_conf = conf['feishu_robot']
    message    = '{}-发送失败'.format(conf['sql'].split('.')[0])
    feishu_robot(robot_conf['webhook'], message, 'text')
    # raise