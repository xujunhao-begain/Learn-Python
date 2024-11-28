import os
import time
import sys
import pandas as pd
from pyhive import hive
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.header import Header
from email import encoders
import yaml
import openpyxl

conf_path = os.getenv('myconf')
conf = yaml.full_load(open(conf_path + '/my_conf.yml', encoding='utf-8').read())

# 1. 连接 Hive 数据库
def connect_to_hive():

    try:
        connection = hive.Connection(host=conf['hive']['host'],
                                     port=conf['hive']['port'],
                                     username=conf['hive']['username'],
                                     password=conf['hive']['password'],
                                     auth='LDAP'
                                     )
        return connection
    except Exception as e:
        print(f"连接 Hive 失败: {e}")
        return None


# 2. 执行 SQL 查询
def execute_hive_query(connection, query):
    """执行 Hive SQL 查询。"""
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        return df
    except Exception as e:
        print(f"执行 Hive 查询失败: {e}")
        return None


# 3. 将结果写入 Excel 文件
def write_to_excel(df, excel_file):
    """将 DataFrame 写入 Excel 文件。"""
    try:
        df.to_excel(excel_file, index=False, engine='openpyxl')  # 使用 openpyxl 引擎
        print(f"数据已成功写入 {excel_file}")
    except Exception as e:
        print(f"写入 Excel 文件失败: {e}")


# 4. 读取 Excel 文件 (如果需要修改后再发送)
def read_from_excel(excel_file):
    """从 Excel 文件读取数据。"""
    try:
        df = pd.read_excel(excel_file)
        return df
    except Exception as e:
        print(f"读取 Excel 文件失败: {e}")
        return None



# 5. 发送到指定邮箱
def email_send(Subject, Content, Attachment, Receiver, Cc=None, Dt=None):

    smtp_server = conf['mail']['server']
    port = conf['mail']['port']
    sender = conf['mail']['sender']
    password = conf['mail']['password']

    Subject = Subject
    content = Content
    attachment = Attachment
    receiver = Receiver
    cc = Cc

    if not Dt:
        dt = time.strftime('_%m%d', time.localtime(time.time()))
        res = Attachment.split('/')[-1].split('.')
        attachname = res[0] + dt + '.' + res[-1]
    else:
        res = Attachment.split('/')[-1].split('.')
        attachname = res[0] + '.' + res[-1]

    server = smtplib.SMTP_SSL(host=smtp_server)
    server.connect(smtp_server, port)
    server.login(sender, password)

    msg = MIMEMultipart()

    # 判断receiver, cc是字符串还是list
    if isinstance(receiver, list):
        receiver_str = ';'.join(receiver)
    else:
        receiver_str = receiver
        receiver = receiver.split(';')

    if isinstance(cc, list):
        cc_str = ';'.join(cc)
    elif cc is not None:
        cc_str = cc
        cc = cc.split(';')
    else:
        cc_str = cc

    # 邮件头信息
    msg['From'] = Header(sender)
    msg['To'] = Header(receiver_str)
    msg['cc'] = Header(cc_str)
    msg['Subject'] = Header(Subject)

    # 邮箱正文内容，第一个参数为内容，第二个参数为格式(plain 为纯文本)，第三个参数为编码
    msg.attach(MIMEText(content, 'plain', 'utf-8'))

    # print('--> 准备添加附件')
    # 构造附件1
    file = MIMEApplication(open(attachment, 'rb').read())
    file.add_header('Content-Disposition', 'attachment', filename='{attachname}'.format(attachname=attachname))
    msg.attach(file)

    if cc is not None:
        receiver.extend(cc)

    server.sendmail(sender, receiver, str(msg))
    server.quit()
    print(f':::: {attachname} :::: 发送完毕 ::::')