import os
import pandas as pd
from pyhive import hive
import mysql.connector
import yaml
import pyarrow.feather as feather
import numpy as np

conf_path = os.getenv('myconf')
conf = yaml.full_load(open(conf_path + '/my_conf.yml', encoding='utf-8').read())

def connect_to_hive():
    """连接到 Hive 数据库。"""
    try:
        hive_conn = hive.Connection(host=conf['hive']['host'],
                             port=conf['hive']['port'],
                             username=conf['hive']['username'],
                             password=conf['hive']['password'],
                             auth='LDAP')
        hive_cursor = hive_conn.cursor()
        print("成功连接到 Hive")
        return  hive_cursor
    except Exception as e:
        print(f"连接 Hive 失败: {e}")
        exit(1)

def execute_hive_query(hive_cursor, query):
    """执行 Hive 查询。"""
    try:
        with open(query, 'r', encoding='utf-8') as sql_file:
            hive_query = sql_file.read()
        hive_cursor.execute(hive_query)
        results = hive_cursor.fetchall()
        columns = [col[0].split('.')[-1] if '.' in col[0] else col[0] for col in hive_cursor.description]
        df = pd.DataFrame(results, columns=columns)
        print("成功执行 Hive 查询")
        hive_cursor.close()
        hive_cursor.close()
        return df
    except Exception as e:
        print(f"执行 Hive 查询失败: {e}")
        exit(1)

def save_to_feather(df, file):
    """将 DataFrame 保存到 Feather 文件。"""
    try:
        feather.write_feather(df, file)
        print("成功将数据保存到 Feather 文件")
    except Exception as e:
        print(f"保存到 Feather 文件失败: {e}")
        exit(1)

def connect_to_mysql():
    """连接到 MySQL 数据库。"""
    try:
        mysql_config = conf['mysql_superset']
        mysql_conn = mysql.connector.connect(host=mysql_config['host'],
                                         user=mysql_config['username'],
                                         password=mysql_config['password'],
                                         database=mysql_config['database'])
        mysql_cursor = mysql_conn.cursor()
        print("成功连接到 MySQL")
        return mysql_conn,mysql_cursor
    except Exception as e:
        print(f"连接 MySQL 失败: {e}")
        exit(1)


def create_or_truncate_mysql_table(mysql_cursor, mysql_table, col_comment):  # 不需要 df 参数了
    """根据配置文件创建或清空 MySQL 表，并添加注释。"""
    try:

        # 1. 删除表（如果存在）
        drop_table_query = f"DROP TABLE IF EXISTS {mysql_table}"
        mysql_cursor.execute(drop_table_query)
        print(f"已删除（如果存在）MySQL 表 {mysql_table}")

        # 2. 创建新表
        columns_str = []
        for col, col_config in col_comment.items(): # 直接迭代字典键值对更简洁
            col_type = col_config.split(';')[0]
            col_comment = col_config.split(';')[1] if ';' in col_config else '' # 更简洁地处理注释
            columns_str.append(f"`{col}` {col_type} COMMENT '{col_comment}'")

        columns_str = ', '.join(columns_str)
        create_table_query = f"""
        CREATE TABLE {mysql_table} (
            {columns_str}
        )
        """
        mysql_cursor.execute(create_table_query)
        print(f"已创建 MySQL 表 {mysql_table}")

    except Exception as e:
        print(f"创建或清空 MySQL 表失败: {e}")
        exit(1)

def to_sql_values(row):
    return "(" + ", ".join(
        [f"NULL" if pd.isna(val) else f"'{val}'" if isinstance(val, str) else str(val) for val in row]
    ) + ")"

def import_to_mysql(mysql_conn, mysql_cursor, mysql_table, feather_filepath):
    """将数据从 Feather 文件导入到 MySQL 表中。"""
    try:
        df = feather.read_feather(feather_filepath)

        # 将 DataFrame 中的 None 或 NaN 替换为 SQL 中的 NULL
        df = df.where(pd.notnull(df), None)

        # 使用 to_sql_values 函数构建 VALUES 部分
        values_sql = ', '.join(map(lambda x: to_sql_values(x), df.values))  # 使用 map 和 to_sql_values

        # 用反引号包裹列名
        columns_str = ', '.join(f"`{col}`" for col in df.columns)

        # 构建完整的 INSERT 语句
        insert_query = f"INSERT INTO {mysql_table} ({columns_str}) VALUES {values_sql}"

        # 使用 conn_db 函数执行 INSERT 语句，并处理错误
        try:
            mysql_cursor.execute(insert_query)
            mysql_conn.commit()
            mysql_cursor.close()
            mysql_conn.close()
            print(f"成功将数据导入到 MySQL 表 {mysql_table}")

        except Exception as e:
            print(f"SQL 执行出错: {e}")
            print("SQL 查询语句:", insert_query[:1000])  # 打印出错的 SQL 语句
            df.to_excel('wrong_sql_data.xlsx')  # 保存出错的数据
            mysql_conn.rollback()  # 回滚事务
            raise  # 重新引发异常

    except Exception as e:
        print(f"导入数据到 MySQL 失败: {e}")
        exit(1)