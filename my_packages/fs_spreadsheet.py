import os
import requests
import yaml
import json
import pandas as pd
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
#from larksuite_oapi import Client, Config, Domain

##--自定义机器人--##
def send_feishu_message(url, text):
    """
    发送飞书消息。

    参数:
        url (str): 飞书 Webhook URL。
        text (str): 发送的文本内容。
    """
    # 请求头
    headers = {
        "Content-Type": "application/json"
    }

    # 请求数据
    payload = {
        "msg_type": "text",
        "content": {
            "text": text
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    return(response)


def get_tenant_access_token(app_id,app_secret):

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "app_id": app_id,
        "app_secret": app_secret
    }

    # 发送 post 请求
    response = requests.post(url, headers=headers, data=json.dumps(data))

    # 检查请求是否成功
    if response.status_code == 200:
        # 解析响应体为 JSON
        response_data = response.json()

        # 打印整个响应
        print("响应数据:", response_data)

        # 提取 tenant_access_token
        if "tenant_access_token" in response_data:
            tenant_access_token = response_data["tenant_access_token"]
            print("获取的 tenant_access_token:", tenant_access_token)
        else:
            print("未找到 tenant_access_token，响应数据可能有问题。")
    else:
        # 请求失败
        print(f"请求失败，状态码: {response.status_code}, 响应: {response.text}")

    return (tenant_access_token)

def get_rows(tenant_access_token,app_token,table_id):
    # 查询工作表信息 行列
    url = "https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/" + str(app_token)+ "/sheets/"+str(table_id)

    headers = {
        "Authorization": "Bearer "+str(tenant_access_token),
        "Content-Type": "application/json"
    }

    # 发送 get 请求
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("获取成功")
        # 解析 JSON 响应
        response_data = response.json()

        # 提取 column_count 和 row_count
        column_count = response_data['data']['sheet']['grid_properties']['column_count']
        row_count = response_data['data']['sheet']['grid_properties']['row_count']

        print(f"Column Count: {column_count}")
        print(f"Row Count: {row_count}")
        return (row_count)
    else:
        print("获取失败:", response.status_code, response.text)
        return None

def delete_rows(tenant_access_token,app_token,table_id,row_n,row_m,batch_size=2000):
    #删除行列
    url = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/"+str(app_token)+"/dimension_range"
    headers = {
        "Authorization": "Bearer "+str(tenant_access_token),
        "Content-Type": "application/json"
    }

    i = 0
    # 从 row_n 到 row_m 的行分批删除
    while row_n < row_m - batch_size * i:
        end_row = min(row_n + batch_size, row_m)  # 每次删除 batch_size 行，防止越界

        data = {
            "dimension": {
                "sheetId": table_id,
                "majorDimension": "ROWS",
                "startIndex": row_n,
                "endIndex": end_row - 1  # endIndex 是排除性的，所以要减去 1
            }
        }

        # 发送 DELETE 请求
        response = requests.delete(url, headers=headers, data=json.dumps(data))

        if response.status_code == 200:
            print(f"删除从第 {row_n} 到 {end_row} 行的数据成功")
        else:
            print(f"删除从第 {row_n} 到 {end_row} 行的数据失败:", response.status_code, response.text)

        # 更新删除范围的起始位置
        row_n = 1  # 继续从下一批次开始删除
        i = i + 1


# 插入数据的函数，添加 batch_size 参数
def insert_data(tenant_access_token, app_token, table_id, cell_range, data, batch_size=2000):
    url = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/" + str(app_token) + "/values"

    headers = {
        "Authorization": "Bearer " + str(tenant_access_token),
        "Content-Type": "application/json"
    }

    # 分批插入数据
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]

        # 构造插入数据的请求体
        value_range = {
            "valueRange": {
                "range": table_id + "!" + str(cell_range),
                "values": batch
            }
        }

        # 发送 PUT 请求
        response = requests.put(url, headers=headers, data=json.dumps(value_range))

        if response.status_code == 200:
            print(f"成功插入第 {i+2} 到 {i+2 + len(batch) - 1} 行的数据")
        else:
            print(f"插入第 {i+2} 到 {i+2 + len(batch) - 1} 行的数据失败:", response.status_code, response.text)


#读取数据
def get_data(tenant_access_token,app_token,table_id,range):

    url = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/" + str(app_token) + "/values/" + str(table_id) + "!" + range

    headers = {
        "Authorization": "Bearer " + str(tenant_access_token),
        "Content-Type": "application/json"
    }

    # 发送 get 请求
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("获取成功")
        # 解析 JSON 响应
        response_data = response.json()

        # 提取 column_count 和 row_count
        data = response_data["data"]["valueRange"]["values"]
        df = pd.DataFrame(data[1:], columns=data[0])
        return df

    else:
        print("获取失败:", response.status_code, response.text)
        return None










