import os
import requests
import yaml
import json
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *

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
    # try:
    #     # 发送 POST 请求
    #     response = requests.post(url, headers=headers, data=json.dumps(payload))
    #
    #     # 检查响应状态
    #     if response.status_code == 200:
    #         print("消息发送成功！")
    #     else:
    #         print(f"消息发送失败，状态码: {response.status_code}, 响应: {response.text}")
    # except Exception as e:
    #     print(f"发送消息时出错: {e}")

# 创建 client
def create_client(app_id, app_secret):

    client = lark.Client.builder() \
        .app_id(app_id) \
        .app_secret(app_secret) \
        .build()

    return client

##--多维表格--##
# 查询表中所有记录的 ID（支持分页）
def query_all_record_ids(client, app_token, table_id):
    record_ids = []
    page_token = ''  # 初始分页标记为空

    while True:
        try:
            # 构造请求对象
            request: SearchAppTableRecordRequest = SearchAppTableRecordRequest.builder() \
                .app_token(app_token) \
                .table_id(table_id) \
                .page_token(page_token) \
                .page_size(500) \
                .request_body(SearchAppTableRecordRequestBody.builder()
                              .automatic_fields(True)
                              .build()) \
                .build()

            # 发起请求
            response: SearchAppTableRecordResponse = client.bitable.v1.app_table_record.search(request)

            # 检查请求是否成功
            if not response.success():
                lark.logger.error(
                    f"Failed to query records, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
                break

            # 提取当前页记录 ID
            record_ids.extend([item.record_id for item in response.data.items])

            # 更新分页标记
            page_token = response.data.page_token
            if not page_token:  # 如果没有更多数据，退出循环
                break

        except Exception as e:
            lark.logger.error(f"An error occurred during query: {e}")
            break

    return record_ids

##--多维表格--##
# 删除表中所有记录（分批处理，每次最多 500 条）
def delete_records(client, app_token, table_id, record_ids):
    batch_size = 500
    for i in range(0, len(record_ids), batch_size):
        batch = record_ids[i:i + batch_size]  # 取当前批次的记录 ID

        request: BatchDeleteAppTableRecordRequest = BatchDeleteAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(BatchDeleteAppTableRecordRequestBody.builder()
                          .records(batch)
                          .build()) \
            .build()

        response: BatchDeleteAppTableRecordResponse = client.bitable.v1.app_table_record.batch_delete(request)

        if not response.success():
            lark.logger.error(
                f"Failed to delete records, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        else:
            lark.logger.info(f"Deleted {len(batch)} records successfully.")


# 插入新数据到表格（分批处理，每次最多 500 条）
# def insert_records(client, app_token, table_id, data):
#     batch_size = 500
#     for i in range(0, len(data), batch_size):
#         batch = data[i:i + batch_size]  # 取当前批次的数据
#
#         records = [AppTableRecord.builder().fields(fields).build() for fields in batch]
#
#         request: BatchCreateAppTableRecordRequest = BatchCreateAppTableRecordRequest.builder() \
#             .app_token(app_token) \
#             .table_id(table_id) \
#             .request_body(BatchCreateAppTableRecordRequestBody.builder()
#                           .records(records)
#                           .build()) \
#             .build()
#
#         response: BatchCreateAppTableRecordResponse = client.bitable.v1.app_table_record.batch_create(request)
#
#         if not response.success():
#             lark.logger.error(
#                 f"Failed to insert records, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
#         else:
#             lark.logger.info(f"Inserted {len(batch)} records successfully.")
##--多维表格--##
#插入多条记录
def insert_records(client, app_token, table_id, data):
    try:
        batch_size = 500
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]  # 当前批次数据

            records = [AppTableRecord.builder().fields(fields).build() for fields in batch]

            request: BatchCreateAppTableRecordRequest = BatchCreateAppTableRecordRequest.builder() \
                .app_token(app_token) \
                .table_id(table_id) \
                .request_body(BatchCreateAppTableRecordRequestBody.builder()
                              .records(records)
                              .build()) \
                .build()

            response: BatchCreateAppTableRecordResponse = client.bitable.v1.app_table_record.batch_create(request)

            if not response.success():
                raise Exception(
                    f"Failed to insert records, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, "
                    f"response: {json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}"
                )
            else:
                lark.logger.info(f"Inserted {len(batch)} records successfully.")
    except Exception as e:
        lark.logger.error(f"An error occurred during record insertion: {e}")
        raise  # 重新抛出异常供外部捕获