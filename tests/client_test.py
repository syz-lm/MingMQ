"""
客户端启动测试
"""
import logging
import sys
import time
from threading import Thread, active_count

from mingmq.client import Client

logging.basicConfig(level=logging.DEBUG)

HTML = ''
with open('./test.html', encoding='utf-8') as file_desc:
    HTML = file_desc.read()

def init_cli(first=False):
    """
    客户端测试
    """
    client = Client('192.168.1.30', 8080)

    if client.login('admin', '123456') is not True:
        sys.exit(-1)

    # print('登录成功')
    if first:
        client.declare_queue('hello')

    return client

def send(client):
    client.send_data_to_queue('hello', HTML)

def get(client):
    # print('发送任务成功')

    message_data = client.get_data_from_queue('hello')

    print('获取任务成功', message_data)

    client.ack_message('hello', message_data[0]['message_id'])

    print('消息确认成功')

def close(client):
    client.logout('admin', '123456')
    client.close()

def main():
    clis = []

    for i in range(5000):
        if i == 0:
            clis.append(init_cli(True))
        else:
            clis.append(init_cli())

    i = 0
    while i < len(clis):
        if active_count() <= 500:
            send(clis[i])
            i += 1
        else:
            time.sleep(2)

    i = 0
    while i < len(clis):
        if active_count() <= 500:
            get(clis[i])
            i += 1
        else:
            time.sleep(2)

main()