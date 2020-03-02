"""
客户端启动测试
"""
import logging
import sys
from threading import Thread

from mingmq.client import Client

logging.basicConfig(level=logging.DEBUG)

HTML = ''
with open('./test.html', encoding='utf-8') as file_desc:
    HTML = file_desc.read()


def init_cli(first=False):
    """
    客户端测试
    """
    client = Client('localhost', 8080)

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


def main(tsn=10):
    clis = []

    for i in range(tsn):
        if i == 0:
            clis.append(init_cli(True))
        else:
            clis.append(init_cli())

    ts = []
    for cli in clis:
        t = Thread(target=send, args=(cli,))
        t.start()
        ts.append(t)

    for t in ts: t.join()

    ts = []
    for cli in clis:
        t = Thread(target=get, args=(cli,))
        t.start()
        ts.append(t)

    for t in ts: t.join()

    for cli in clis:
        close(cli)


main(100)
