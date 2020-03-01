"""
客户端启动测试
"""
import logging
import sys
import time
from threading import Thread, active_count

from mingmq.client import Client

logging.basicConfig(level=logging.DEBUG)


def main():
    """
    客户端测试
    """
    client = Client('127.0.0.1', 8080)

    if client.login('admin', '123456') is not True:
        sys.exit(-1)

    print('登录成功')

    client.declare_queue('hello')

    print('声明队列成功')

    html = ''
    with open('./test.html', encoding='utf-8') as file_desc:
        html = file_desc.read()
    client.send_data_to_queue('hello', html)

    print('发送任务成功')

    message_data = client.get_data_from_queue('hello')

    print('获取任务成功', message_data)

    client.ack_message('hello', message_data[0]['message_id'])

    print('消息确认成功')

    client.logout('admin', '123456')

    client.close()


def bingfa():
    """
    并发测试
    """
    # for i in range(100):
    while 1:
        if active_count() <= 100:
            Thread(target=main).start()
        else:
            time.sleep(2)


# for i in range(100):
#     main()
bingfa()
