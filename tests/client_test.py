"""
客户端启动测试
"""
import logging, time
from threading import Thread, active_count

from mingmq.client import Client

logging.basicConfig(level=logging.ERROR)

# HTML = ''
# with open('./test.html', encoding='utf-8') as file_desc:
#     HTML = file_desc.read()
#

def init_cli(first, queue_name):
    """
    客户端测试
    """
    client = Client('192.168.1.30', 15673)

    client.login('mingmq', 'mm5201314')

    print('登录成功')
    if first:
        client.declare_queue(queue_name)

    return client


def send(client, queue_name, data):
    client.send_data_to_queue(queue_name, data)
    print('发送任务成功')


def get(client, queue_name):
    message_data = client.get_data_from_queue(queue_name)

    print('获取任务成功', message_data)

    json_obj = message_data['json_obj']
    message_id = None
    if len(json_obj) != 0:
        message_id = json_obj[0]['message_id']

    if message_id:
        print('消息确认成功', client.ack_message(queue_name, message_id))


def close(client):
    client.logout('mingmq', 'mm5201314')
    print('退出成功')
    client.close()
    print('关闭成功')


def main(tsn, queue_name, data):
    clis = []

    for i in range(tsn):
        if i == 0:
            clis.append(init_cli(True, queue_name))
        else:
            clis.append(init_cli(False, queue_name))


    i = 0
    ts = []
    while i < len(clis):
        t = Thread(target=send, args=(clis[i], queue_name, data))
        t.start()
        ts.append(t)
        i += 1

    for t in ts: t.join()

    i = 0
    ts = []
    while i < len(clis):
        t = Thread(target=get, args=(clis[i], queue_name))
        t.start()
        ts.append(t)
        i += 1

    for t in ts: t.join()

    for cli in clis:
        close(cli)


tsn = 1000
queue_names = ['word']
datas = ['hello teacher']

main(tsn, queue_names[0], datas[0])