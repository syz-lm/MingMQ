"""
客户端启动测试
"""
import logging
from threading import Thread

from mingmq.client import Client

logging.basicConfig(level=logging.ERROR)

HTML = ''
with open('./test.html', encoding='utf-8') as file_desc:
    HTML = file_desc.read()


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
    if len(json_obj) != 0:
        message_id = json_obj[0]['message_id']

    if message_id:
        client.ack_message('hello', message_id)

        print('消息确认成功')


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

    ts = []
    for cli in clis:
        t = Thread(target=send, args=(cli, queue_name, data))
        t.start()
        ts.append(t)

    for t in ts: t.join()

    # ts = []
    # for cli in clis:
    #     t = Thread(target=get, args=(cli,))
    #     t.start()
    #     ts.append(t)
    #
    # for t in ts: t.join()

    for cli in clis:
        close(cli)


tsn = 5000
queue_names = ['web_page', 'word']
datas = [HTML, 'hello teacher']

main(tsn, queue_names[0], datas[0])
main(tsn, queue_names[1], datas[1])