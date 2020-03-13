# MingMQ

[文档](http://serv_pro:5000/MingMQ/)，
[TODO](http://serv_pro:3000/zswj123/MingMQ/src/master/TODO.md)

一个跨平台MQ消息服务，支持和RabbitMQ服务器的大多数功能。

![](http://serv_pro:3000/zswj123/MingMQ/raw/master/logo.jpg)

## Install

```
pip3 install mingmq
```

## Server

```
$ mmserver --help
usage: 欢迎使用MingMQ消息队列服务器。 [-h] [--CONFIG_REUSE CONFIG_REUSE] [--HOST HOST] [--PORT PORT] [--MAX_CONN MAX_CONN] [--USER_NAME USER_NAME] [--PASSWD PASSWD] [--TIMEOUT TIMEOUT]
                         [--ACK_PROCESS_DB_FILE ACK_PROCESS_DB_FILE] [--COMPLETELY_PERSISTENT_PROCESS_DB_FILE COMPLETELY_PERSISTENT_PROCESS_DB_FILE]

optional arguments:
 -h, --help            show this help message and exit
 --CONFIG_REUSE CONFIG_REUSE
                       是否读取配置文件来启动服务：0为不读取，1为读取，1则使用默认配置文件路径/etc/mingmq_config，该路径不允许修改。
 --HOST HOST           输入服务器IP地址：: 默认，0.0.0.0
 --PORT PORT           输入服务器端口：默认，15673
 --MAX_CONN MAX_CONN   输入服务器的最大并发数，默认，100
 --USER_NAME USER_NAME
                       输入服务器账号，默认，mingmq
 --PASSWD PASSWD       输入服务器密码，默认，mm5201314
 --TIMEOUT TIMEOUT     输入服务器超时时间（仅linux下有效），默认，10
 --ACK_PROCESS_DB_FILE ACK_PROCESS_DB_FILE
                       输入服务器确认消息文件名
 --COMPLETELY_PERSISTENT_PROCESS_DB_FILE COMPLETELY_PERSISTENT_PROCESS_DB_FILE
                       输入服务器确认消息文件名

$ mmserver
正在启动，服务器的配置为
IP/端口:0.0.0.0:15673, 用户名/密码:mingmq/mm5201314，最大并发数:100，超时时间: 10
```

默认端口15673。

## Web Console

启动mmserver监控WEB控制台：

```
$ mmweb
```

![](http://serv_pro:3000/zswj123/MingMQ/raw/master/web_console.png)

默认端口15674。

## Client

### Hello, World!

```
"""
客户端启动测试
"""
import logging, time
from threading import Thread, active_count

from mingmq.client import Client

logging.basicConfig(level=logging.ERROR)

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
```

## Future

* K/V数据库；
* 服务器集群，容灾；

## More

* https://www.rabbitmq.com/