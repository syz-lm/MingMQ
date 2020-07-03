# MingMQ

[文档](http://serv_pro:5000/MingMQ/)，
[TODO](http://serv_pro:3000/zswj123/MingMQ/src/master/TODO.md)

一个跨平台MQ消息服务，支持和RabbitMQ服务器的大多数功能。

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

## 企业API

```
$ pip3.8 install mingmq
```

```
import sys
from mingmq.client import Pool

_port = 15673
_user_name = 'mingmq'
_passwd = 'mm5201314'
_pool_size = 10
_pool = Pool('localhost', _port, _user_name, _passwd, _pool_size)

# 声明队列
_queue_name = 'hello'
result = _pool.opera('declare_queue', *(_queue_name,))
# {"json_obj":[],"status":1,"type":2}
if result['status'] != 1:
    print('队列声明失败')
    sys.exit(-1)

# 向队列发送数据
message = 'hello, mingmq'
result = _pool.opera('send_data_to_queue', *(_queue_name, message))
# {"json_obj":[],"status":1,"type":3}
if result['status'] != 1:
    print('发送失败')

# 从队列获取数据
result = _pool.opera('get_data_from_queue', *(_queue_name,))
# {"json_obj":[{"message_data":"12","message_id":"task_id:1593816809.7238715"}],"status":1,"type":4}
if result['status'] != 1:
    print('获取失败')
    sys.exit(-1)
message_id = result['json_obj'][0]['message_id']
message_data = result['json_obj'][0]['message_data']
print('任务数据为', message_data)
# 12

# 确认任务
result = _pool.opera('ack_message', *(_queue_name, message_id))
# {"json_obj":[],"status":1,"type":5}
if result['status'] != 1:
    print('确认失败')

_pool.release()
```