# MingMQ

一个跨平台高性能MQ消息服务，支持和RabbitMQ服务器的大多数功能。

![](https://github.com/zswj123/MingMQ/blob/local/logo.jpg)

## Server

```
$ mmserver --help
usage: 欢迎使用MingMQ消息队列服务器！
 [-h] [--HOST HOST] [--PORT PORT] [--MAX_CONN MAX_CONN] [--USER_NAME USER_NAME] [--PASSWD PASSWD] [--TIMEOUT TIMEOUT]

optional arguments:
  -h, --help            show this help message and exit
  --HOST HOST           输入服务器IP地址：: 默认，0.0.0.0
  --PORT PORT           输入服务器端口：默认，15673
  --MAX_CONN MAX_CONN   输入服务器的最大并发数，默认，100
  --USER_NAME USER_NAME
                        输入服务器账号，默认，mingmq
  --PASSWD PASSWD       输入服务器密码，默认，mm5201314
  --TIMEOUT TIMEOUT     输入服务器超时时间（仅linux下有效），默认，10

$ mmserver
正在启动，服务器的配置为
IP/端口:0.0.0.0:15673, 用户名/密码:mingmq/mm5201314，最大连接数:100，超时时间: 10

```

## Client
### Hello, World!
```
client = Client('192.168.1.30', 15673)

if client.login('mingmq', 'mm5201314') is not True:
    sys.exit(-1)

print('登录成功')
client.declare_queue('hello')

result = client.send_data_to_queue('hello', HTML)
print('发送任务', result)

message_data = client.get_data_from_queue('hello')
print('获取任务成功', message_data)

result = client.ack_message('hello', message_data[0]['message_id'])
print('消息确认成功', result)

result = client.delete_queue('hello')
print('删除队列', result)

result = client.logout('mingmq', 'mm5201314')
print('注销', result)

client.close()
print('关闭成功')
```

# MORE

* https://www.rabbitmq.com/