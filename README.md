# MingMQ

一个跨平台MQ消息服务，支持和RabbitMQ服务器的大多数功能。

![](https://github.com/zswj123/MingMQ/blob/master/logo.jpg)

## Server

```
$ mmserver --help
usage: 欢迎使用MingMQ消息队列服务器。 [-h] [--CONFIG_REUSE CONFIG_REUSE] [--HOST HOST]
                          [--PORT PORT] [--MAX_CONN MAX_CONN]
                          [--USER_NAME USER_NAME] [--PASSWD PASSWD]
                          [--TIMEOUT TIMEOUT]
                          [--ACK_PROCESS_DB_FILE ACK_PROCESS_DB_FILE]
                          [--COMPLETELY_PERSISTENT_PROCESS_DB_FILE COMPLETELY_PERSISTENT_PROCESS_DB_FILE]

optional arguments:
  -h, --help            show this help message and exit
  --CONFIG_REUSE CONFIG_REUSE
                        是否读取配置文件来启动服务：0为不读取，1为读取，1则使用默认配置文件路径/etc/mingmq_confi
                        g，该路径不允许修改。
  --HOST HOST           输入服务器IP地址：: 默认，0.0.0.0
  --PORT PORT           输入服务器端口：默认，15673
  --MAX_CONN MAX_CONN   输入服务器的最大并发数，默认，100
  --USER_NAME USER_NAME
                        输入服务器账号，默认，mingmq
  --PASSWD PASSWD       输入服务器密码，默认，mm5201314
  --TIMEOUT TIMEOUT     输入服务器超时时间（仅linux下有效），默认，10
  --ACK_PROCESS_DB_FILE ACK_PROCESS_DB_FILE
                        输入服务器确认消息文件名，默认/var/mingmq/ack_process_db_file.db(wind
                        ows系统下为C:\mingmqck_process_db_file.db)
  --COMPLETELY_PERSISTENT_PROCESS_DB_FILE COMPLETELY_PERSISTENT_PROCESS_DB_FILE
                        输入服务器确认消息文件名，默认/var/mingmq/completely_persistent_proce
                        ss_db_file.db(windows系统下为C:\mingmq\completely_persiste
                        nt_process_db_file.db)


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

![](https://github.com/zswj123/MingMQ/blob/master/web_console.png)

默认端口15674。

## Client
### Hello, World!
```
from mingmq import client

client = Client('192.168.1.30', 15673)

if client.login('mingmq', 'mm5201314') is not True:
    print('登录失败')
    sys.exit(-1)

print('登录成功')
client.declare_queue('hello')

result = client.send_data_to_queue('hello', HTML)
print('发送任务', result)

message_data = client.get_data_from_queue('hello')
print('获取任务', message_data)

result = client.ack_message('hello', message_data[0]['message_id'])
print('消息确认', result)

result = client.delete_queue('hello')
print('删除队列', result)

result = client.logout('mingmq', 'mm5201314')
print('注销', result)

client.close()
print('关闭成功')
```

## Future

* 需要开发一些测试，并生成测试数据图片，用以在readme页面进行展示，用来吸引更多的用户；
* 需要编写一定的rst文档，编写之前需要去询问一下tornado或者借鉴一下他们rst文档是怎么快速生成的方法；
* 需要进行内存优化，省掉不必要的内存开销；
* 需要对api进行优化，需要寻找一种方式去搞定flask app与uswgi 零依赖的运行方式，或许有替代uswgi的方案；
* Linux下等内存和Mac下的内存不一致，同样的任务放在虚拟机中的比mac真是机器中所占用的内存要多，我实验的
Linux发行版为Ubuntu 19 server，可能需要在这方面去探索下造成这个差距的原因；
* gevent运行flask app在mac下会报错，但是在linux下却是可以的；

## More

* https://www.rabbitmq.com/