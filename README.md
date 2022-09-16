# MingMQ

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

启动消息服务
```
$ mmserver --ACK_PROCESS_DB_FILE ./ack_process_db_file.db --COMPLETELY_PERSISTENT_PROCESS_DB_FILE ./completely_persistent_process_db_file.db
```

## Web Console

启动mmserver监控WEB控制台：

```
$ mmweb
```

默认端口15674。

:bug:时光流逝，转眼已经过去了两年了，这个项目代码并不好，之前的zswj123账号又忘了，为了分享就只得用新账号，本来准备删掉.git目录
但是过去的提交记录有非常多的bug修改记录以及知识学习记录，所以还是选择不删了。
