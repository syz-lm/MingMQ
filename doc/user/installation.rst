Installation
===============

The last stable release is available on PyPI and can be installed with ``pip``::

    pip3 install MingMQ


依赖
-------------

* Python -- one of the following:

  - CPython_ >= 3.8
  - CPython_ >= 3.5
  - CPython_ >= 3.4

.. _CPython: http://www.python.org/

安装成功之后就会生成mmserver和mmweb这两个可执行文件。

启动mmserver
-------------

调用mmserver来查看命令行参数和启动mmserver服务

.. code:: bash

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

从上面可以看到，默认端口15673，用户名密码，mingmq, mm5201314。

启动mmweb
---------------

这是一个web控制台程序

.. code:: bash

    $ mmweb

启动之后在浏览器中输入http://服务器运行地址:15674输入账号密码就可以登录使用了。