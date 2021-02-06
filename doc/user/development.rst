Development
=============

* Ubuntu
* Pycharm
* Python3

如果是windows系统，可以在windows上安装virtualbox 然后在virtualbox上安装ubuntu虚拟机，再在windows上安装pycharm，使用pycharm打开项目
时，指定解释器为ssh的远程解释器，确保解释器时以sudo权限运行之后，然后运行command.py和api.py就可以启动消息队列服务和web控制台了。

用户也可以根据自己的想法去更改默认的启动参数，具体都在command.py中写到。

另外，windows上也可以运行，但是，windows上运行消息队列服务，由于windows不支持epoll所以，就是默认的多线程网络编程模型，在线数上来之后，就会比较
吃内存。

另外调试api.py由于使用了gevent所以必须对pycharm设置一下，在pycharm file ->settings -> Build, Execution... -> Python debugger ->
勾选Gevent compatible即可。