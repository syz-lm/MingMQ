"""
服务器
"""

import logging
import sys
import json
import traceback
import socket
import select
from queue import Queue, Empty

from mingmq import settings
from mingmq.memory import QueueMemory, QueueAckMemory
from mingmq.handler import Handler


class Server:
    """
    服务器
    """

    def __init__(self):
        # 定义消息队列内存
        self._queue_memory = QueueMemory()
        # 定义消息队列应答内存
        self._queue_ack_memory = QueueAckMemory()

        # 创建socket对象
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 设置IP地址复用
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # 绑定IP地址和端口号
        self._sock.bind((settings.HOST, settings.PORT))

        # 监听并设置最大连接数
        self._sock.listen(settings.MAX_CONN)

        # 服务设置非阻塞
        self._sock.setblocking(False)

        # 超时时间
        self._timeout = 10

        # 创建epoll事件对象，后续要监控的事件添加到其中
        self._epoll = select.epoll()

        # 注册服务监听文件描述符到等待读事件集合
        self._epoll.register(self._sock.fileno(), select.EPOLLIN)

        # 文件描述符对应socket
        self._fd_to_sock = {
            self._sock.fileno(): self._sock
        }

        # 保存客户端消息的字典
        self._message_queues = dict()

    def loop_event(self):
        """
        进行epoll事件循环
        """
        while True:
            logging.info("等待活动连接")
            # 轮询注册的事件集合，返回值为[(文件句柄，对应的事件)，(...),....]
            events = self._epoll.poll(self._timeout)
            if not events:
                logging.info("epoll超时无活动连接，重新轮询")
                continue
            logging.info("有 %d个新事件，开始处理", len(events))

            # 文件描述符，事件
            for fd, event in events:
                client_sock = self._fd_to_sock[fd]

                # 如果活动socket为当前服务器socket，表示有新连接
                if client_sock.fileno() == self._sock.fileno():
                    conn, addr = self._sock.accept()
                    logging.info("新连接：%s", addr)
                    # 新连接socket设置为非阻塞
                    conn.setblocking(False)
                    # 注册新连接fd到待读事件集合
                    self._epoll.register(conn.fileno(), select.EPOLLIN)
                    # 把新连接的文件句柄以及对象保存到字典
                    self._fd_to_sock[conn.fileno()] = conn
                    # 以新连接的对象为键值，值存储在队列中，保存每个连接的信息
                    self._message_queues[conn] = Queue()

                # 关闭事件
                elif event & select.EPOLLHUP:
                    logging.info('client close')
                    # 在self._epoll中注销客户端的文件句柄
                    self._epoll.unregister(fd)
                    # 关闭客户端的文件句柄
                    self._fd_to_sock[fd].close()
                    # 在字典中删除与已关闭客户端相关的信息
                    del self._fd_to_sock[fd]

                # 可读事件
                elif event & select.EPOLLIN:
                    # 接收数据
                    data = client_sock.recv(1024)
                    if data:
                        logging.info("收到数据：%s，客户端：%s", repr(data), client_sock.getpeername())
                        # 将数据放入对应客户端的字典
                        self._message_queues[client_sock].put_nowait(data)
                        # 修改读取到消息的连接到等待写事件集合(即对应客户端收到消息后，再将其fd修改并加入写事件集合)
                        self._epoll.modify(fd, select.EPOLLOUT)

                # 可写事件
                elif event & select.EPOLLOUT:
                    try:
                        # 从字典中获取对应客户端的信息
                        msg = self._message_queues[client_sock].get_nowait()
                    except Empty:
                        logging.info("%s queue empty", client_sock.getpeername())
                        # 修改文件句柄为读事件
                        self._epoll.modify(fd, select.EPOLLIN)
                    else:
                        logging.info("发送数据：%s, 客户端：%s", repr(data), client_sock.getpeername())
                        # 发送数据
                        client_sock.send(msg)

    def close(self):
        """
        关闭服务
        """
        # 在epoll中注销服务器文件句柄
        self._epoll.unregister(self._sock.fileno())
        # 关闭epoll
        self._epoll.close()
        # 关闭服务器socket
        self._sock.close()


def main(config_file=None):
    """
    启动服务

    :param config_file: file desc, 配置文件路径
    """
    with open(config_file) as file_desc:
        config = json.load(file_desc)
        settings.HOST = config['HOST']
        settings.PORT = config['PORT']
        settings.MAX_CONN = config['MAX_CONN']
        settings.USER_NAME = config['USER_NAME']
        settings.PASSWD = config['PASSWD']

    logging.info('服务器[IP %s PORT %s]正在启动。',
                 repr(settings.HOST), repr(settings.PORT))
    server = None
    try:
        server = Server()
        logging.info('启动成功。')
    except (ConnectionRefusedError,
            ConnectionAbortedError, ConnectionResetError, ConnectionError):
        print(traceback.print_exc())
        logging.info('启动失败。')
        sys.exit(-1)

    try:
        server.loop_event()
    except KeyboardInterrupt:
        logging.info('用户使用键盘中断了程序。')
    except (ConnectionRefusedError,
            ConnectionAbortedError, ConnectionResetError, ConnectionError):
        print(traceback.print_exc())
        logging.info('运行时出现无法预知的错误。')

        if server is not None:
            try:
                server.close()
                logging.info('服务器已关闭。')
            except (ConnectionRefusedError,
                    ConnectionAbortedError,
                    ConnectionResetError, ConnectionError):
                print(traceback.print_exc())
                logging.info('服务器主动关闭与客户端的连接时出现无法预知的错误。')
