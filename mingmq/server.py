""" 服务器 """

import logging
import platform
import socket

if platform.platform().startswith('Linux'):
    import select
    from mingmq.memory import QueueMemory, TaskAckMemory
else:
    from threading import Thread
    from mingmq.memory import SyncQueueMemory as QueueMemory, SyncTaskAckMemory as TaskAckMemory

from mingmq.memory import StatMemory

from mingmq.handler import Handler
from mingmq.status import ServerStatus


class Server:
    def __init__(self, server_status: ServerStatus):
        self._server_status = server_status

        self._queue_memory = QueueMemory()  # 定义消息队列内存
        self._stat_memory = StatMemory() # 统计内存
        self._queue_ack_memory = TaskAckMemory()  # 定义消息队列应答内存

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # 设置IP地址复用
        self._sock.bind((self._server_status.get_host(), self._server_status.get_port()))
        self._sock.listen(self._server_status.get_max_conn())  # 监听并设置最大连接数

        plat = platform.platform()

        if plat.startswith('Linux'):
            self._init_server_socket()

    def _init_server_socket(self):
        self._sock.setblocking(False)  # 服务设置非阻塞
        self._timeout = self._server_status.get_timeout()  # 超时时间
        self._epoll = select.epoll()  # 创建epoll事件对象，后续要监控的事件添加到其中
        self._epoll.register(self._sock.fileno(), select.EPOLLIN)  # 注册服务监听文件描述符到等待读事件集合
        self._fd_to_handler = dict()  # 文件描述符对应socket

        self._fd_to_handler[self._fileno()] = self

    def serv_forever(self):
        if platform.platform().startswith('Linux'):
            self._epoll_mode()
        else:
            self._thread_mode()

    def _fileno(self):
        return self._sock.fileno()

    def _thread_mode(self):
        while True:
            client_sock, addr = self._sock.accept()
            handler = Handler(client_sock, addr, self._queue_memory,
                              self._queue_ack_memory, self._stat_memory, self._server_status)
            Thread(target=handler.handle_thread_mode_read).start()

    def _epoll_mode(self):
        while True:
            logging.info("等待活动连接")
            events = self._epoll.poll(self._timeout)  # 轮询注册的事件集合，返回值为[(文件句柄，对应的事件)，(...),....]
            if not events:
                logging.info("epoll超时无活动连接，重新轮询")
            else:
                self._loop_events(events)

    def _loop_events(self, events):
        logging.info("有 %d个新事件，开始处理", len(events))
        for fd, event in events:  # 文件描述符，事件
            handler = self._fd_to_handler[fd]

            # 如果活动socket为当前服务器socket，表示有新连接
            if handler == self:
                self._new_conn_comming()
            # 关闭事件
            elif event & select.EPOLLHUP:
                self._close_event(fd)
            # 可读事件
            elif event & select.EPOLLIN:
                self._readable_event(handler, fd)
            # 可写事件
            elif event & select.EPOLLOUT:
                self._writeable_event(handler, fd)

    def _new_conn_comming(self):
        conn, addr = self._sock.accept()
        logging.info("新连接：%s", addr)
        conn.setblocking(False)  # 新连接socket设置为非阻塞
        self._epoll.register(conn.fileno(), select.EPOLLIN)  # 注册新连接fd到待读事件集合
        self._fd_to_handler[conn.fileno()] = Handler(conn, addr, self._queue_memory,
                                                     self._queue_ack_memory, self._stat_memory,
                                                     self._server_status)

    def _close_event(self, fd):
        logging.info('client close')
        self._epoll.unregister(fd)  # 在self._epoll中注销客户端的文件句柄
        self._fd_to_handler[fd].close()  # 关闭客户端的文件句柄
        del self._fd_to_handler[fd]  # 在字典中删除与已关闭客户端相关的信息

    def _readable_event(self, handler: Handler, fd):
        handler.handle_epoll_mode_read()
        if handler.is_connected() is False:
            self._close_event(fd)
        else:
            self._epoll.modify(fd, select.EPOLLOUT)  # 修改句柄为可写事件

    def _writeable_event(self, handler: Handler, fd):
        handler.handle_epoll_mode_write()
        if handler.is_connected() is False:
            self._close_event(fd)
        else:
            self._epoll.modify(fd, select.EPOLLIN)  # 修改文件句柄为读事件

    def close(self):
        if platform.platform().startswith('Linux'):
            self._epoll.unregister(self._sock.fileno())  # 在epoll中注销服务器文件句柄
            self._epoll.close()  # 关闭epoll

        self._sock.close()  # 关闭服务器socket
