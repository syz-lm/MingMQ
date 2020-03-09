""" 服务器 """

import logging
import platform
import traceback
import socket
from multiprocessing import Queue

if platform.platform().startswith('Linux'):
    import select
    from mingmq.memory import QueueMemory, TaskAckMemory
else:
    from threading import Thread
    from mingmq.memory import SyncQueueMemory as QueueMemory
    from mingmq.memory import SyncTaskAckMemory as TaskAckMemory

from mingmq.memory import StatMemory

from mingmq.handler import Handler
from mingmq.status import ServerStatus


class Server:
    logger = logging.getLogger('Server')

    def __init__(
        self,
        server_status: ServerStatus,
        completely_persistent_process_queue: Queue,
        ack_process_queue: Queue
    ):
        self._server_status = server_status

        self._queue_memory = QueueMemory()
        self._stat_memory = StatMemory()
        self._queue_ack_memory = TaskAckMemory()

        self._completely_persistent_process_queue = completely_persistent_process_queue
        self._ack_process_queue = ack_process_queue

    def get_memory(self):
        return self._queue_memory, self._queue_ack_memory

    def init_server_socket(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self._server_status.get_host(), self._server_status.get_port()))
        self._sock.listen(self._server_status.get_max_conn())

        plat = platform.platform()

        if plat.startswith('Linux'):
            self._sock.setblocking(False)
            self._timeout = self._server_status.get_timeout()
            self._epoll = select.epoll()
            self._epoll.register(self._sock.fileno(), select.EPOLLIN)
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
            try:
                handler = Handler(client_sock, addr, self._queue_memory,
                                  self._queue_ack_memory, self._stat_memory,
                                  self._server_status, self._completely_persistent_process_queue,
                                  self._ack_process_queue)
                Thread(target=handler.handle_thread_mode_read).start()
            except:
                self.logger.error(traceback.format_exc())

    def _epoll_mode(self):
        while True:
            self.logger.info("等待活动连接，还有%d个连接。", len(self._fd_to_handler))
            events = self._epoll.poll(self._timeout)
            if not events:
                self.logger.info("epoll超时无活动连接，重新轮询")
            else:
                self._loop_events(events)

    def _loop_events(self, events):
        self.logger.info("有 %d个新事件，开始处理", len(events))
        for fd, event in events:  # 文件描述符，事件
            handler = self._fd_to_handler[fd]

            """
            EPOLLERR = 8
            EPOLLET = 2147483648
            EPOLLEXCLUSIVE = 268435456
            EPOLLHUP = 16
            EPOLLIN = 1
            EPOLLMSG = 1024
            EPOLLONESHOT = 1073741824
            EPOLLOUT = 4
            EPOLLPRI = 2
            EPOLLRDBAND = 128
            EPOLLRDHUP = 8192
            EPOLLRDNORM = 64
            EPOLLWRBAND = 512
            EPOLLWRNORM = 256
            """
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
            # 错误事件
            elif event & select.EPOLLERR:
                self._close_event(fd)
            else:
                self._close_event(fd)


    def _new_conn_comming(self):
        try:
            conn, addr = self._sock.accept()
            self.logger.info("新连接：%s", addr)
            conn.setblocking(False)  # 新连接socket设置为非阻塞
            self._epoll.register(conn.fileno(), select.EPOLLIN)  # 注册新连接fd到待读事件集合
            self._fd_to_handler[conn.fileno()] = Handler(conn, addr, self._queue_memory,
                                                         self._queue_ack_memory, self._stat_memory,
                                                         self._server_status, self._completely_persistent_process_queue,
                                                         self._ack_process_queue)
        except: # 因为不知道会出现什么不可预知的问题。
            self.logger.error(traceback.format_exc())

    def _close_event(self, fd):
        self.logger.info('client close, 还有%d个连接未释放。', len(self._fd_to_handler))
        try:
            self._epoll.unregister(fd)  # 在self._epoll中注销客户端的文件句柄
        except:
            self.logger.error(traceback.format_exc())

        try:
            self._fd_to_handler[fd].close()  # 关闭客户端的文件句柄
        except:
            self.logger.error(traceback.format_exc())

        try:
            del self._fd_to_handler[fd]  # 在字典中删除与已关闭客户端相关的信息
        except:
            self.logger.error(traceback.format_exc())

    def _readable_event(self, handler: Handler, fd):
        try:
            handler.handle_epoll_mode_read()
        except:
            self.logger.error(traceback.format_exc())
            self._close_event(fd)
            return

        try:
            if handler.is_connected() is False:
                self._close_event(fd)
            else:
                self._epoll.modify(fd, select.EPOLLOUT)  # 修改句柄为可写事件
        except:
            self.logger.error(traceback.format_exc())

    def _writeable_event(self, handler: Handler, fd):
        try:
            handler.handle_epoll_mode_write()
        except:
            self.logger.error(traceback.format_exc())
            self._close_event(fd)
            return

        if handler.is_connected() is False:
            self._close_event(fd)
        else:
            self._epoll.modify(fd, select.EPOLLIN)  # 修改文件句柄为读事件

    def close(self):
        try:
            if platform.platform().startswith('Linux'):
                self._epoll.unregister(self._sock.fileno())  # 在epoll中注销服务器文件句柄
                self._epoll.close()  # 关闭epoll

            self._sock.close()  # 关闭服务器socket
        except:
            self.logger.error(traceback.format_exc())
