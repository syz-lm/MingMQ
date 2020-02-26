"""
服务器
"""

import logging
import sys
import json
import traceback
import socket
from threading import Thread
from mingmq.handler import Handler
from mingmq import settings
from mingmq.memory import QueueMemory, QueueAckMemory


class Server:
    """
    主服务类型

    主要有3个功能：
    1. 等待客户端的连接，并分发请求；
    2. 创建Handler去专门的对获取的socket进行信息收发；
    """

    def __init__(self):
        self._queue_memory = QueueMemory()
        self._queue_ack_memory = QueueAckMemory()
        self._sock = socket.socket()
        self._sock.bind((settings.HOST, settings.PORT))
        self._sock.listen(settings.MAX_CONN)

    def handle_accepted(self):
        """
        等待客户端连接
        """
        while True:
            client_sock, addr = self._sock.accept()
            logging.info('客户端[IP %s]已连接。', repr(addr))
            handler = Handler(client_sock, addr,
                              self._queue_memory,
                              self._queue_ack_memory)
            Thread(target=handler.handle_read).start()

    def close(self):
        """
        关闭服务
        """
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
        server.handle_accepted()
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
