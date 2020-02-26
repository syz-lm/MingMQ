"""
与客户端进行通信的模块
"""

import json
import logging

from mingmq.message import (ResMessage, SUCCESS, FAIL, MESSAGE_TYPE,
                            package_message, MessageWindow, Task)
from mingmq.settings import USER_NAME, PASSWD
from mingmq.utils import hex_to_str, to_json, check_msg
from mingmq.memory import QueueMemory, QueueAckMemory


class Handler:
    """
    Handler处理模块
    """

    def __init__(self, sock, addr,
                 queue_memory: QueueMemory,
                 queue_ack_memory: QueueAckMemory):
        """
        初始化
        :param sock: socket，套接字
        :param addr: 地址信息
        :param queue_memory: QueueMemory，队列数据内存对象
        :param queue_ack_memory: QueueAckMemory，队列任务应答内存对象
        """
        self._sock = sock
        self._addr = addr
        self._queue_memory = queue_memory
        self._queue_ack_memory = queue_ack_memory

        self._session_id = None

        self._connected = False

        self._message_window = MessageWindow()

        self._connected = True

    def close(self):
        """
        关闭连接
        """
        if self._connected:
            self._sock.close()
            logging.info('客户端[IP %s]已断开。', repr(self._addr))
            self._connected = False

    def _recv(self, size):
        """
        接收数据
        """
        data = None
        try:
            data = self._sock.recv(size)
        except (ConnectionResetError, OSError) as err:
            # OSError: [WinError 10038] 在一个非套接字上尝试了一个操作。
            # ConnectionResetError 远程主机主动断开了连接
            logging.info(err)

        return data

    def handle_read(self):
        """
        读取客户端发送的数据
        """
        while self._connected:
            data = self._recv(1024)

            logging.info('客户端发来数据 [%s]', repr(data))
            # 如果客户端传递消息字节的长度为0，则断开连接
            if data is not None and len(data) > 0:
                self._message_window.grouping_message(data.decode())
                # 将data转换成json对象，如果转换失败则发送参数错误消息给客户端，并且返回False
                for buf in self._message_window.loop_message_window():
                    logging.info('客户端[IP %s]从消息窗口中取出的消息[%s]。',
                                 repr(self._addr), buf[: 100])
                    self._deal_message(buf)
            else:
                self.close()

    def _deal_message(self, buf):
        """
        处理消息
        """
        chex = None
        try:
            chex = hex_to_str(buf)
        except ValueError:
            pass

        if chex is None:
            res_msg = ResMessage(MESSAGE_TYPE['DATA_WRONG'],
                                 FAIL, [])
            res_pkg = package_message(json.dumps(res_msg))
            self._send_data(res_pkg)
            self.close()
            return

        msg = to_json(chex)

        if msg is False:
            res_msg = ResMessage(MESSAGE_TYPE['DATA_WRONG'],
                                 FAIL, [])
            res_pkg = package_message(json.dumps(res_msg))
            self._send_data(res_pkg)
            self.close()
            return

        logging.info('客户端[IP %s]发来数据转换成JSON对象[%s]。',
                     repr(self._addr), repr(msg))

        # 如果msg为False则断开连接
        if msg is not False:
            # 校验msg Json对象，如果校验失败则发送消息类型错误给客户端，并返回False
            # 如果返回False，则断开连接
            if check_msg(msg) is not False:
                # 判断当前socket是否已经登录，未登录，转换到登录请求处理函数
                # 如果返回False则表示没有登录，如果已经登录则开始验证之后的消息
                if self._has_loggin() is not False:
                    self._dispatch_request(msg)
                else:
                    self._login(msg)
            else:
                self.close()
        else:
            self.close()

    def _dispatch_request(self, msg):
        """
        分发请求
        """
        _type = msg['type']
        if _type == MESSAGE_TYPE['LOGOUT']:
            self._logout(msg)
        elif _type == MESSAGE_TYPE['DECLARE_QUEUE']:
            self._declare_queue(msg)
        elif _type == MESSAGE_TYPE['GET_DATA_FROM_QUEUE']:
            self._get_data_from_queue(msg)
        elif _type == MESSAGE_TYPE['SEND_DATA_TO_QUEUE']:
            self._send_data_to_queue(msg)
        elif _type == MESSAGE_TYPE['ACK_MESSAGE']:
            self._ack_message(msg)
        else:
            self._not_found(msg)

    def _data_wrong(self, opera, args, msg):
        """
        数据错误
        """
        err = 0
        for arg in args:
            if arg not in msg:
                err += 1
                break

        if err > 0:
            logging.info('%s, 参数错误 %s, 需要参数 %s', opera, msg, args)

            res_msg = ResMessage(MESSAGE_TYPE['DATA_WRONG'],
                                 FAIL, [])
            res_pkg = package_message(json.dumps(res_msg))
            self._send_data(res_pkg)
            return False
        return True

    def _ack_message(self, msg):
        """
        任务消息确认
        """
        if self._data_wrong('_ack_message',
                            ('queue_name', 'message_id'), msg) is not False:
            queue_name = msg['queue_name']
            message_id = msg['message_id']
            if self._queue_ack_memory.get(queue_name, message_id):
                res_msg = ResMessage(MESSAGE_TYPE['ACK_MESSAGE'], SUCCESS, [])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(MESSAGE_TYPE['ACK_MESSAGE'], FAIL, [])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)

    def _send_data_to_queue(self, msg):
        """
        向指定队列发送任务
        """
        if self._data_wrong('_send_data_to_queue',
                            ('queue_name', 'message_data'), msg) is not False:
            queue_name = msg['queue_name']
            message_data = msg['message_data']
            task = Task(message_data)
            if self._queue_memory.put(queue_name, task):
                res_msg = ResMessage(MESSAGE_TYPE['SEND_DATA_TO_QUEUE'],
                                     SUCCESS, [])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(MESSAGE_TYPE['SEND_DATA_TO_QUEUE'],
                                     FAIL, [])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)

    def _get_data_from_queue(self, msg):
        """
        从指定队列中获取一条任务
        """
        if self._data_wrong('_get_data_from_queue',
                            ('queue_name', ), msg) is not False:
            queue_name = msg['queue_name']
            task = self._queue_memory.get(queue_name)
            if task is not None:
                message_id = task.message_id
                self._queue_ack_memory.put(queue_name, message_id)

                res_msg = ResMessage(
                    MESSAGE_TYPE['GET_DATA_FROM_QUEUE'], SUCCESS, [task])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(
                    MESSAGE_TYPE['GET_DATA_FROM_QUEUE'], FAIL, [task])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)

    def _declare_queue(self, msg):
        """
        队列声明
        """
        if self._data_wrong('_declare_queue',
                            ('queue_name', ), msg) is not False:
            queue_name = msg['queue_name']
            if self._queue_memory.decleare_queue(queue_name):
                self._queue_ack_memory.declare_queue(queue_name)

                res_msg = ResMessage(MESSAGE_TYPE['DECLARE_QUEUE'],
                                     SUCCESS, [])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(MESSAGE_TYPE['DECLARE_QUEUE'], FAIL, [])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)

    def _not_found(self, msg):
        """
        找不到这个方法
        """
        res_msg = ResMessage(MESSAGE_TYPE['NOT_FOUND'], FAIL, [msg])
        res_pkg = package_message(json.dumps(res_msg))
        self._send_data(res_pkg)

    def _login(self, msg):
        """
        登录验证
        """
        if self._data_wrong('_login',
                            ('user_name', 'passwd'), msg) is not False:
            user_name = msg['user_name']
            passwd = msg['passwd']

            if USER_NAME != user_name or PASSWD != passwd:
                res_msg = ResMessage(MESSAGE_TYPE['LOGIN'], FAIL, [])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)
                self.close()
            else:
                self._session_id = str(self) + ':' +\
                        repr(self._addr) + ':' + user_name + '/' + passwd
                res_msg = ResMessage(MESSAGE_TYPE['LOGIN'], SUCCESS, [])
                self._send_data(package_message(json.dumps(res_msg)))
        else:
            self.close()

    def _logout(self, msg):
        """
        退出
        """
        # type = msg['type']
        if self._data_wrong('_logout',
                            ('user_name', 'passwd'), msg) is not False:
            user_name = msg['user_name']
            passwd = msg['passwd']
            if USER_NAME != user_name or PASSWD != passwd:
                res_msg = ResMessage(MESSAGE_TYPE['LOGOUT'], FAIL, [])
                res_pkg = package_message(json.dumps(res_msg))
                self._send_data(res_pkg)
            else:
                self._session_id = str(self) + ':' + \
                        repr(self._addr) + ':' + user_name + '/' + passwd
                res_msg = ResMessage(MESSAGE_TYPE['LOGOUT'], SUCCESS, [])
                self._send_data(package_message(json.dumps(res_msg)))

                self.close()

    def _send_data(self, data):
        """
        发送数据
        """
        if self._connected:
            self._sock.send(data)

    def _has_loggin(self):
        """
        验证当前socket是否已经登录
        :return: boolean, True 已登录，False未登录
        """
        if self._session_id is not None:
            return True
        return False