"""
客户端
"""

import json
import logging
import socket
import struct
import math

from mingmq.message import (ReqLoginMessage, MessageWindow,
                            SUCCESS, ReqLogoutMessage, ReqDeclareQueueMessage,
                            ReqGetDataFromQueueMessage, ReqClearQueueMessage,
                            ReqSendDataToQueueMessage, ReqDeleteQueueMessage,
                            ReqACKMessage, MAX_DATA_LENGTH)
from mingmq.utils import to_json


class Client:
    """
    服务器客户端
    """

    def __init__(self, host, port):
        self._message_window = MessageWindow()

        self._sock = socket.socket()
        self._connected = False
        self._sock.connect((host, port))
        self._connected = True

    def _send(self, data):
        try:
            if self._connected:
                self._sock.sendall(data)
        except OSError:
            pass

    def _recv(self, bytes_size):
        if self._connected:
            try:
                data = self._sock.recv(bytes_size)
                if data:
                    return data
                else:
                    self._connected = False
            except OSError:
                self._connected = False

        return None

    def close(self):
        """
        关闭连接
        """
        if self._connected:
            self._sock.close()

    def login(self, user_name, passwd):
        """
        登录服务器
        :param user_name: str，帐号
        :param passwd: str，密码
        :return: boolean，True登录成功，False登录失败
        """
        # 发送数据
        req_login_msg = ReqLoginMessage(user_name, passwd)
        req_pkg = json.dumps(req_login_msg).encode()
        send_header = struct.pack('!i', len(req_pkg))
        self._send(send_header + req_pkg)

        # 接收数据
        recv_header = self._recv(4)
        if recv_header:
            data_size, = struct.unpack('!i', recv_header)

            should_read = min(data_size, MAX_DATA_LENGTH)

            data = b''
            while self._connected and len(data) < data_size:
                buf = self._recv(should_read)
                if buf:
                    data += buf
                    if len(data) == data_size:
                        msg = to_json(data)
                        logging.info('服务器发送过来的消息[%s]。', repr(msg))
                        if msg['status'] == SUCCESS:
                            return True
                        return False
                else:
                    self._connected = False
                    self.close()
        else:
            self._connected = False
            self.close()

    def logout(self, user_name, passwd):
        """
        退出
        """
        req_logout_msg = ReqLogoutMessage(user_name, passwd)
        req_pkg = json.dumps(req_logout_msg).encode()
        send_header = struct.pack('!i', len(req_pkg))
        self._send(send_header + req_pkg)

        # 接收数据
        recv_header = self._recv(4)
        if recv_header:
            data_size, = struct.unpack('!i', recv_header)

            should_read = min(data_size, MAX_DATA_LENGTH)
            data = b''
            while self._connected and len(data) < data_size:
                buf = self._recv(should_read)
                if buf:
                    data += buf
                    if len(data) >= data_size:
                        msg = to_json(data)
                        logging.info('服务器发送过来的消息[%s]。', repr(msg))
                        if msg['status'] == SUCCESS:
                            return True
                        return False
                else:
                    self._connected = False
                    self.close()
                    return False
        else:
            self._connected = False
            self.close()
            return False

    def declare_queue(self, queue_name):
        """
        声明队列
        """
        req_declare_queue_msg = ReqDeclareQueueMessage(queue_name)
        req_pkg = json.dumps(req_declare_queue_msg).encode()
        send_header = struct.pack('!i', len(req_pkg))
        self._send(send_header + req_pkg)

        # 接收数据
        recv_header = self._recv(4)
        if recv_header:
            data_size, = struct.unpack('!i', recv_header)

            should_read = min(data_size, MAX_DATA_LENGTH)
            data = b''
            while self._connected and len(data) < data_size:
                buf = self._recv(should_read)
                if buf:
                    data += buf
                    if len(data) >= data_size:
                        msg = to_json(data)
                        logging.info('服务器发送过来的消息[%s]。', repr(msg))
                        if msg['status'] == SUCCESS:
                            return True
                        return False
                else:
                    self._connected = False
                    self.close()
                    return False
        else:
            self._connected = False
            self.close()
            return False

    def get_data_from_queue(self, queue_name):
        """
        从队列中获取数据
        """
        req_get_data_from_queue_msg = ReqGetDataFromQueueMessage(queue_name)
        req_pkg = json.dumps(req_get_data_from_queue_msg).encode()
        send_header = struct.pack('!i', len(req_pkg))
        self._send(send_header + req_pkg)

        # 接收数据
        recv_header = self._recv(4)
        if recv_header:
            data_size, = struct.unpack('!i', recv_header)

            should_read = min(data_size, MAX_DATA_LENGTH)
            data = b''
            while self._connected and len(data) < data_size:
                buf = self._recv(should_read)
                if buf:
                    data += buf

                    if len(data) == data_size:
                        msg = to_json(data)
                        logging.info('服务器发送过来的消息[%s]。', repr(msg))
                        if msg['status'] == SUCCESS:
                            return msg['json_obj']
                        return None
                else:
                    self._connected = False
                    self.close()
                    return False
        else:
            self._connected = False
            self.close()
            return False

    def send_data_to_queue(self, queue_name, message_data):
        """
        向队列中发送数据
        """
        rsdfqm = ReqSendDataToQueueMessage(queue_name, message_data)
        req_pkg = json.dumps(rsdfqm).encode()
        send_header = struct.pack('!i', len(req_pkg))
        self._send(send_header + req_pkg)

        # 接收数据
        recv_header = self._recv(4)
        if recv_header:
            data_size, = struct.unpack('!i', recv_header)

            should_read = min(data_size, MAX_DATA_LENGTH)
            data = b''
            while self._connected and len(data) < data_size:
                buf = self._recv(should_read)
                if buf:
                    data += buf

                    if len(data) == data_size:
                        msg = to_json(data)
                        logging.info('服务器发送过来的消息[%s]。', repr(msg))
                        if msg['status'] == SUCCESS:
                            return True
                        return False
                else:
                    self._connected = False
                    self.close()
                    return False
        else:
            self._connected = False
            self.close()
            return False

    def ack_message(self, queue_name, message_id):
        """
        消息确认
        """
        req_ack_msg = ReqACKMessage(queue_name, message_id)
        req_pkg = json.dumps(req_ack_msg).encode()
        send_header = struct.pack('!i', len(req_pkg))
        self._send(send_header + req_pkg)

        # 接收数据
        recv_header = self._recv(4)
        if recv_header:
            data_size, = struct.unpack('!i', recv_header)

            should_read = min(data_size, MAX_DATA_LENGTH)
            data = b''
            while self._connected and len(data) < data_size:
                buf = self._recv(should_read)
                if buf:
                    data += buf
                    if len(data) >= data_size:
                        msg = to_json(buf)
                        logging.info('服务器发送过来的消息[%s]。', repr(msg))
                        if msg['status'] == SUCCESS:
                            return True
                        return False
                else:
                    self._connected = False
                    self.close()
                    return False
        else:
            self._connected = False
            self.close()
            return False

    def del_queue(self, queue_name):
        """
        删除队列
        """
        req_ack_msg = ReqDeleteQueueMessage(queue_name)
        req_pkg = json.dumps(req_ack_msg).encode()
        send_header = struct.pack('!i', len(req_pkg))
        self._send(send_header + req_pkg)

        # 接收数据
        recv_header = self._recv(4)
        if recv_header:
            data_size, = struct.unpack('!i', recv_header)

            should_read = min(data_size, MAX_DATA_LENGTH)
            data = b''
            while self._connected and len(data) < data_size:
                buf = self._recv(should_read)
                if buf:
                    data += buf
                    if len(data) >= data_size:
                        msg = to_json(buf)
                        logging.info('服务器发送过来的消息[%s]。', repr(msg))
                        if msg['status'] == SUCCESS:
                            return True
                        return False
                else:
                    self._connected = False
                    self.close()
                    return False
        else:
            self._connected = False
            self.close()
            return False

    def clear_queue(self, queue_name):
        """
        清空队列
        """
        req_ack_msg = ReqClearQueueMessage(queue_name)
        req_pkg = json.dumps(req_ack_msg).encode()
        send_header = struct.pack('!i', len(req_pkg))
        self._send(send_header + req_pkg)

        # 接收数据
        recv_header = self._recv(4)
        if recv_header:
            data_size, = struct.unpack('!i', recv_header)

            should_read = min(data_size, MAX_DATA_LENGTH)
            data = b''
            while self._connected and len(data) < data_size:
                buf = self._recv(should_read)
                if buf:
                    data += buf
                    if len(data) >= data_size:
                        msg = to_json(buf)
                        logging.info('服务器发送过来的消息[%s]。', repr(msg))
                        if msg['status'] == SUCCESS:
                            return True
                        return False
                else:
                    self._connected = False
                    self.close()
                    return False
        else:
            self._connected = False
            self.close()
            return False
