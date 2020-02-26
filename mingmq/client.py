"""
客户端
"""

import json
import logging
import socket

from mingmq.message import (ReqLoginMessage, package_message, MessageWindow,
                            SUCCESS, ReqLogoutMessage, ReqDeclareQueueMessage,
                            ReqGetDataFromQueueMessage,
                            ReqSendDataToQueueMessage,
                            ReqACKMessage)
from mingmq.utils import hex_to_str, to_json


class Client:
    """
    服务器客户端
    """
    def __init__(self, host, port):
        self._message_window = MessageWindow()

        self._sock = socket.socket()
        self._sock.connect((host, port))

    def close(self):
        """
        关闭连接
        """
        self._sock.close()

    def login(self, user_name, passwd):
        """
        登录服务器
        :param user_name: str，帐号
        :param passwd: str，密码
        :return: boolean，True登录成功，False登录失败
        """
        req_login_msg = ReqLoginMessage(user_name, passwd)
        self._sock.send(package_message(json.dumps(req_login_msg)))

        while True:
            data = self._sock.recv(1024)
            self._message_window.grouping_message(data.decode())
            if self._message_window.finished():
                for buf in self._message_window.loop_message_window():
                    msg = to_json(hex_to_str(buf))
                    logging.info('服务器发送过来的消息[%s]。', repr(msg))
                    if msg['status'] == SUCCESS:
                        return True
                    return False

    def logout(self, user_name, passwd):
        """
        退出
        """
        req_logout_msg = ReqLogoutMessage(user_name, passwd)
        self._sock.send(package_message(json.dumps(req_logout_msg)))

        while True:
            data = self._sock.recv(1024)
            self._message_window.grouping_message(data.decode())

            if self._message_window.finished():
                for buf in self._message_window.loop_message_window():
                    msg = to_json(hex_to_str(buf))
                    logging.info('服务器发送过来的消息[%s]。', repr(msg))
                    if msg['status'] == SUCCESS:
                        return True
                    return False

    def declare_queue(self, queue_name):
        """
        声明队列
        """
        req_declare_queue_msg = ReqDeclareQueueMessage(queue_name)
        self._sock.send(package_message(json.dumps(req_declare_queue_msg)))

        while True:
            data = self._sock.recv(1024)
            self._message_window.grouping_message(data.decode())
            if self._message_window.finished():
                for buf in self._message_window.loop_message_window():
                    msg = to_json(hex_to_str(buf))
                    logging.info('服务器发送过来的消息[%s]。', repr(msg))
                    if msg['status'] == SUCCESS:
                        return True
                    return False

    def get_data_from_queue(self, queue_name):
        """
        从队列中获取数据
        """
        req_get_data_from_queue_msg = ReqGetDataFromQueueMessage(queue_name)
        self._sock.send(package_message(json.dumps(
            req_get_data_from_queue_msg)))

        while True:
            data = self._sock.recv(1024)
            self._message_window.grouping_message(data.decode())

            if self._message_window.finished():
                for buf in self._message_window.loop_message_window():
                    msg = to_json(hex_to_str(buf))
                    logging.info('服务器发送过来的消息[%s]。', repr(msg))
                    if msg['status'] == SUCCESS:
                        return msg['json_obj']
                    return None

    def send_data_to_queue(self, queue_name, message_data):
        """
        向队列中发送数据
        """
        rsdfqm = ReqSendDataToQueueMessage(queue_name, message_data)
        self._sock.send(package_message(json.dumps(rsdfqm)))

        while True:
            data = self._sock.recv(1024)
            self._message_window.grouping_message(data.decode())

            if self._message_window.finished():
                for buf in self._message_window.loop_message_window():
                    msg = to_json(hex_to_str(buf))
                    logging.info('服务器发送过来的消息[%s]。', repr(msg))
                    if msg['status'] == SUCCESS:
                        return True
                    return False

    def ack_message(self, queue_name, message_id):
        """
        消息确认
        """
        req_ack_msg = ReqACKMessage(queue_name, message_id)
        self._sock.send(package_message(json.dumps(req_ack_msg)))

        while True:
            data = self._sock.recv(1024)
            self._message_window.grouping_message(data.decode())

            if self._message_window.finished():
                for buf in self._message_window.loop_message_window():
                    msg = to_json(hex_to_str(buf))
                    logging.info('服务器发送过来的消息[%s]。', repr(msg))
                    if msg['status'] == SUCCESS:
                        return True
                    return False
