"""
客户端
"""

import json
import logging
import socket
import struct
import traceback
from collections import deque

from mingmq.message import (ReqLoginMessage,
                            SUCCESS, ReqLogoutMessage, ReqDeclareQueueMessage,
                            ReqGetDataFromQueueMessage, ReqClearQueueMessage,
                            ReqSendDataToQueueMessage, ReqDeleteQueueMessage,
                            ReqACKMessage, MAX_DATA_LENGTH,
                            ReqGetSpeedMessage, ReqGetStatMessage,
                            ReqDeleteAckMessageIDMessage, ReqRestoreAckMessageIDMessage,
                            ReqRestoreSendMessage)
from mingmq.utils import to_json

from threading import Lock


class Pool:
    _LOCK = Lock()

    def __init__(self, host, port, user_name, passwd, size):
        self._host = host
        self._port = port
        self._user_name = user_name
        self._passwd = passwd

        self._size = size

        self._que = deque()

    def init_pool(self):
        for i in range(self._size):
            cli = Client(self._host, self._port)
            cli.login(self._user_name, self._passwd)
            self._que.append(cli)

    def get_conn(self):
        with Pool._LOCK:
            try:
                if len(self._que) != 0:
                    return self._que.popleft()
                else:
                    raise Exception('连接池已空')
            except:
                self.init_pool()
                return self._que.popleft()

    def back_conn(self, conn):
        with Pool._LOCK:
            self._que.append(conn)

    def release(self):
        for conn in self._que:
            try:
                if conn: conn.close()
            except OSError as e:
                print(e)

    def opera(self, method_name, *args):
        conn = None
        try:
            conn = self.get_conn()
            callback = getattr(conn, method_name)
            result = callback(*args)
            if result: return result
            raise
        except Exception:
            print(traceback.format_exc())
            try:
                conn.close()
            except Exception:
                print(traceback.format_exc())
            conn = None
        finally:
            if conn: self.back_conn(conn)


class Client:
    """
    服务器客户端
    """
    def __init__(self, host, port):
        self._sock = socket.socket()
        self._connected = False
        self._sock.connect((host, port))
        self._connected = True
        self._lock = Lock()

    def is_connected(self):
        return self._connected

    def dup(self):
        try:
            self.login(self._user_name, self._passwd)
        except:
            logging.info('重连失败')

    def _send(self, data):
        try:
            if self._connected:
                self._sock.sendall(data)
        except OSError as e:
            print(e)
            self._connected = False

    def _recv(self, bytes_size):
        if self._connected:
            try:
                data = self._sock.recv(bytes_size)
                if data:
                    return data
                else:
                    self._connected = False
            except OSError as e:
                print(e)
                self._connected = False

        return None

    def close(self):
        """
        关闭连接
        """
        self._sock.close()
        self._connected = False

    def login(self, user_name, passwd):
        """
        登录服务器
        :param user_name: str，帐号
        :param passwd: str，密码
        :return: boolean，True登录成功，False登录失败
        """
        with self._lock:
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
                                self._user_name = user_name
                                self._passwd = passwd
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def logout(self, user_name, passwd):
        """
        退出
        """
        with self._lock:
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def declare_queue(self, queue_name):
        """
        声明队列
        """
        with self._lock:
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def get_data_from_queue(self, queue_name):
        """
        从队列中获取数据
        """
        with self._lock:
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def send_data_to_queue(self, queue_name, message_data):
        """
        向队列中发送数据
        """
        with self._lock:
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def ack_message(self, queue_name, message_id):
        """
        消息确认
        """
        with self._lock:
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def del_queue(self, queue_name):
        """
        删除队列
        """
        with self._lock:
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def clear_queue(self, queue_name):
        """
        清空队列
        """
        with self._lock:
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def get_speed(self, queue_name):
        """
        获取队列速度
        """
        with self._lock:
            req_ack_msg = ReqGetSpeedMessage(queue_name)
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def get_stat(self):
        """
        获取统计数据
        """
        with self._lock:
            req_get_stat_msg = ReqGetStatMessage()
            req_pkg = json.dumps(req_get_stat_msg).encode()
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def delete_ack_message_id_queue_name(self, message_id, queue_name):
        """
        删除未ack的数据
        """
        with self._lock:
            req_delete_ack_message_id = ReqDeleteAckMessageIDMessage(queue_name, message_id)
            req_pkg = json.dumps(req_delete_ack_message_id).encode()
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def restore_ack_message_id(self, message_id, queue_name):
        """
        恢复ack message_id
        """
        with self._lock:
            req_restore_ack_message_id_message = ReqRestoreAckMessageIDMessage(message_id, queue_name)
            req_pkg = json.dumps(req_restore_ack_message_id_message).encode()
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False

    def restore_send_message(self, queue_name, message_data, message_id):
        """
        恢复消费者未消费的任务
        """
        with self._lock:
            restore_send_message = ReqRestoreSendMessage(queue_name, message_id, message_data)
            req_pkg = json.dumps(restore_send_message).encode()
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
                            return msg
                    else:
                        self._connected = False
                        return False
            else:
                self._connected = False
                return False