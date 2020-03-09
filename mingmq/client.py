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
                            ReqACKMessage, MAX_DATA_LENGTH, ReqPingMessage,
                            ReqGetSpeedMessage, ReqGetStatMessage,
                            ReqDeleteAckMessageIDMessage, ReqRestoreAckMessageIDMessage,
                            ReqRestoreSendMessage)
from mingmq.utils import to_json
from mingmq.error import ClientPoolEmpty

from threading import Lock


class Pool:
    _LOCK = Lock()

    logger = logging.getLogger('Pool')


    def __init__(self, host, port, user_name, passwd, size):
        self._host = host
        self._port = port
        self._user_name = user_name
        self._passwd = passwd

        self._size = size

        self._que = deque()

        self.init_pool()

    def init_pool(self):
        for i in range(self._size):
            cli = Client(self._host, self._port)
            cli.login(self._user_name, self._passwd)
            self._que.append(cli)

    def get_conn(self):
        with Pool._LOCK:
            try:
                if len(self._que) != 0:
                    conn = self._que.popleft()
                    if conn.ping() is False:
                        self.logger.debug("conn ping不通，或者为None: %s", repr(conn))
                        raise Exception("conn ping不通，或者为None")
                    return conn
                else:
                    raise ClientPoolEmpty('连接池已空')
            except ClientPoolEmpty as e:
                self.logger.error(str(e))

                self.init_pool()
                return self._que.popleft()

    def back_conn(self, conn):
        with Pool._LOCK:
            self._que.append(conn)

    def release(self):
        for conn in self._que:
            try:
                if conn: conn.close()
            except Exception:
                self.logger.error(traceback.format_exc())

        self._que.clear()

    def opera(self, method_name, *args):
        conn = None
        try:
            conn = self.get_conn()
            callback = getattr(conn, method_name)
            result = callback(*args)
            if result: return result
            self.logger.debug('返回数据：%s', repr(result))
            raise Exception('返回数据为False，或为None')
        except Exception:
            self.logger.error(traceback.format_exc())
            try:
                if conn: conn.close()
            except Exception:
                self.logger.error(traceback.format_exc())

            conn = None
        finally:
            if conn: self.back_conn(conn)


class Client:
    """
    服务器客户端
    """
    logger = logging.getLogger('Client')


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
            self.logger.debug('重连失败')
            self.logger.error(traceback.format_exc())

    def _send(self, data):
        try:
            if self._connected:
                self._sock.sendall(data)
        except Exception:
            self.logger.error(traceback.format_exc())
            self._connected = False

    def _recv(self, bytes_size):
        if self._connected:
            try:
                data = self._sock.recv(bytes_size)
                if data:
                    return data
                else:
                    self._connected = False
            except Exception:
                self.logger.error(traceback.format_exc())
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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            if msg['status'] == SUCCESS:
                                self._user_name = user_name
                                self._passwd = passwd
                            return msg
                    else:
                        self.logger.error('login数据在接收过程中出现了空字符，当前data:%s', data)
                        self._connected = False
                        return False
            else:
                self.logger.error('login在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_login_msg))
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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('logout数据在接收过程中出现了空字符，当前data:%s', data)
                        self._connected = False
                        return False
            else:
                self.logger.error('logout在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_logout_msg))
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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('declare_queue数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('declare_queue在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_declare_queue_msg))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('get_data_from_queue数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('get_data_from_queue在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_get_data_from_queue_msg))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('send_data_to_queue数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('send_data_to_queue在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(rsdfqm))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('ack_message数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('ack_message在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_ack_msg))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('del_queue数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('del_queue在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_ack_msg))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('clear_queue数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('clear_queue在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_ack_msg))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('get_speed数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('get_speed在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_ack_msg))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('get_stat数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('get_stat在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_ack_msg))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('delete_ack_message_id_queue_name数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('delete_ack_message_id_queue_name在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_delete_ack_message_id))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('restore_ack_message_id数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('restore_ack_message_id在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_restore_ack_message_id_message))

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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('restore_send_message数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('restore_send_message在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(restore_send_message))

                self._connected = False
                return False

    def ping(self):
        with self._lock:
            req_ping_message = ReqPingMessage()
            req_pkg = json.dumps(req_ping_message).encode()
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
                            self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                            return msg
                    else:
                        self.logger.error('ping数据在接收过程中出现了空字符，当前data:%s', data)

                        self._connected = False
                        return False
            else:
                self.logger.error('ping在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_ping_message))

                self._connected = False
                return False