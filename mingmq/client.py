"""为MingMQ提供一个Python的客户端驱动，并且还提供了一个连接池，但是是
基于多线程的，因为传统编程框架大多数是使用多线程的，所以是必须提供一个，
后面就再增加一个异步的客户端驱动；

总的来说，也就是提供两个类，Client和Pool:

* Client封装了对MingMQ的各种操作；
* Pool主要是不用经常登陆，可以很好的节省时间，如果是其它应用调用连接池，还能保持服务的高可用，用户根本不需要管出现的问题；
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
    """一个多线程的连接池。提供了一些方法的调用，但是用户只需要关心
    opera这个方法就够了，其它都已经封装好了；

    类成员:

    * ``_LOCK``: 连接池锁；
    * ``_logger``: 用于打印日志的对象；

    Command line example:

    >>> from mingmq.client import Pool
    >>> pool = Pool('localhost', 15673, 'mingmq', 'mm5201314', 10)
    >>> pool.opera("declare_queue", 'img')
    会有输出，但是这里先暂时没有
    >>> pool.release() # 关闭连接

    """
    _LOCK = Lock()

    _logger = logging.getLogger('Pool')


    def __init__(self, host, port, user_name, passwd, size):
        """主要的作用是初始化连接池，保存连接池的连接信息用于重连
        和重新初始化连接池；

        :param host: 服务器主机地址；
        :type host: str
        :param port: 服务器端口；
        :type port: int
        :param user_name: 用户名；
        :type user_name: str
        :param passwd: 密码；
        :type passwd: str
        :param size: 连接池大小；
        :type size: int

        """
        self._host = host
        self._port = port
        self._user_name = user_name
        self._passwd = passwd

        self._size = size

        self._que = deque()

        self._init_pool()

    def _init_pool(self):
        """初始化连接池，会创建Client对象，并且自动登陆，然后将登陆
        后的Client存放到连接池中；

        """
        for i in range(self._size):
            cli = Client(self._host, self._port)
            cli.login(self._user_name, self._passwd)
            self._que.append(cli)

    def get_conn(self):
        """从连接池中获取一个连接，然后就可以用这个连接做自己想做的事
        这个是线程安全的，因为也是必须线程安全。在获取后，会发送ping
        消息给服务端，如果服务端响应了，证明这个连接是可用的，如果未
        响应，说明这个连接失效了，或者服务器不可用了。如果连接池中没有
        连接用了，会重新初始化连接，如果其中一个连接失效了，会抛出异常
        ，但是不会重新初始化连接池。连接池空时会抛出连接池已空的异常(
        ClientPoolEmpty)，另外，循环获取连接池中的连接，如果ping不通
        ，就再循环，直到ping通就返回conn，如若连接池都空了，则初始化连
        接池，如果失败就不管了；

        :return: client连接；
        :rtype: Client

        """
        with Pool._LOCK:
            try:
                if len(self._que) != 0:
                    i = 0
                    while i < len(self._que):
                        try:
                            conn = self._que.popleft()
                            if conn.ping() is False:
                                self._logger.debug("conn ping不通，或者为None: %s", repr(conn))
                            else:
                                return conn
                        except Exception as e:
                            self._logger.error(str(e))

                        i+= 1

                    raise ClientPoolEmpty('连接池已空')
                else:
                    raise ClientPoolEmpty('连接池已空')
            except ClientPoolEmpty as e:
                self._logger.error(str(e))

                self._init_pool()
                return self._que.popleft()

    def back_conn(self, conn):
        """将连接归还给连接池，这个操作是线程安全的；

        :param conn: 从连接池中取出的连接；
        :type conn: Client

        """
        with Pool._LOCK:
            self._que.append(conn)

    def release(self):
        """释放连接池中所有连接，这个操作是线程安全的；

        """
        with Pool._LOCK:
            for conn in self._que:
                try:
                    if conn: conn.close()
                except Exception:
                    self._logger.error(traceback.format_exc())

            self._que.clear()

    def opera(self, method_name, *args):
        """用来向服务器发送请求，如果没有返回数据，证明客户端与
        服务器通信失败，所以就关闭连接，反之则归还连接到连接池；

        :param method_name: 方法名；
        :type method_name: str
        :param args: 方法名方法的参数；
        :type args: 可变参数

        :return: 方法名调用后返回的结果；
        :rtype: dict

        """
        conn = None
        try:
            conn = self.get_conn()
            callback = getattr(conn, method_name)
            result = callback(*args)
            if result: return result
            self._logger.debug('返回数据：%s', repr(result))
            raise Exception('返回数据为False，或为None')
        except Exception:
            self._logger.error(traceback.format_exc())
            try:
                if conn: conn.close()
            except Exception:
                self._logger.error(traceback.format_exc())

            conn = None
        finally:
            if conn: self.back_conn(conn)

    def all(self):
        """返回连接池中所有的连接；

        :return: 连接数组；
        :rtype: list

        """
        with Pool._LOCK: return [i for i in self._que]


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

    def send_data_to_queue(self, queue_name: str, message_data: str):
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

    def ack_message(self, queue_name: str, message_id: str):
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
                        msg = to_json(data)
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
                        msg = to_json(data)
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
                        msg = to_json(data)
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
                        msg = to_json(data)
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
                        msg = to_json(data)
                        self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                        return msg
                else:
                    self.logger.error('get_stat数据在接收过程中出现了空字符，当前data:%s', data)

                    self._connected = False
                    return False
        else:
            self.logger.error('get_stat在发送了请求之后，服务器返回了空字符，当前req_pkg:%s', repr(req_get_stat_msg))

            self._connected = False
            return False

    def delete_ack_message_id_queue_name(self, message_id, queue_name):
        """
        删除未ack的数据
        """
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
                        msg = to_json(data)
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
                        msg = to_json(data)
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
                        msg = to_json(data)
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
                        msg = to_json(data)
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