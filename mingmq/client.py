"""为MingMQ提供一个Python的客户端驱动，并且还提供了一个连接池，但是是
基于多线程的，因为传统编程框架大多数是使用多线程的，所以是必须提供一个，
后面就再增加一个异步的客户端驱动；

* Producer生产者用于向消息队列中发送任务
* ConsumerTaskThread用于消费者任务线程处理
* Consumer消费者用于从任务消息队列中获取任务再将处理后的数据放入存储队列的应用中
* Client封装了对MingMQ的各种操作；
* Pool主要是不用经常登陆，可以很好的节省时间，如果是其它应用调用连接池，还能保持服务的高可用，用户根本不需要管出现的问题
"""

import time
import sys
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
                            ReqRestoreSendMessage, FAIL)
from mingmq.utils import to_json
from mingmq.error import ClientPoolEmpty

from threading import Lock, Thread


class Producer(object):
    """生产者，用于发送消息到指定队列
    """

    def __init__(self, host, port, user_name, passwd, task_queue_name):
        self._host = host
        self._port = port
        self._user_name = user_name
        self._passwd = passwd
        self._task_queue_name = task_queue_name
        self._log = logging.getLogger('Producer')
        self._init_mingmq_conn()
        self._mingmq_conn.declare_queue(self._task_queue_name)

    def _init_mingmq_conn(self):
        """初始化mingmq连接
        """
        self._mingmq_conn = Client(self._host, self._port)
        if self._mingmq_conn.login(self._user_name, self._passwd) != SUCCESS:
            raise Exception('登录失败')

    def send_task(self, task_data):
        """发送数据到消息队列中

        :param task_data: 数据
                例子:
                self._mingmq.opera('send_data_to_queue', *(MINGMQ_CONFIG['get_article_category']['queue_name'], json.dumps({
                    "category_id": 40
                })))
        """
        json_obj = json.dumps(task_data)
        result = self._mingmq_conn.send_data_to_queue(self._task_queue_name, json_obj)
        if result is None or result and result['status'] != SUCCESS:
            raise Exception('发送任务到消息队列中失败！')
        self._log.debug('发送数据到消息队列中成功: queue=%s, data=%s', self._task_queue_name, task_data)

    def release(self):
        self._mingmq_conn.close()


class ConsumerTaskThread(Thread):
    """消费者线程任务处理类
    """

    def __init__(self, target, args, consumer) -> None:
        """初始化

        :param target: 回调函数
        :type target: callable function
        :param args: 函数参数
        :type args: tuple
        :param consumer: 消费者
        :type consumer: Consumer
        """
        Thread.__init__(self, target=target, args=args)
        self.args = args
        self._consumer = consumer
        self._log = logging.getLogger(self.getName())

    def run(self) -> None:
        # 线程执行之前，消费者使用的连接自增
        with self._consumer.lock:
            self._consumer.used_conn_num += 1
            self._log.debug('消费者正在运行的任务数递增：%d', self._consumer.used_conn_num)

        try:
            # 实际的回调函数执行
            super().run()
            # 确认任务
            message_id = self.args[1]
            msg = self._consumer.mingmq_pool.opera('ack_message', *(self._consumer.task_queue_name, message_id))
            if msg and msg['status'] == SUCCESS:
                self._log.debug('确认消息成功: %s, %s', self._consumer.task_queue_name, message_id)
            else:
                self._log.debug('确认消息失败: %s, %s', self._consumer.task_queue_name, message_id)
        except:
            self._consumer.log.error(traceback.format_exc())

        # 执行完毕后，消费者的连接递减
        with self._consumer.lock:
            self._consumer.used_conn_num -= 1
            self._log.debug('消费者正在运行的任务数递减：%d', self._consumer.used_conn_num)


class Consumer(object):
    """适用于从队列中获取任务处理后将数据再放到另一个队列中存储的应用
    """

    def __init__(self, host, port, user_name, passwd, size, task_queue_name, data_queue_name):
        """初始化消费者

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
        :param task_queue_name: 任务队列名
        :type task_queue_name: str
        :param data_queue_name: 存储队列名
        :type data_queue_name: str
        """
        self._host = host
        self._port = port
        self._user_name = user_name
        self._passwd = passwd
        self._size = size + 1
        self._task_queue_name = task_queue_name
        self._data_queue_name = data_queue_name

        self._init_mingmq_pool()
        self._declare_queue()

        self._lock = Lock()
        self._used_conn_num = 0

        self._log = logging.getLogger('Consumer')

    @property
    def used_conn_num(self):
        return self._used_conn_num

    @used_conn_num.setter
    def used_conn_num(self, used_conn_num):
        self._used_conn_num = used_conn_num

    @property
    def lock(self):
        return self._lock

    @property
    def task_queue_name(self):
        return self._task_queue_name

    @property
    def mingmq_pool(self):
        return self._mingmq_pool

    @property
    def data_queue_name(self):
        return self._data_queue_name

    @property
    def log(self):
        return self._log

    def _init_mingmq_pool(self):
        """初始化mingmq连接池
        """
        self._mingmq_pool = Pool(self._host, self._port, self._user_name, self._passwd, self._size)

    def _declare_queue(self):
        """声明队列名
        """
        self._mingmq_pool.opera('declare_queue', *(self._task_queue_name,))
        self._mingmq_pool.opera('declare_queue', *(self._data_queue_name,))

    def serv_forever(self, func):
        """从队列中获取任务

        :param func: 回调函数
        :type func:

        XX: 回调函数例子
        ```
        def callback(message_data, message_id):
            print(message_data, message_id)
        ```
        """
        while True:
            self._log.debug('正在运行的任务数：%d', self.used_conn_num)

            if self.used_conn_num < self._size - 1:
                try:
                    mq_res: dict = self._mingmq_pool.opera('get_data_from_queue', *(self._task_queue_name,))
                    if mq_res and mq_res['status'] == FAIL:
                        raise Exception("任务队列中没有任务")
                    if mq_res is None:
                        self._log.error("服务器内部错误")
                        sys.exit(1)
                    self._log.debug('从消息队列中获取的消息为: %s', mq_res)
                except Exception as e:
                    self._log.debug('XX: 从消息队列中获取任务失败，错误信息: %s', str(e))
                    continue
                try:
                    message_data = mq_res['json_obj'][0]['message_data']
                    message_id = mq_res['json_obj'][0]['message_id']
                    ConsumerTaskThread(target=func, args=(message_data, message_id), consumer=self).start()
                except Exception as e:
                    self._log.debug("XX: 线程在执行过程中出现异常，错误信息为: %s", str(e))
            else:
                time.sleep(3)

    def _release_mingmq_pool(self):
        self._mingmq_pool.release()


class Pool(object):
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
            if cli.login(self._user_name, self._passwd) == SUCCESS:
                self._que.append(cli)
            else:
                self._logger.debug('登录失败')

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

                        i += 1

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


class Client(object):
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

        msg = self._recv_surplus(recv_header)
        if msg and msg['status'] == SUCCESS:
            self._user_name = user_name
            self._passwd = passwd

            return SUCCESS
        return FAIL

    def _recv_surplus(self, recv_header):
        if recv_header:
            data_size, = struct.unpack('!i', recv_header)

            should_read = min(data_size, MAX_DATA_LENGTH)

            data = b''
            while self._connected and len(data) < data_size:
                buf = self._recv(should_read)
                if buf:
                    data += buf
                    should_read -= len(buf)
                else:
                    self.logger.error('数据在接收过程中出现了空字符，当前data:%s', data)
                    self._connected = False
                    return False

            if len(data) == data_size:
                msg = to_json(data)
                self.logger.debug('服务器发送过来的消息[%s]。', repr(msg))
                return msg
            else:
                self.logger.error('数据在接收过程中没有接收完毕，应接收数据长度: %d，实际接收数据长度：%d', data_size, len(data))
                self._connected = False
                return False
        else:
            self.logger.error('在发送了请求之后，服务器返回了空字符')
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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

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
        return self._recv_surplus(recv_header)

    def ping(self):
        req_ping_message = ReqPingMessage()
        req_pkg = json.dumps(req_ping_message).encode()
        send_header = struct.pack('!i', len(req_pkg))
        self._send(send_header + req_pkg)

        # 接收数据
        recv_header = self._recv(4)
        return self._recv_surplus(recv_header)