""" 与客户端进行通信的模块 """

import json
import logging
import math
import platform
import socket
import struct
import time
from multiprocessing import Queue

if platform.platform().startswith('Linux'):
    from mingmq.memory import QueueMemory, TaskAckMemory
else:
    from mingmq.memory import SyncQueueMemory as QueueMemory, SyncTaskAckMemory as TaskAckMemory

from mingmq.memory import StatMemory

from mingmq.message import (ResMessage, SUCCESS, FAIL, MESSAGE_TYPE, Task,
                            MAX_DATA_LENGTH, GET, SEND, ACK, PipeAckProcessGetMessage,
                            PipeAckProcessAckMessage, PipeDeleteQueueNoackMessage,
                            PipeDeleteAckMessageID, PipeCompletelyPersistentProcessSendMessage,
                            PipeCompletelyPersistentProcessGetMessage, PipeCompletelyPersistentProcessDeleteQueueMessage)
from mingmq.utils import to_json, check_msg
from mingmq.status import ServerStatus


class Handler:
    def __init__(
            self,
            sock: socket.socket,
            addr: str,
            queue_memory: QueueMemory,
            task_ack_memory: TaskAckMemory,
            stat_memory: StatMemory,
            server_status: ServerStatus,
            completely_persistent_process_queue: Queue,
            ack_process_queue: Queue
    ):
        self._sock = sock
        self._addr = addr
        self._queue_memory = queue_memory
        self._stat_memory = stat_memory
        self._task_ack_memory = task_ack_memory
        self._session_id = None
        self._connected = True
        self.server_status = server_status
        self._completely_persistent_process_queue = completely_persistent_process_queue
        self._ack_process_queue = ack_process_queue

        self._buf: bytes = b''
        self._should_read = 0
        self._read_size = 0

        self._ok = False

    def is_connected(self):
        return self._connected

    def fileno(self):
        return self._sock.fileno()

    def close(self):
        self._sock.close()
        logging.info('客户端[IP %s]已断开。', repr(self._addr))

    def _recv(self, size):
        data = None
        try:
            data = self._sock.recv(size)
        except (ConnectionResetError, OSError) as err:
            # OSError: [WinError 10038] 在一个非套接字上尝试了一个操作。
            # ConnectionResetError 远程主机主动断开了连接
            logging.info(err)

        return data

    def _handle_read(self):
        if self._should_read == 0:
            try:
                self._header = self._recv(4)
                if self._header:
                    self._read_size, = struct.unpack('!i', self._header)
                    self._should_read = min(self._read_size, MAX_DATA_LENGTH)
                    self._read_count = 0
                else:
                    self._connected = False
            except:
                self._connected = False
        else:
            buf = self._recv(self._should_read)
            if buf:
                self._buf += buf

                if len(self._buf) == self._read_size:
                    self._should_read = 0
                    self._ok = True
            else:
                self._connected = False

    def _handle_write(self):
        if self._connected and self._ok:
            self._deal_message(self._buf)
            self._ok = False
            self._buf = b''

    def handle_epoll_mode_read(self):
        self._handle_read()

    def handle_epoll_mode_write(self):
        self._handle_write()

    def handle_thread_mode_read(self):
        while self.is_connected():
            self._handle_read()
            self._handle_write()

    def _deal_message(self, buf):
        msg = to_json(buf)

        if msg is False:
            res_msg = ResMessage(MESSAGE_TYPE['DATA_WRONG'], FAIL, [])
            res_pkg = json.dumps(res_msg).encode()
            self._send_data(res_pkg)
            self._connected = False
            return

        logging.info('客户端[IP %s]发来数据转换成JSON对象[%s]。', repr(self._addr), repr(msg)[:100])

        if msg is not False:  # 如果msg为False则断开连接
            if check_msg(msg) is not False:
                if self._has_loggin() is not False:
                    self._dispatch_request(msg)
                else:
                    self._login(msg)
            else:
                self._connected = False
        else:
            self._connected = False

    def _dispatch_request(self, msg):
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
        elif _type == MESSAGE_TYPE['GET_SPEED']:
            self._get_speed(msg)
        elif _type == MESSAGE_TYPE['GET_STAT']:
            self._get_stat()
        elif _type == MESSAGE_TYPE['DELETE_QUEUE']:
            self._delete_queue(msg)
        elif _type == MESSAGE_TYPE['CLEAR_QUEUE']:
            self._clear_queue(msg)
        elif _type == MESSAGE_TYPE['DELETE_ACK_MESSAGE_ID']:
            self._delete_ack_message_id_queue_name(msg)
        elif _type == MESSAGE_TYPE['RESTORE_ACK_MESSAGE_ID']:
            self._restore_ack_message_id(msg)
        else:
            self._not_found(msg)

    def _restore_ack_message_id(self, msg):
        if self._data_wrong('_restore_ack_message_id', ('message_id', 'queue_name'), msg) is not False:
            queue_name = msg['queue_name']
            message_id = msg['message_id']

            if self._task_ack_memory.put(queue_name, message_id):
                res_msg = ResMessage(MESSAGE_TYPE['RESTORE_ACK_MESSAGE_ID'], SUCCESS, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(MESSAGE_TYPE['RESTORE_ACK_MESSAGE_ID'], FAIL, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)

    def _delete_ack_message_id_queue_name(self, msg):
        if self._data_wrong('_delete_ack_message_id', ('message_id', 'queue_name'), msg) is not False:
            queue_name = msg['queue_name']
            message_id = msg['message_id']

            if self._task_ack_memory.get(queue_name, message_id):

                pdam = PipeDeleteAckMessageID(queue_name, message_id)
                self._ack_process_queue.put_nowait(pdam)

                res_msg = ResMessage(MESSAGE_TYPE['DELETE_ACK_MESSAGE_ID'], SUCCESS, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(MESSAGE_TYPE['DELETE_ACK_MESSAGE_ID'], FAIL, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)

    def _get_stat(self):
        res_msg = ResMessage(MESSAGE_TYPE['GET_SPEED'], SUCCESS, [{
            'queue_infor': self._queue_memory.get_stat(),
            'speed_infor': self._stat_memory.get_stat(),
            'task_ack_infor': self._task_ack_memory.get_stat()
        }])
        res_pkg = json.dumps(res_msg).encode()
        self._send_data(res_pkg)

    def _get_speed(self, msg):
        if self._data_wrong('_get_speed', ('queue_name',), msg) is not False:
            queue_name = msg['queue_name']

            send_stat_var = 'send_' + queue_name
            get_stat_var = 'get_' + queue_name
            ack_stat_var = 'ack_' + queue_name

            send_speed = self._stat_memory.get(send_stat_var)
            get_speed = self._stat_memory.get(get_stat_var)
            ack_speed = self._stat_memory.get(ack_stat_var)

            res_msg = ResMessage(MESSAGE_TYPE['GET_SPEED'], SUCCESS, [{
                'send_speed': send_speed,
                'get_speed': get_speed,
                'ack_speed': ack_speed
            }])
            res_pkg = json.dumps(res_msg).encode()
            self._send_data(res_pkg)

    def _data_wrong(self, opera, args, msg):
        err = 0
        for arg in args:
            if arg not in msg:
                err += 1
                break

        if err > 0:
            logging.info('%s, 参数错误 %s, 需要参数 %s', opera, msg[:100], args)

            res_msg = ResMessage(MESSAGE_TYPE['DATA_WRONG'], FAIL, [])
            res_pkg = json.dumps(res_msg).encode()
            self._send_data(res_pkg)
            return False
        return True

    def _ack_message(self, msg):
        try:
            if self._data_wrong('_ack_message', ('queue_name', 'message_id'), msg) is not False:
                queue_name = msg['queue_name']
                message_id = msg['message_id']
                if self._task_ack_memory.get(queue_name, message_id):
                    papam = PipeAckProcessAckMessage(message_id, queue_name)
                    self._ack_process_queue.put_nowait(papam)

                    res_msg = ResMessage(MESSAGE_TYPE['ACK_MESSAGE'], SUCCESS, [])
                    res_pkg = json.dumps(res_msg).encode()
                    self._send_data(res_pkg)
                else:
                    res_msg = ResMessage(MESSAGE_TYPE['ACK_MESSAGE'], FAIL, [])
                    res_pkg = json.dumps(res_msg).encode()
                    self._send_data(res_pkg)
        finally:
            self._stat(ACK, queue_name)

    def _send_data_to_queue(self, msg):
        try:
            if self._data_wrong('_send_data_to_queue', ('queue_name', 'message_data'), msg) is not False:
                queue_name = msg['queue_name']
                message_data = msg['message_data']
                task = Task(message_data)
                if self._queue_memory.put(queue_name, task):

                    pcppsm = PipeCompletelyPersistentProcessSendMessage(queue_name, message_data, task.message_id)
                    self._completely_persistent_process_queue.put_nowait(pcppsm)

                    res_msg = ResMessage(MESSAGE_TYPE['SEND_DATA_TO_QUEUE'], SUCCESS, [])
                    res_pkg = json.dumps(res_msg).encode()
                    self._send_data(res_pkg)
                else:
                    res_msg = ResMessage(MESSAGE_TYPE['SEND_DATA_TO_QUEUE'], FAIL, [])
                    res_pkg = json.dumps(res_msg).encode()
                    self._send_data(res_pkg)
        finally:
            self._stat(SEND, queue_name)

    def _get_data_from_queue(self, msg):
        try:
            if self._data_wrong('_get_data_from_queue', ('queue_name',), msg) is not False:
                queue_name = msg['queue_name']
                task = self._queue_memory.get(queue_name)

                if task is not None and \
                        self._task_ack_memory.put(queue_name, task.message_id):

                    papgm = PipeAckProcessGetMessage(task.message_id, queue_name, task.message_data)
                    self._ack_process_queue.put_nowait(papgm)

                    pcppgm = PipeCompletelyPersistentProcessGetMessage(queue_name, task.message_id)
                    self._completely_persistent_process_queue.put_nowait(pcppgm)

                    res_msg = ResMessage(MESSAGE_TYPE['GET_DATA_FROM_QUEUE'], SUCCESS, [task])
                    res_pkg = json.dumps(res_msg).encode()
                    self._send_data(res_pkg)
                else:
                    res_msg = ResMessage(MESSAGE_TYPE['GET_DATA_FROM_QUEUE'], FAIL, [task])
                    res_pkg = json.dumps(res_msg).encode()
                    self._send_data(res_pkg)
        finally:
            self._stat(GET, queue_name)

    def _stat(self, action, queue_name):
        stat_var = None
        if action == SEND:
            stat_var = 'send_' + queue_name

        elif action == GET:
            stat_var = 'get_' + queue_name

        elif action == ACK:
            stat_var = 'ack_' + queue_name

        self._stat_memory.set(stat_var, 1)

        now = time.time()

        last_time = self._stat_memory.get_last_time()
        t = now - last_time
        if t > 10:
            n = self._stat_memory.get(stat_var)
            self._stat_memory.set_speed_per_second(stat_var, math.ceil(n / t))
            self._stat_memory.set_last_time(now)

    def _declare_queue(self, msg):
        if self._data_wrong('_declare_queue', ('queue_name',), msg) is not False:
            queue_name = msg['queue_name']
            if self._queue_memory.decleare(queue_name) and \
                    self._task_ack_memory.declare(queue_name) and \
                    self._stat_memory.declare('send_' + queue_name) and \
                    self._stat_memory.declare('get_' + queue_name) and \
                    self._stat_memory.declare('ack_' + queue_name):
                res_msg = ResMessage(MESSAGE_TYPE['DECLARE_QUEUE'], SUCCESS, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(MESSAGE_TYPE['DECLARE_QUEUE'], FAIL, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)

    def _not_found(self, msg):
        res_msg = ResMessage(MESSAGE_TYPE['NOT_FOUND'], FAIL, [msg])
        res_pkg = json.dumps(res_msg).encode()
        self._send_data(res_pkg)

    def _delete_queue(self, msg):
        if self._data_wrong('_declare_queue', ('queue_name',), msg) is not False:
            queue_name = msg['queue_name']
            if self._queue_memory.delete(queue_name) and \
                    self._task_ack_memory.delete(queue_name) and \
                    self._stat_memory.delete('send_' + queue_name) and \
                    self._stat_memory.delete('get_' + queue_name) and \
                    self._stat_memory.delete('ack_' + queue_name):

                pcppdqm = PipeCompletelyPersistentProcessDeleteQueueMessage(queue_name)
                self._completely_persistent_process_queue.put_nowait(pcppdqm)

                pdqnm = PipeDeleteQueueNoackMessage(queue_name)
                self._ack_process_queue.put_nowait(pdqnm)

                res_msg = ResMessage(MESSAGE_TYPE['DECLARE_QUEUE'], SUCCESS, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(MESSAGE_TYPE['DECLARE_QUEUE'], FAIL, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)

    def _clear_queue(self, msg):
        if self._data_wrong('_clear_queue', ('queue_name',), msg) is not False:
            queue_name = msg['queue_name']
            if self._queue_memory.clear(queue_name) and \
                    self._task_ack_memory.clear(queue_name):
                res_msg = ResMessage(MESSAGE_TYPE['DECLARE_QUEUE'], SUCCESS, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(MESSAGE_TYPE['DECLARE_QUEUE'], FAIL, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)

    def _login(self, msg):
        if self._data_wrong('_login', ('user_name', 'passwd'), msg) is not False:
            user_name = msg['user_name']
            passwd = msg['passwd']

            if self.server_status.get_user_name() != user_name or \
                    self.server_status.get_passwd() != passwd:
                res_msg = ResMessage(MESSAGE_TYPE['LOGIN'], FAIL, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)
                self._connected = False
            else:
                self._session_id = str(self) + ':' + repr(self._addr) + ':' + user_name + '/' + passwd
                res_msg = ResMessage(MESSAGE_TYPE['LOGIN'], SUCCESS, [])
                self._send_data(json.dumps(res_msg).encode())
        else:
            self._connected = False

    def _logout(self, msg):
        # type = msg['type']
        if self._data_wrong('_logout', ('user_name', 'passwd'), msg) is not False:
            user_name = msg['user_name']
            passwd = msg['passwd']
            if self.server_status.get_user_name() != user_name or \
                    self.server_status.get_passwd() != passwd:
                res_msg = ResMessage(MESSAGE_TYPE['LOGOUT'], FAIL, [])
                res_pkg = json.dumps(res_msg).encode()
                self._send_data(res_pkg)
            else:
                res_msg = ResMessage(MESSAGE_TYPE['LOGOUT'], SUCCESS, [])
                self._send_data(json.dumps(res_msg).encode())

                self._connected = False

    def _send_data(self, data):
        if self._connected:
            header = struct.pack('!i', len(data))
            logging.info('发送给客户端[%s]的消息为: %s', self._addr, str(header + data)[:100])
            self._sock.sendall(header + data)

    def _has_loggin(self):
        if self._session_id is not None: return True
        return False