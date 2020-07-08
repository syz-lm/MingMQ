"""用于进程间通信的需求"""

import logging
import traceback
import math
import socket
import time
import json

from multiprocessing import Queue
from mingmq.db import AckProcessDB, CompletelyPersistentProcessDB
from mingmq.message import ACK_PROCESS_MESSAGE, COMPLETELY_PERSISTENT_PROCESS_MESSAGE
from mingmq.client import Client
from mingmq.message import FAIL
from mingmq.server import Server
from mingmq.client import Pool
from threading import Thread
from multiprocessing import Process
from mingmq.settings import CONFIG_FILE


class CompletelyPersistentProcess:
    _logger = logging.getLogger('CompletelyPersistentProcess')

    def __init__(
            self,
            completely_persistent_process_db_file,
            completely_persistent_process_queue: Queue,
            client_host,
            client_port,
            client_user,
            client_passwd
    ):
        self._completely_persistent_process_queue = completely_persistent_process_queue
        self._completely_persistent_process_db = CompletelyPersistentProcessDB(completely_persistent_process_db_file)

        self._client_host = client_host
        self._client_port = client_port
        self._client_user = client_user
        self._client_passwd = client_passwd

    def _test_client(self):
        client = Client(self._client_host, self._client_port)
        msg = client.login(self._client_user, self._client_passwd)

        if msg['status'] == FAIL:
            raise Exception('连接服务器失败')

        client.logout(self._client_user, self._client_passwd)
        client.close()

    def load_send_db_memory(self):
        while True:
            test_socket = None
            try:
                test_socket = socket.socket()
                test_socket.connect((self._client_host, self._client_port))
                break
            except:
                self.logger.error('正在测试与服务器的连接，连接失败，5秒后即将重试')
                time.sleep(5)
                continue
            finally:
                try:
                    if test_socket: test_socket.close()
                except:
                    self.logger.error('关闭测试套接字失败, %s', traceback.format_exc())

        self._load_send_db_memory()

    def _load_send_db_memory(self):
        try:
            self._logger.debug('正在恢复数据send。')
            pool = Pool(self._client_host, self._client_port, self._client_user, self._client_passwd, 100)

            result = self._completely_persistent_process_db.total_num()
            total_pages = 0
            if result:
                try:
                    total_pages = result[0][0]
                    total_pages = math.ceil(total_pages / 100)
                except:
                    total_pages = 0
                    self._logger.error(traceback.format_exc())

            method_name = 'restore_send_message'
            page = 1
            filter = []
            while page <= total_pages:
                try:
                    ts = []
                    rows = self._completely_persistent_process_db.pagnation_page(page)
                    if rows:
                        for row in rows:
                            message_id, queue_name, message_data, pub_date = row
                            if queue_name not in filter:
                                pool.opera('declare_queue', *(queue_name,))
                                filter.append(queue_name)

                            t = Thread(target=pool.opera, args=(method_name, *(queue_name, message_data, message_id)))
                            t.start()
                            ts.append(t)

                        for t in ts: t.join()

                    page += 1
                except Exception:
                    self._logger.error(traceback.format_exc())
        except:
            self._logger.error(traceback.format_exc())
        try:
            if pool: pool.release()
            self._logger.info('连接已释放完毕，剩余连接：%s。', repr(pool.all()))
        except Exception:
            self._logger.error(traceback.format_exc())

    def serv_forever(self):
        self._logger.debug('正在启动')
        while True:
            try:
                msg = self._completely_persistent_process_queue.get()
                self._dispatch(msg)
            except Exception:
                self._logger.error(traceback.format_exc())

    def _dispatch(self, msg):
        if 'type' not in msg:
            self._logger.error('错误_dispatch1：msg: %s', repr(msg)[:100])
            return

        _type = msg['type']

        if _type not in COMPLETELY_PERSISTENT_PROCESS_MESSAGE.values():
            self._logger.error('错误_dispatch2：msg: %s', repr(msg)[:100])
            return

        if _type == COMPLETELY_PERSISTENT_PROCESS_MESSAGE['SEND']:
            self._send(msg)
        elif _type == COMPLETELY_PERSISTENT_PROCESS_MESSAGE['GET']:
            self._get(msg)
        elif _type == COMPLETELY_PERSISTENT_PROCESS_MESSAGE['DELETE_QUEUE']:
            self._delete_queue(msg)
        else:
            self._logger.error('错误_dispatch2：msg: %s', repr(msg)[:100])

    def _send(self, msg):
        if 'queue_name' not in msg or \
                'message_data' not in msg or \
                'message_id' not in msg or \
                'pub_date' not in msg:
            self._logger.error('错误_send1：msg: %s', repr(msg)[:100])
            return

        queue_name = msg['queue_name']
        message_data = msg['message_data']
        message_id = msg['message_id']
        pub_date = msg['pub_date']

        self._completely_persistent_process_db. \
            insert_message_id_queue_name_message_data_pub_date(
            message_id,
            queue_name,
            message_data,
            pub_date
        )

    def _get(self, msg):
        if 'queue_name' not in msg or \
                'message_id' not in msg:
            self._logger.error('错误_get1：msg: %s', repr(msg)[:100])
            return

        # queue_name = msg['queue_name']
        message_id = msg['message_id']

        self._completely_persistent_process_db.delete_by_message_id(message_id)

    def _delete_queue(self, msg):
        if 'queue_name' not in msg:
            self._logger.error('错误_delete_queue1：msg: %s', repr(msg)[:100])
            return

        queue_name = msg['queue_name']

        self._completely_persistent_process_db.delete_by_queue_name(queue_name)


class MQProcess:
    def __init__(
            self,
            server_status,
            completely_persistent_process_queue: Queue,
            ack_process_queue: Queue
    ):
        self._server_status = server_status
        self._completely_persistent_process_queue = completely_persistent_process_queue
        self._ack_process_queue = ack_process_queue

    def serv_forever(self):
        self._server = Server(self._server_status,
                              self._completely_persistent_process_queue,
                              self._ack_process_queue)
        self._server.init_server_socket()
        self._server.serv_forever()

    def close(self):
        self._server.close()


class AckProcess:
    logger = logging.getLogger('AckProcess')

    def __init__(
            self,
            ack_process_db_file,
            client_host,
            client_port,
            client_user,
            client_passwd,
            ack_process_queue: Queue
    ):
        self._ack_process_db = AckProcessDB(ack_process_db_file)
        self._ack_process_db.create_table()

        self._client_host = client_host
        self._client_port = client_port
        self._client_user = client_user
        self._client_passwd = client_passwd

        self._ack_process_queue = ack_process_queue
        self._client = None

        # self._test_client()

    def load_send_db_memory(self):
        while True:
            test_socket = None
            try:
                test_socket = socket.socket()
                test_socket.connect((self._client_host, self._client_port))
                break
            except:
                self.logger.error('正在测试与服务器的连接，连接失败，5秒后即将重试')
                time.sleep(5)
                continue
            finally:
                try:
                    if test_socket: test_socket.close()
                except:
                    self.logger.error('关闭测试套接字失败, %s', traceback.format_exc())

        self._load_send_db_memory()

    def _load_send_db_memory(self):
        try:
            self.logger.debug('正在恢复数据ack。')
            pool = Pool(self._client_host, self._client_port, self._client_user, self._client_passwd, 100)

            result = self._ack_process_db.total_num()
            total_pages = 0
            try:
                total_pages = result[0][0]
                total_pages = math.ceil(total_pages / 100)
            except:
                self.logger.error(traceback.format_exc())
                total_pages = 0

            method_name = 'restore_ack_message_id'
            page = 1
            filter = []
            while page <= total_pages:
                try:
                    ts = []
                    rows = self._ack_process_db.pagnation_page(page)
                    if rows:
                        for row in rows:
                            message_id, queue_name, message_data, pub_date = row
                            if queue_name not in filter:
                                pool.opera('declare_queue', *(queue_name,))
                            t = Thread(target=pool.opera, args=(method_name, *(message_id, queue_name)))
                            t.start()
                            ts.append(t)

                        for t in ts: t.join()
                    page += 1
                except Exception:
                    self.logger.error(traceback.format_exc())
        except:
            self.logger.error(traceback.format_exc())

        try:
            if pool: pool.release()
            self.logger.info('连接已释放完毕，剩余连接：%s。', repr(pool.all()))
        except Exception:
            self.logger.error(traceback.format_exc())

    def close(self):
        if self._client:
            self._client.logout()
            self._client.close()

    def _test_client(self):
        client = Client(self._client_host, self._client_port)
        msg = client.login(self._client_user, self._client_passwd)

        if msg['status'] == FAIL:
            raise Exception('连接服务器失败')

        client.logout(self._client_user, self._client_passwd)
        client.close()

    def serv_forever(self):
        self.logger.debug('正在启动')
        while True:
            try:
                msg = self._ack_process_queue.get()
                self._dispatch(msg)
            except Exception:
                self.logger.error(traceback.format_exc())

    def _dispatch(self, msg):
        if 'type' not in msg:
            self.logger.error('错误_dispatch1：msg: %s', repr(msg)[:100])
            return

        _type = msg['type']

        if _type not in ACK_PROCESS_MESSAGE.values():
            self.logger.error('错误_dispatch2：msg: %s', repr(msg)[:100])
            return

        if _type == ACK_PROCESS_MESSAGE['GET']:
            self._get(msg)
        elif _type == ACK_PROCESS_MESSAGE['ACK']:
            self._ack(msg)
        elif _type == ACK_PROCESS_MESSAGE['ACK_RETRY']:
            self._ack_retry(msg)
        elif _type == ACK_PROCESS_MESSAGE['DELETE_QUEUE_NOACK']:
            self._delete_queue_noack(msg)
        elif _type == ACK_PROCESS_MESSAGE['DELETE_ACK_MESSAGE_ID']:
            self._delete_ack_message_id(msg)
        else:
            self.logger.error('错误_dispatch3：msg: %s', repr(msg)[:100])

    def _delete_ack_message_id(self, msg):
        if 'queue_name' not in msg or 'message_id' not in msg:
            self.logger.error('错误_delete_ack_message_id1：msg: %s', repr(msg)[:100])
            return

        # queue_name = msg['queue_name']
        message_id = msg['message_id']

        self._ack_process_db.delete_by_message_id(message_id)

    def _delete_queue_noack(self, msg):
        if 'queue_name' not in msg:
            self.logger.error('错误_get1：msg: %s', repr(msg)[:100])
            return

        queue_name = msg['queue_name']

        self._ack_process_db.delete_by_queue_name(queue_name)

    def _get(self, msg):
        if 'message_id' not in msg or \
                'queue_name' not in msg or \
                'message_data' not in msg or \
                'pub_date' not in msg:
            self.logger.error('错误_get1：msg: %s', repr(msg)[:100])
            return

        self.logger.debug('get：%s', repr(msg)[:100])

        message_id = msg['message_id']
        queue_name = msg['queue_name']
        message_data = msg['message_data']
        pub_date = msg['pub_date']

        self._ack_process_db.insert_message_id_queue_name_message_data_pub_date(
            message_id,
            queue_name,
            message_data,
            pub_date
        )

    def _ack(self, msg):
        if 'message_id' not in msg or \
                'queue_name' not in msg or \
                'pub_date' not in msg:
            # 需要QUEUE_NAME的原因是因为，需要追踪这个MESSAGE_ID是哪个QUEUE_NAME的。
            self.logger.error('错误_get1：msg1: %s', repr(msg)[:100])
            return

        self.logger.debug('ack：%s', repr(msg)[:100])

        message_id = msg['message_id']
        # queue_name = msg['queue_name']

        self._ack_process_db.delete_by_message_id(message_id)

    def _ack_retry(self, msg):
        if 'message_id' not in msg or 'pub_date' not in msg:
            self.logger.error('错误_get1：msg1: %s', repr(msg)[:100])
            return

        self.logger.debug('_ack_retry: msg: %s', repr(msg)[:100])

        # 最多重连3次
        self._client = Client(self._client_host, self._client_port)
        err = 0
        while err < 3:
            if self._client.login(self._client_user, self._client_passwd) is not False:
                break
            err += 1

        pub_date = msg['pub_date']  # int

        # 获取很久很久未确认的消息数据
        message_id_queue_name_message_data_pub_dates = self._ack_process_db.pagnation(pub_date)
        while len(message_id_queue_name_message_data_pub_dates) < 100:  # 小于100相当于是最后一页
            for message_id_queue_name_message_data_pub_date in message_id_queue_name_message_data_pub_dates:
                message_id = message_id_queue_name_message_data_pub_date[0]
                queue_name = message_id_queue_name_message_data_pub_date[1]
                message_data = message_id_queue_name_message_data_pub_date[2]
                _2pub_date = message_id_queue_name_message_data_pub_date[3]

                if not self._resend(self._client, message_id, queue_name, message_data, _2pub_date):
                    continue
                if not self._delete_ack_mm(self._client, message_id, queue_name, message_data, _2pub_date):
                    continue

                self._ack_process_db.delete_by_message_id(message_id)

            message_id_queue_name_message_data_pub_dates = self._ack_process_db.pagnation(pub_date)

    def _resend(self, client, message_id, queue_name, message_data, _2pub_date):
        # 重新长时间未确认的任务，失败则重新插入数据库。
        try:
            res_msg = client.send_data_to_queue(queue_name, message_data)
            self.logger.debug('_ack_retry: 重新发送任务: %s, %s, 服务器返回:%s',
                              repr(queue_name), repr(message_data), repr(res_msg)[:100])
            if res_msg['status'] == FAIL:  # 如果发送失败，重新放到数据库
                return False
            return True
        except:
            return False

    def _delete_ack_mm(self, client, message_id, queue_name, message_data, _2pub_date):
        # 如果删除mmserver中未确认的内存失败，如果不进行处理，可能会导致mmserver服务器内存溢出。
        # 如果仅仅是通过日志去排查，并进行删除，很有可能会导致其它更害怕出现的问题。
        # TODO
        try:
            res_msg = client.delete_ack_message_id_queue_name(message_id, queue_name)
            self.logger.debug('_ack_retry: 删除mmserver中的未确认ack内存: %s，%s, mmserver返回:%s',
                              repr(message_id), repr(queue_name), repr(res_msg)[:100])
            if res_msg['status'] == FAIL:  # 如果发送失败，我想我还是写log文件吧。
                # TODO
                return False
            return True
        except:
            return False


class NoAckProcess:
    """这个进程用于隔一段时间将未确认的任务重新推送到队列。

    """
    logger = logging.getLogger('NoAckProcess')

    def __init__(
            self,
            ack_process_db_file,
            client_host,
            client_port,
            client_user,
            client_passwd
    ):
        self._ack_process_db: AckProcessDB = AckProcessDB(ack_process_db_file)
        self._ack_process_db.create_table()

        self._client_host = client_host
        self._client_port = client_port
        self._client_user = client_user
        self._client_passwd = client_passwd

    def _read_resend_interval(self):
        f = open(CONFIG_FILE, 'r')
        config = json.load(f)
        f.close()
        return config['RESEND_INTERVAL']

    def _sleep(self, resend_interval):
        time.sleep(resend_interval)

    def _delete_row_ack_db(self, message_id):
        return self._ack_process_db.delete_by_message_id(message_id)

    def _task(self, queue_name, message_data, message_id, pool):
        conn: Client = None
        try:
            conn = pool.get_conn()

            self.logger.debug('即将重新推送到队列的数据为: %s', (queue_name, message_data, message_id))
            res = conn.send_data_to_queue(queue_name, message_data)
            if res and res['status'] != FAIL:
                self.logger.debug('重新很久未ack的数据推送到队列成功')
                res = conn.ack_message(queue_name, message_id)
                if res and res['status'] != FAIL:
                    if self._delete_row_ack_db(message_id) < 0:
                        self.logger.error('删除ack_db失败，message_id: %s', message_id)
                else:
                    self.logger.debug('消息重新推送陈宫，但是，统计内存中的未确认数没有递减成功')
            else:
                self.logger.debug('消息重新推送失败, %s，可能是因为mq内存刚刚启动，请等待一下下一次检查', res)
        except:
            self.logger.error(traceback.format_exc())
        finally:
            if conn: pool.back_conn(conn)

    def serv_forever(self):
        while True:
            resend_interval = self._read_resend_interval()
            self._sleep(resend_interval)

            pool = None
            rows_len = 0
            try:
                pool = Pool(self._client_host, self._client_port, self._client_user, self._client_passwd, 100)

                current_timestamp = int(time.time())
                by_pub_date = current_timestamp - resend_interval

                tasks = []
                rows = self._ack_process_db.pagnation(by_pub_date)
                rows_len = len(rows)
                for row in rows:
                    message_id, queue_name, message_data, pub_date = row
                    task = Thread(target=self._task,
                                  args=(queue_name, message_data, message_id, pool))
                    tasks.append(task)
                    task.start()

                for task in tasks:
                    task.join()
            except:
                self.logger.error(traceback.format_exc())
            finally:
                if rows_len < 100:
                    self._sleep(resend_interval)
                    try:
                        if pool: pool.release()
                    except:
                        self.logger.error(traceback.format_exc())