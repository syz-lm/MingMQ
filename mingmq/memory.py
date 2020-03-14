"""
以下代码片段是我舍不得删的代码，tcp数据分组算法(较差，能用)::

    def package_message(data):
        res_pkg = MessageWindow.MESSAGE_BEGIN + str_to_hex(data) + MessageWindow.MESSAGE_END
        return res_pkg.encode()


    class MessageWindow:
        # 这段代码，我想了3天，还进行优化过，我真的舍不得删掉。
        # 虽然，我通过pymysql源码找到了更好的算法。


        # 数据包开始
        MESSAGE_BEGIN = 'K'
        # 数据包结束
        MESSAGE_END = 'J'

        def __init__(self):
            self._current_buffer = StringIO()
            self._message_window = Queue()

        def grouping_message(self, data):
            # 对客户端发送来的数据组装，并分组
            start_count = -1
            end_count = -1

            if len(data) > 0:
                try:
                    start_count = data.index(MessageWindow.MESSAGE_BEGIN)
                except ValueError:
                    pass
                try:
                    end_count = data.index(MessageWindow.MESSAGE_END)
                except ValueError:
                    pass

                # `aaaa`
                if start_count == -1 and end_count == -1:
                    self._current_buffer.write(data)

                # 正常数据：`Kaaa', 错误数据：`aaKaa'
                elif start_count != -1 and end_count == -1:
                    if start_count != 0:
                        err_data = data[:start_count]
                        logging.info('客户端发送的数据可能被网络中被篡改：%s', err_data)

                    self._current_buffer.write(data[start_count + 1:])

                # `Jaa`, `aaJaa`, `aaj`
                elif start_count == -1 and end_count != -1:
                    self._current_buffer.write(data[:end_count])
                    self._message_window.put_nowait(self._current_buffer.getvalue())
                    self._current_buffer.close()
                    self._current_buffer = StringIO()
                    # self.current_buffer.truncate(0)
                    # self.current_buffer.write(data[end_count + 1:])

                # `KaaJ`, `KaaJaa`, `aaKaaJaa`, `aaKaaJ`, `KaaJKaaJ`, `JaaK`, `JKaa`
                elif start_count != -1 and end_count != -1:
                    if start_count < end_count:
                        self._current_buffer.truncate(0)
                        self._current_buffer.write(data[start_count + 1: end_count])
                        self._message_window.put_nowait(self._current_buffer.getvalue())
                        self._current_buffer.close()
                        self._current_buffer = StringIO()
                        self.grouping_message(data[end_count + 1:])
                    else:
                        self._current_buffer.write(data[:end_count])
                        self._message_window.put_nowait(self._current_buffer.getvalue())
                        self._current_buffer.close()
                        self._current_buffer = StringIO()
                        self.grouping_message(data[start_count + 1:])

        def loop_message_window(self):
            while self._message_window.empty() is False:
                yield self._message_window.get_nowait()

        def finished(self):
            return self._message_window.qsize() > 0

"""

import math
import platform
import time
from queue import Queue
from mingmq.utils import get_size

if not platform.platform().startswith('Linux'):
    from threading import Lock

    _LOCK = Lock()


class QueueMemory:
    """
    队列的内存模型
    """

    def __init__(self):
        self._map = dict()

    def get_self(self):
        return self._map

    def decleare(self, queue_name):
        """
        声明一个队列
        :param queue_name: str，队列名称
        :return: boolean，True成功，False失败
        """
        if queue_name not in self._map:
            self._map[queue_name] = Queue()
            return True
        return False

    def clear(self, queue_name):
        """
        清空一个队列
        :param queue_name: str，队列名称
        :return: boolean，True成功，False失败
        """
        if queue_name in self._map:
            del self._map[queue_name]
            self._map[queue_name] = Queue()
            return True
        return False

    def delete(self, queue_name):
        """
        删除一个队列
        :param queue_name: str，队列名称
        :return: boolean，True成功，False失败
        """
        if queue_name in self._map:
            del self._map[queue_name]
            return True
        return False

    def put(self, queue_name, message):
        """
        向队指定的队列中发布任务
        :param queue_name: str，队列名
        :param message: Task，消息
        :return: boolean，True成功，False失败
        """
        if queue_name in self._map:
            self._map[queue_name].put_nowait(message)
            return True
        return False

    def get(self, queue_name):
        """
        从指定的队列中获取任务
        :param queue_name: str，队列名
        :return: str，None则表示没有获取到数据
        """
        if queue_name in self._map and self._map[queue_name].qsize() != 0:
            return self._map[queue_name].get_nowait()
        return None

    def get_stat(self):
        tmp = dict()
        for k, v in self._map.items():
            tmp[k] = [v.qsize(), get_size(v)]

        return tmp


class TaskAckMemory:
    """
    消息应答的内存模型

    用于存放未应答的消息id
    """

    def __init__(self):
        self._map = dict()

    def get_self(self):
        return self._map

    def declare(self, set_name):
        """
        :param set_name: str，队列名
        :return: boolean，True成功，False失败
        """
        if set_name not in self._map:
            self._map[set_name] = set()
            return True
        return False

    def clear(self, set_name):
        """
        :param set_name: str，队列名
        :return: boolean，True成功，False失败
        """
        if set_name in self._map:
            del self._map[set_name]
            self._map[set_name] = set()
            return True
        return False

    def delete(self, set_name):
        """
        :param set_name: str，队列名
        :return: boolean，True成功，False失败
        """
        if set_name in self._map:
            del self._map[set_name]
            return True
        return False

    def put(self, queue_name, message_id):
        """
        :param queue_name: str，队列名
        :param message_id: str，消息id
        :return: bookean，True成功，False失败
        """
        if queue_name in self._map:
            self._map[queue_name].add(message_id)
            return True
        return False

    def get(self, queue_name, message_id):
        """
        :param queue_name: str，队列名称
        :param message_id: str，消息id
        :return: boolean，True成功 , False表示失败
        """
        if queue_name in self._map and len(self._map[queue_name]) != 0:
            try:
                self._map[queue_name].remove(message_id)
                return True
            except KeyError:
                return False
        return False

    def get_stat(self):
        tmp = dict()
        for k, v in self._map.items():
            tmp[k] = [len(v), get_size(v)]

        return tmp


class SyncQueueMemory:
    """
    队列的内存模型
    """

    def __init__(self):
        self._map = dict()

    def get_self(self):
        return self._map

    def decleare(self, queue_name):
        """
        声明一个队列
        :param queue_name: str，队列名称
        :return: boolean，True成功，False失败
        """
        with _LOCK:
            if queue_name not in self._map:
                self._map[queue_name] = Queue()
                return True
            return False

    def clear(self, queue_name):
        """
        清空一个队列
        :param queue_name: str，队列名称
        :return: boolean，True成功，False失败
        """
        with _LOCK:
            if queue_name in self._map:
                del self._map[queue_name]
                self._map[queue_name] = Queue()
                return True
            return False

    def delete(self, queue_name):
        """
        删除一个队列
        :param queue_name: str，队列名称
        :return: boolean，True成功，False失败
        """
        with _LOCK:
            if queue_name in self._map:
                del self._map[queue_name]
                return True
            return False

    def put(self, queue_name, message):
        """
        向队指定的队列中发布任务
        :param queue_name: str，队列名
        :param message: str，消息
        :return: boolean，True成功，False失败
        """
        with _LOCK:
            if queue_name in self._map:
                self._map[queue_name].put_nowait(message)
                return True
            return False

    def get(self, queue_name):
        """
        从指定的队列中获取任务
        :param queue_name: str，队列名
        :return: str，None则表示没有获取到数据
        """
        with _LOCK:
            if queue_name in self._map and self._map[queue_name].qsize() != 0:
                return self._map[queue_name].get_nowait()
            return None

    def get_stat(self):
        with _LOCK:
            tmp = dict()
            for k, v in self._map.items():
                tmp[k] = [v.qsize(), get_size(v)]

            return tmp


class SyncTaskAckMemory:
    """
    队列消息应答的内存模型

    用于存放未应答的消息id
    """

    def __init__(self):
        self._map = dict()

    def get_self(self):
        return self._map

    def declare(self, queue_name):
        """
        声明一个队列
        :param queue_name: str，队列名
        :return: boolean，True成功，False失败
        """
        with _LOCK:
            if queue_name not in self._map:
                self._map[queue_name] = set()
                return True
            return False

    def put(self, queue_name, message_id):
        """
        向指定队列中增加一条消息
        :param queue_name: str，队列名
        :param message_id: str，消息id
        :return: boolean，True成功，False失败
        """
        with _LOCK:
            if queue_name in self._map:
                self._map[queue_name].add(message_id)
                return True
            return False

    def get(self, queue_name, message_id):
        """
        从指定队列中获取一条消息id
        :param queue_name: str，队列名称
        :param message_id: str，消息id
        :return: any，消息id , False表示失败
        """
        with _LOCK:
            if queue_name in self._map and len(self._map[queue_name]) != 0:
                try:
                    self._map[queue_name].remove(message_id)
                    return True
                except KeyError:
                    return False
            return False

    def clear(self, queue_name):
        """
        :param queue_name: str，队列名称
        :return: boolean，True成功，False失败
        """
        with _LOCK:
            if queue_name in self._map:
                del self._map[queue_name]
                self._map[queue_name] = set()
                return True
            return False

    def delete(self, queue_name):
        """
        :param queue_name: str，队列名称
        :return: boolean，True成功，False失败
        """
        with _LOCK:
            if queue_name in self._map:
                del self._map[queue_name]
                return True
            return False

    def get_stat(self):
        with _LOCK:
            tmp = dict()
            for k, v in self._map.items():
                tmp[k] = [len(v), get_size(v)]

            return tmp


class StatMemory:
    '''
    队列的send/get/ack速度统计
    '''

    def __init__(self):
        self._map = dict()
        self._speed = dict()
        self._last_time = time.time()

    def get_stat(self):
        if math.ceil((time.time() - self._last_time) / 10) > 2:
            for k in self._speed.keys():
                self._speed[k] = 0
        return self._speed

    def set_speed_per_second(self, key, n):
        if key in self._speed:
            self._speed[key] = n

    def set_last_time(self, ts):
        self._last_time = ts

    def get_last_time(self):
        return self._last_time

    def declare(self, key):
        if key not in self._map:
            self._map[key] = 0
            self._speed[key] = 0
            return True
        else:
            return False

    def set(self, key, n):
        if key in self._map:
            self._map[key] += n

    def get(self, key):
        if key in self._map:
            return self._map[key]

    def delete(self, key):
        if key in self._map:
            del self._map[key]
            return True
        else:
            return False
