"""
QueueMemory用于声明任务队列，和存放队列任务；
QueueAckMemory用于存放指定任务队列的未确认任务id号；

使用方式：

1. 每当客户端声明队列时，向QueueMemory的map结构中申请一个Key，并且在QueueAckMemrory中也申明一个Key；
2. 每当客户端从指定队列名中放入一个任务时，先在QueueMemroy中指定的Queue中存放一个任务；
3. 客户端每当从指定队列名中获取一个任务时，先在QueueMemory中指定的Queue中获取一个任务，然后将任务id
放入QueueAckMemory中，这样也不至于让QeueuAckMemory变得很占内存空间；
4. 每当客户端确认一个消息时，在QueueAckMemory中pop出指定消息id；
"""

import math
import platform
import time
from queue import Queue

if not platform.platform().startswith('Linux'):
    from threading import Lock

    _LOCK = Lock()


class QueueMemory:
    """
    队列的内存模型
    """

    def __init__(self):
        self._map = dict()

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
        :param message: str，消息
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
            tmp[k] = v.qsize()

        return tmp


class TaskAckMemory:
    """
    消息应答的内存模型

    用于存放未应答的消息id
    """

    def __init__(self):
        self._map = dict()

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

    def put(self, set_name, message_id):
        """
        :param set_name: str，队列名
        :param message_id: str，消息id
        :return: bookean，True成功，False失败
        """
        if set_name in self._map:
            self._map[set_name].add(message_id)
            return True
        return False

    def get(self, set_name, message_id):
        """
        :param set_name: str，队列名称
        :param message_id: str，消息id
        :return: str，消息id , None表示失败
        """
        if set_name in self._map and len(self._map[set_name]) != 0:
            try:
                self._map[set_name].remove(message_id)
                return True
            except KeyError:
                return False
        return False

    def get_stat(self):
        tmp = dict()
        for k, v in self._map.items():
            tmp[k] = len(v)

        return tmp


class SyncQueueMemory:
    """
    队列的内存模型
    """

    def __init__(self):
        self._map = dict()

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
        tmp = dict()
        for k, v in self._map.items():
            tmp[k] = v.qsize()

        return tmp


class SyncTaskAckMemory:
    """
    队列消息应答的内存模型

    用于存放未应答的消息id
    """

    def __init__(self):
        self._map = dict()

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
        :return: bookean，True成功，False失败
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
        :return: str，消息id , None表示失败
        """
        with _LOCK:
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
            tmp[k] = len(v)

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
