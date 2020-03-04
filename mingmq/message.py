"""
数据类型
"""

import logging
import time
from io import StringIO
from queue import Queue

from mingmq.utils import str_to_hex

# 命令
MESSAGE_TYPE = {
    'LOGIN': 0,  # 登录
    'LOGOUT': 1,  # 退出
    'DECLARE_QUEUE': 2,  # 声明队列
    'SEND_DATA_TO_QUEUE': 3,  # 向队列推送任务
    'GET_DATA_FROM_QUEUE': 4,  # 从队列中获取数据
    'ACK_MESSAGE': 5,  # 消息确认
    'NOT_FOUND': 6,  # 找不到这个方法
    'FORBIDDEN': 7,  # 阻止访问
    'DATA_WRONG': 8,  # 数据错误
    'DELETE_QUEUE': 9, # 删除队列
    'CLEAR_QUEUE': 10, # 清空队列数据
    'GET_SPEED': 11, # 获取速度
    'GET_STAT': 12, # 获取所有统计数据
}

# 数据最大长度
MAX_DATA_LENGTH = 1024 * 1024 * 16 - 1

# 操作成功
SUCCESS = 1
# 操作失败
FAIL = 0

GET = 0
SEND = 1
ACK = 2

class ReqGetStatMessage(dict):
    '''
    获取统计数据
    '''

    def __init__(self):
        self.type = MESSAGE_TYPE['GET_STAT']

        super().__init__({
            'type': self.type
        })


class ReqGetSpeedMessage(dict):
    """
    获取队列的速度
    """

    def __init__(self, queue_name):
        self.type = MESSAGE_TYPE['GET_SPEED']
        self.queue_name = queue_name

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
        })


class ReqDeleteQueueMessage(dict):
    """
    删除队列
    """

    def __init__(self, queue_name):
        self.type = MESSAGE_TYPE['DELETE_QUEUE']
        self.queue_name = queue_name

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
        })


class ReqClearQueueMessage(dict):
    """
    清空队列
    """

    def __init__(self, queue_name):
        self.type = MESSAGE_TYPE['CLEAR_QUEUE']
        self.queue_name = queue_name

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
        })


class ReqLoginMessage(dict):
    """
    登录
    """

    def __init__(self, user_name, passwd):
        """
        初始化
        :param user_name: str，帐号
        :param passwd: str，密码
        """
        self.type = MESSAGE_TYPE['LOGIN']
        self.user_name = user_name
        self.passwd = passwd

        super().__init__({
            'type': self.type,
            'user_name': self.user_name,
            'passwd': self.passwd
        })


class ReqLogoutMessage(dict):
    """
    退出登录
    """

    def __init__(self, user_name, passwd):
        """
        初始化
        :param user_name: str，帐号
        :param passwd: str，密码
        """
        self.type = MESSAGE_TYPE['LOGOUT']
        self.user_name = user_name
        self.passwd = passwd

        super().__init__({
            'type': self.type,
            'user_name': self.user_name,
            'passwd': self.passwd
        })


class ReqDeclareQueueMessage(dict):
    """
    声明一个指定名称的队列
    """

    def __init__(self, queue_name):
        """
        初始化
        :param queue_name: str，消息队列的名称
        """
        self.type = MESSAGE_TYPE['DECLARE_QUEUE']
        self.queue_name = queue_name

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name
        })


class ReqGetDataFromQueueMessage(dict):
    """
    从指定的队列中获取一条消息
    """

    def __init__(self, queue_name):
        """
        初始化
        :param queue_name: str，消息队列的名称
        """
        self.type = MESSAGE_TYPE['GET_DATA_FROM_QUEUE']
        self.queue_name = queue_name

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name
        })


class ReqSendDataToQueueMessage(dict):
    """
    向指定的队列推送任务
    """

    def __init__(self, queue_name, message_data):
        """
        初始化
        :param queue_name: str，消息队列的名称
        :param message_data: str, 任务字符串
        """
        self.type = MESSAGE_TYPE['SEND_DATA_TO_QUEUE']
        self.queue_name = queue_name
        self.message_data = message_data

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
            'message_data': self.message_data
        })


class Task(dict):
    """
    任务数据结构
    """

    def __init__(self, message_data):
        """
        初始化
        :param message_id: str，用于消息确认时对应的任务id，因为不可能去用任务字符串去当索引，因为任务数据可能是非常长的
                            一个字符串，比如说任务数据可能是爬虫抓取的一个网页的所有源代码
        :param message_data: str，任务数据字符串
        """
        self.message_id = gen_message_id()
        self.message_data = message_data

        super().__init__({
            'message_id': self.message_id,
            'message_data': self.message_data
        })

    def get_message_id(self):
        """
        获取任务id
        """
        return self.message_id


def gen_message_id():
    """
    生成全局唯一任务id
    """
    return 'task_id:' + str(time.time())


class ReqACKMessage(dict):
    """
    消息确认，必须要带上队列名称和消息ID，方便查找
    """

    def __init__(self, queue_name, message_id):
        """
        初始化
        :param queue_name: str，消息队列名称
        :param message_id: str，消息的id
        """
        self.type = MESSAGE_TYPE['ACK_MESSAGE']
        self.queue_name = queue_name
        self.message_id = message_id

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
            'message_id': self.message_id
        })


class ResMessage(dict):
    """
    响应消息
    """

    def __init__(self, _type, status, json_obj):
        """
        初始化
        :param type: 响应消息的类型
        :param status: int，成功或者失败，1为成功，0为失败
        :param json_obj: json对象，响应的数据json化
        """
        self.type = _type
        self.status = status
        self.json_obj = json_obj

        super().__init__({
            'type': self.type,
            'status': self.status,
            'json_obj': self.json_obj
        })


def package_message(data):
    """
    封包
    """
    res_pkg = MessageWindow.MESSAGE_BEGIN + str_to_hex(data) + MessageWindow.MESSAGE_END
    return res_pkg.encode()


class MessageWindow:
    """
    这段代码，我想了3天，还进行优化过，我真的舍不得删掉。
    虽然，我通过pymysql源码找到了更好的算法。

    消息窗口
    """

    # 数据包开始
    MESSAGE_BEGIN = 'K'
    # 数据包结束
    MESSAGE_END = 'J'

    def __init__(self):
        self._current_buffer = StringIO()
        self._message_window = Queue()

    def grouping_message(self, data):
        """
        对客户端发送来的数据组装，并分组
        :param data: str, 数据
        """
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
        """
        遍历消息窗口
        """
        while self._message_window.empty() is False:
            yield self._message_window.get_nowait()

    def finished(self):
        """
        是否结束读取
        """
        return self._message_window.qsize() > 0