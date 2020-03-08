"""
数据类型
"""

import time

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
    'DELETE_QUEUE': 9,  # 删除队列
    'CLEAR_QUEUE': 10,  # 清空队列数据
    'GET_SPEED': 11,  # 获取速度
    'GET_STAT': 12,  # 获取所有统计数据
    'DELETE_ACK_MESSAGE_ID': 13, # 删除ack内存中指定的message_id内存
    'RESTORE_ACK_MESSAGE_ID': 14, # 从磁盘文件恢复ack message_id一般用于服务器重启时重新加载内存
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


class ReqRestoreAckMessageIDMessage(dict):
    def __init__(self, message_id, queue_name):
        self.type = MESSAGE_TYPE['RESTORE_ACK_MESSAGE_ID']
        self.queue_name = queue_name
        self.message_id = message_id

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
            'message_id': self.message_id
        })


class ReqDeleteAckMessageIDMessage(dict):
    """
    删除ack message_id
    """

    def __init__(self, queue_name, message_id):
        self.type = MESSAGE_TYPE['DELETE_ACK_MESSAGE_ID']
        self.queue_name = queue_name
        self.message_id = message_id

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
            'message_id': self.message_id
        })


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


ACK_PROCESS_MESSAGE = {
    'GET': 0, # 当消费者获取任务
    'ACK': 1, # 当消费者确认任务
    'ACK_RETRY': 2, # 重发未确认消息
    'DELETE_QUEUE_NOACK': 3, # 删除指定队列名未确认的任务
    'DELETE_ACK_MESSAGE_ID': 4
}


class PipeDeleteQueueNoackMessage(dict):
    """
    删除指定队列名未确认的任务
    """
    def __init__(self, queue_name):
        self.type = ACK_PROCESS_MESSAGE['DELETE_QUEUE_NOACK']
        self.queue_name = queue_name

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
        })


class PipeDeleteAckMessageID(dict):
    def __init__(self, queue_name, message_id):
        self.type = ACK_PROCESS_MESSAGE['DELETE_ACK_MESSAGE_ID']
        self.queue_name = queue_name
        self.message_id = message_id

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
            'message_id': self.message_id
        })


class PipeAckProcessGetMessage(dict):
    """
    确认消息进程请求Get结构体
    """
    def __init__(self, message_id, queue_name, message_data):
        self.type = ACK_PROCESS_MESSAGE['GET']
        self.message_id = message_id
        self.queue_name = queue_name
        self.message_data = message_data

        super().__init__({
            'type': self.type,
            'message_id': self.message_id,
            'queue_name': self.queue_name,
            'message_data': self.message_data,
            'pub_date': time.time()
        })


class PipeAckProcessAckMessage(dict):
    """
    确认消息进程请求Ack结构体
    """
    def __init__(self, message_id, queue_name):
        self.type = ACK_PROCESS_MESSAGE['ACK']
        self.message_id = message_id
        self.queue_name = queue_name

        super().__init__({
            'type': self.type,
            'message_id': self.message_id,
            'queue_name': self.queue_name,
            'pub_date': time.time()
        })


class PipeAckProcessAckRetryMessage(dict):
    """
    确认消息重发任务结构体
    """
    def __init__(self):
        self.type = ACK_PROCESS_MESSAGE['ACK_RETRY']

        super().__init__({
            'type': self.type,
            'pub_date': time.time()
        })


COMPLETELY_PERSISTENT_PROCESS_MESSAGE = {
    'SEND': 0, # 当消费者发送任务
    'GET': 1, # 当消费者获取任务
    'DELETE_QUEUE': 2, # 根据队列名删除
}


class PipeCompletelyPersistentProcessSendMessage(dict):
    def __init__(self, queue_name, message_data, message_id):
        self.type = COMPLETELY_PERSISTENT_PROCESS_MESSAGE['SEND']
        self.queue_name = queue_name
        self.message_data = message_data
        self.message_id = message_id

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name,
            'message_data': self.message_data,
            'message_id': self.message_id,
            'pub_date': time.time()
        })


class PipeCompletelyPersistentProcessGetMessage(dict):
    def __init__(self, queue_name, message_id):
        self.type = COMPLETELY_PERSISTENT_PROCESS_MESSAGE['GET']
        self.queue_name = queue_name
        self.message_id = message_id

        super().__init__({
            'type': self.type,
            'queue_name': queue_name,
            'message_id': message_id
        })


class PipeCompletelyPersistentProcessDeleteQueueMessage(dict):
    def __init__(self, queue_name):
        self.type = COMPLETELY_PERSISTENT_PROCESS_MESSAGE['DELETE_QUEUE']
        self.queue_name = queue_name

        super().__init__({
            'type': self.type,
            'queue_name': self.queue_name
        })