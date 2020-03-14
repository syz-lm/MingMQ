"""
工具
"""
import json
import traceback
import netifaces
import socket
import os
from io import StringIO
import sys
import inspect

from mingmq.message import MESSAGE_TYPE


def str_to_hex(stri):
    # return ' '.join([hex(ord(c)) for c in stri])
    buf = StringIO()

    for char in stri:
        n16 = hex(ord(char))
        buf.write(n16)
        buf.write(' ')

    buf.truncate(buf.tell() - 1)
    value = buf.getvalue()
    buf.close()
    return value


def hex_to_str(stri):
    # return ''.join([chr(i) for i in [int(b, 16) for b in stri.split(' ')]])
    buf = StringIO()

    for hex_str in stri.split(' '):
        n16 = int(hex_str, 16)
        buf.write(chr(n16))

    value = buf.getvalue()
    buf.close()
    return value


def str_to_bin(stri):
    return ' '.join([bin(ord(c)) for c in stri])


def bin_to_str(stri):
    return ''.join([chr(i) for i in [int(b, 2) for b in stri.split(' ')]])


def to_json(data):
    try:
        msg = json.loads(data)
        return msg
    except (json.JSONDecodeError, TypeError):
        print(traceback.print_exc())
        return False


def check_msg(msg):
    if 'type' not in msg and msg['type'] not in MESSAGE_TYPE.values():
        return False
    return True


def check_config(flags):
    # 检查ip是否正确
    ips = [_z['addr'] for _x in netifaces.interfaces() for _y in netifaces.ifaddresses(_x).values() for _z in _y]
    ips.append(socket.gethostname())
    ips.append('0.0.0.0')
    ips.append('localhost')
    ips.append('127.0.0.1')

    if flags is None:
        return 0

    host = flags.HOST
    if host not in ips:
        return 1  # 1

    # 检查端口范围是否正确
    port = flags.PORT

    if not (port > 0 and port < 65536):
        return 2

    user_name = flags.USER_NAME
    passwd = flags.PASSWD

    # 检查用户名和密码
    if len(user_name) < 5 or len(passwd) < 5:
        return 3

    ack_process_db_file = flags.ACK_PROCESS_DB_FILE.replace('\\', os.path.sep).replace('/', os.path.sep)

    try:
        os.makedirs(ack_process_db_file.rsplit(os.path.sep, 1)[0])
    except FileExistsError:
        pass

    # 检查确认消息文件
    if not os.path.exists(ack_process_db_file.rsplit(os.path.sep, 1)[0]):
        return 4

    completely_persistent_process_db_file = flags.COMPLETELY_PERSISTENT_PROCESS_DB_FILE.replace('\\', os.path.sep).replace('/', os.path.sep)

    try:
        os.makedirs(completely_persistent_process_db_file.rsplit(os.path.sep, 1)[0])
    except FileExistsError:
        pass

    # 检查发送消息文件
    if not os.path.exists(completely_persistent_process_db_file.rsplit(os.path.sep, 1)[0]):
        return 5

    return 417


def get_size(obj, seen=None):
    """
    获取对象的内存占用大小，单位字节。

    :param obj: 对象
    :param seen:
    :return: int 字节
    """
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if hasattr(obj, '__dict__'):
        for cls in obj.__class__.__mro__:
            if '__dict__' in cls.__dict__:
                d = cls.__dict__['__dict__']
                if inspect.isgetsetdescriptor(d) or inspect.ismemberdescriptor(d):
                    size += get_size(obj.__dict__, seen)
                break
    if isinstance(obj, dict):
        size += sum((get_size(v, seen) for v in obj.values()))
        size += sum((get_size(k, seen) for k in obj.keys()))
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum((get_size(i, seen) for i in obj))

    if hasattr(obj, '__slots__'):  # can have __slots__ with __dict__
        size += sum(get_size(getattr(obj, s), seen) for s in obj.__slots__ if hasattr(obj, s))

    return size