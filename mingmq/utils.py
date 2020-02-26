"""
工具
"""
import json
import traceback
from io import StringIO
from mingmq import message


def str_to_hex(stri):
    """
    字符串转16进制
    """
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
    """
    16进制转字符串
    """
    #return ''.join([chr(i) for i in [int(b, 16) for b in stri.split(' ')]])
    buf = StringIO()

    for hex_str in stri.split(' '):
        n16 = int(hex_str, 16)
        buf.write(chr(n16))

    value = buf.getvalue()
    buf.close()
    return value


def str_to_bin(stri):
    """
    字符串转二进制
    """
    return ' '.join([bin(ord(c)) for c in stri])

def bin_to_str(stri):
    """
    二进制转字符串
    """
    return ''.join([chr(i) for i in [int(b, 2) for b in stri.split(' ')]])


def to_json(data):
    """
    转换成json
    """
    try:
        msg = json.loads(data)
        return msg
    except (json.JSONDecodeError, TypeError):
        print(traceback.print_exc())
        return False


def check_msg(msg):
    """
    校验数据
    """
    if 'type' not in msg and msg['type'] not in message.MESSAGE_TYPE.values():
        return False
    return True
