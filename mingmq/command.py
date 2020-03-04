import argparse
import logging

from mingmq.server import Server
from mingmq.status import ServerStatus


def main():
    parser = argparse.ArgumentParser('欢迎使用MingMQ消息队列服务器！\n')

    parser.add_argument('--HOST', type=str, default='0.0.0.0',
                        help='输入服务器IP地址：: 默认，0.0.0.0')
    parser.add_argument('--PORT', type=int, default=15673,
                        help='输入服务器端口：默认，15673')
    parser.add_argument('--MAX_CONN', type=int, default=100,
                        help='输入服务器的最大并发数，默认，100')
    parser.add_argument('--USER_NAME', type=str, default='mingmq',
                        help='输入服务器账号，默认，mingmq')
    parser.add_argument('--PASSWD', type=str, default='mm5201314',
                        help='输入服务器密码，默认，mm5201314')
    parser.add_argument('--TIMEOUT', type=int, default='10',
                        help='输入服务器超时时间（仅linux下有效），默认，10')

    flags = parser.parse_args()

    print('正在启动，服务器的配置为\nIP/端口:%s:%d, 用户名/密码:%s/%s，最大连接数:%d，超时时间: %d' %
          (flags.HOST, flags.PORT, flags.USER_NAME, flags.PASSWD,
           flags.MAX_CONN, flags.TIMEOUT))

    logging.basicConfig(level=logging.INFO)

    server_status = ServerStatus(flags.HOST, flags.PORT, flags.MAX_CONN,
                                 flags.USER_NAME, flags.PASSWD, flags.TIMEOUT)

    server = Server(server_status)
    server.serv_forever()