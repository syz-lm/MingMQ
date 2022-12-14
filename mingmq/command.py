import argparse
import platform
import logging

from multiprocessing import Queue, Process, freeze_support, active_children
import json
import time

from mingmq.status import ServerStatus
from mingmq.settings import CONFIG_FILE
from mingmq.utils import check_config
from mingmq.process import MQProcess, AckProcess, CompletelyPersistentProcess, NoAckProcess


LOGGER = logging.getLogger('Main')

def main(log_level=logging.DEBUG):
    global LOGGER
    logging.basicConfig(level=log_level, format='%(levelname)s:%(asctime)s:%(name)s[%(message)s]')

    parser = argparse.ArgumentParser('欢迎使用MingMQ消息队列服务器。')

    parser.add_argument('--CONFIG_REUSE', type=int, default=0,
                        help='是否读取配置文件来启动服务：0为不读取，1为读取，1则使用默认配置文件路径' + CONFIG_FILE + '，该路径不允许修改。')

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

    ack_process_db_file = '/Volumes/GoodByeUbuntu/mingmq/ack_process_db_file.db'
    if platform.platform().startswith("Windows"):
        ack_process_db_file = "C:\mingmq\ack_process_db_file.db"
    elif platform.platform().startswith('Linux'):
        ack_process_db_file = '/mnt/hgfs/mingmq/ack_process_db_file.db'

    parser.add_argument('--ACK_PROCESS_DB_FILE', type=str, default=ack_process_db_file,
                        help='输入服务器确认消息文件名')

    completely_persistent_process_db_file = '/Volumes/GoodByeUbuntu/mingmq/ompletely_persistent_process_db_file.db'
    if platform.platform().startswith("Windows"):
        completely_persistent_process_db_file = "C:\mingmq\completely_persistent_process_db_file.db"
    elif platform.platform().startswith('Linux'):
        completely_persistent_process_db_file = '/mnt/hgfs/mingmq/completely_persistent_process_db_file.db'

    parser.add_argument('--COMPLETELY_PERSISTENT_PROCESS_DB_FILE', type=str, default=completely_persistent_process_db_file,
                        help='输入服务器确认消息文件名')

    parser.add_argument('--RESEND_INTERVAL', type=int, default=300, help='输入将未ack的任务重新推送到队列的时间间隔')

    flags = parser.parse_args()
    try:
        _read_command_line(flags)
    except KeyboardInterrupt:
        import traceback
        LOGGER.error(traceback.format_exc())


def _read_command_line(flags):
    global LOGGER
    check_result = check_config(flags)

    if check_result == 0:
        LOGGER.debug('FLAGS出现问题。')
        return
    elif check_result == 1:
        LOGGER.debug('HOST输入有误。')
        return
    elif check_result == 2:
        LOGGER.debug('PORT输入有误。')
        return
    elif check_result == 3:
        LOGGER.debug('USER_NAME 或者 PASSWD 输入有误。')
        return
    elif check_result == 4:
        LOGGER.debug('确认消息文件路径错误。')
        return
    elif check_result == 5:
        LOGGER.debug('发送消息文件路径错误。')
        return

    bd = dict()
    if flags.CONFIG_REUSE == 0:
        bd['HOST'] = flags.HOST
        bd['PORT'] = flags.PORT
        bd['USER_NAME'] = flags.USER_NAME
        bd['PASSWD'] = flags.PASSWD
        bd['MAX_CONN'] = flags.MAX_CONN
        bd['TIMEOUT'] = flags.TIMEOUT
        bd['ACK_PROCESS_DB_FILE'] = flags.ACK_PROCESS_DB_FILE
        bd['COMPLETELY_PERSISTENT_PROCESS_DB_FILE'] = flags.COMPLETELY_PERSISTENT_PROCESS_DB_FILE
        bd['RESEND_INTERVAL'] = flags.RESEND_INTERVAL

        with open(CONFIG_FILE, 'w') as f:
            # ensure_ascii写中文, indent 格式化json
            json.dump(bd, f, ensure_ascii=False, indent=4)
    elif flags.CONFIG_REUSE == 1:
        with open(CONFIG_FILE, 'r') as f:
            # ensure_ascii写中文, indent 格式化json
            bd = json.load(f)
    else:
        LOGGER.error('您是否要使用上一次使用过的配置来启动服务。')
        return

    LOGGER.debug('正在启动，服务器的配置为\nIP/端口:%s:%d, 用户名/密码:%s/%s，'
          '最大并发数:%d，超时时间: %d，服务器配置路径: %s，'
          '服务器确认消息文件名: %s，服务器发送消息文件名: %s,'
          '重发未ACK任务的时间间隔: %d' %
          (bd['HOST'], bd['PORT'], bd['USER_NAME'], bd['PASSWD'],
           bd['MAX_CONN'], bd['TIMEOUT'], CONFIG_FILE, bd['ACK_PROCESS_DB_FILE'],
           bd['COMPLETELY_PERSISTENT_PROCESS_DB_FILE'], bd['RESEND_INTERVAL']))

    server_status = ServerStatus(bd['HOST'], bd['PORT'], bd['MAX_CONN'],
                                 bd['USER_NAME'], bd['PASSWD'], bd['TIMEOUT'])

    completely_persistent_process_queue = Queue()
    ack_process_queue = Queue()

    freeze_support() # 这行没有不能fork

    mmserver = MQProcess(server_status, completely_persistent_process_queue, ack_process_queue)
    mq_process = Process(target=mmserver.serv_forever, name='mq_process')
    mq_process.start() # mq服务器第一启动

    ackp = AckProcess(bd['ACK_PROCESS_DB_FILE'], bd['HOST'], bd['PORT'],
                      bd['USER_NAME'], bd['PASSWD'], ack_process_queue)
    ackp.load_send_db_memory() # 恢复数据到内存

    ack_process = Process(target=ackp.serv_forever, name='ack_process')

    cpp = CompletelyPersistentProcess(bd['COMPLETELY_PERSISTENT_PROCESS_DB_FILE'],
                                      completely_persistent_process_queue,
                                      bd['HOST'], bd['PORT'],
                                      bd['USER_NAME'], bd['PASSWD'])
    cpp.load_send_db_memory() # 恢复数据到内存

    completely_persistent_process = Process(target=cpp.serv_forever, name='completely_persistent_process')

    ack_process.start()
    completely_persistent_process.start()

    noap = NoAckProcess(bd['ACK_PROCESS_DB_FILE'], bd['HOST'], bd['PORT'],
                        bd['USER_NAME'], bd['PASSWD'])

    no_ack_process = Process(target=noap.serv_forever, name='no_ack_process')
    no_ack_process.start()

    while True:
        for p in active_children():
            LOGGER.debug('监控，子进程名: %s, PID: %s', p.name, p.pid)

        time.sleep(30)


if __name__ == '__main__':
    main(logging.DEBUG)