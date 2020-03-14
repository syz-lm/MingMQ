IP = 'serv_pro'
PORT = 15673
USER = 'mingmq'
PASSWD = 'mm5201314'

HTML = ''

import os

current_dir = os.path.dirname(os.path.abspath(__file__))

with open(current_dir + os.path.sep + 'test.html', encoding='utf-8') as file_desc:
    HTML = file_desc.read()