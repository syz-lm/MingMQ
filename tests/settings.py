IP = 'localhost'
PORT = 15673
USER = 'mingmq'
PASSWD = 'mm5201314'

HTML = ''
BUG0 = ''

import os

current_dir = os.path.dirname(os.path.abspath(__file__))

with open(current_dir + os.path.sep + 'test.html', encoding='utf-8') as file_desc:
    HTML = file_desc.read()

with open(current_dir + os.path.sep + 'bug0', encoding='utf-8') as file_desc:
    BUG0 = file_desc.read()