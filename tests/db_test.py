# import logging
# logging.basicConfig(level=logging.DEBUG)

from mingmq.db import AckProcessDB
import time

am = AckProcessDB('./test.db')
am.create_table()
now = int(time.time())

for i in range(1000):
    am.insert_message_id_queue_name_message_data_pub_date(
        'message_%d' % i,
        'queue_name_%d' %i,
        'message_data_%d' % i,
        int(time.time())
    )
    if i == 500:
        now = int(time.time())

result = am.pagnation(now)
print(result)
for r in result:
    am.delete_by_message_id(r[0])
print('total num:', am.total_num())

result = am.pagnation(now)
print(result)
for r in result:
    am.delete_by_message_id(r[0])
print('total num:', am.total_num())