from mingmq.client import Producer, Consumer
import logging, time
logging.basicConfig(level=logging.DEBUG)

host, port, user_name, passwd, task_queue_name = 'localhost', 15673, 'mingmq', 'mm5201314', 'send_producer_consumer_test'
p = Producer(host, port, user_name, passwd, task_queue_name)
for i in range(1000):
    p.send_task(i)
p.release()

size = 100
data_queue_name = 'save_producer_consumer_test'
c = Consumer(host, port, user_name, passwd, size, task_queue_name, data_queue_name)


def f(message_data, message_id):
    print('从消息队列中获取的消息为：', message_data, message_id)
    time.sleep(10)


c.serv_forever(f)