# MingMQ

一个跨平台高性能MQ消息服务，支持和RabbitMQ服务器的大多数功能。

![](https://github.com/zswj123/MingMQ/blob/local/logo.jpg)

## Hello, World!

```
client = Client('192.168.1.30', 15673)

if client.login('mingmq', 'mm5201314') is not True:
    sys.exit(-1)

print('登录成功')
client.declare_queue('hello')

result = client.send_data_to_queue('hello', HTML)
print('发送任务', result)

message_data = client.get_data_from_queue('hello')
print('获取任务成功', message_data)

result = client.ack_message('hello', message_data[0]['message_id'])
print('消息确认成功', result)

result = client.delete_queue('hello')
print('删除队列', result)

result = client.logout('mingmq', 'mm5201314')
print('注销', result)

client.close()
print('关闭成功')
```

# MORE

* https://www.rabbitmq.com/