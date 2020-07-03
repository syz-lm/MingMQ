Examples
==========


CRUD
------

The following examples make use of a simple queue

.. code:: python

    import sys
    from mingmq.client import Pool

    _port = 15673
    _user_name = 'mingmq'
    _passwd = 'mm5201314'
    _pool_size = 10
    _pool = Pool('localhost', _port, _user_name, _passwd, _pool_size)

    # 声明队列
    _queue_name = 'hello'
    result = _pool.opera('declare_queue', *(_queue_name,))
    # {"json_obj":[],"status":1,"type":2}
    if result['status'] != 1:
        print('队列声明失败')
        sys.exit(-1)

    # 向队列发送数据
    message = 'hello, mingmq'
    result = _pool.opera('send_data_to_queue', *(_queue_name, message))
    # {"json_obj":[],"status":1,"type":3}
    if result['status'] != 1:
        print('发送失败')

    # 从队列获取数据
    result = _pool.opera('get_data_from_queue', *(_queue_name,))
    # {"json_obj":[{"message_data":"12","message_id":"task_id:1593816809.7238715"}],"status":1,"type":4}
    if result['status'] != 1:
        print('获取失败')
        sys.exit(-1)
    message_id = result['json_obj'][0]['message_id']
    message_data = result['json_obj'][0]['message_data']
    print('任务数据为', message_data)
    # 12

    # 确认任务
    result = _pool.opera('ack_message', *(_queue_name, message_id))
    # {"json_obj":[],"status":1,"type":5}
    if result['status'] != 1:
        print('确认失败')

    _pool.release()