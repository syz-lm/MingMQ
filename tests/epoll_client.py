#!/usr/bin/env python
#-*- coding:utf-8 -*-

import socket

html = ''
with open('./test.html', encoding='utf-8') as file_desc:
    html = file_desc.read()

def test():
    #创建客户端socket对象
    clientsocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    #服务端IP地址和端口号元组
    server_address = ('127.0.0.1',8080)
    #客户端连接指定的IP地址和端口号
    clientsocket.connect(server_address)
    l= len(html)
    while 1:
        #输入数据
        #客户端发送数据
        clientsocket.sendall(html.encode())
        #客户端接收数据
        count = 0
        while count <= l:
            server_data = clientsocket.recv(1024)
            count += len(server_data)
            print('客户端收到的数据：', server_data)

    #关闭客户端socket
    clientsocket.close()

def bf():
    from threading import Thread, active_count
    import time

    while 1:
        if active_count() < 500:
            Thread(target=test).start()
        else:
            time.sleep(2)

bf()
#
# test()