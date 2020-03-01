"""
服务端启动测试
"""
import logging

from mingmq.server import main

# logging.basicConfig(level=logging.DEBUG)
main("./config_file.json")
