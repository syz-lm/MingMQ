import platform

if platform.platform().startswith('Linux') or platform.platform().startswith('macOS'):
    CONFIG_FILE = '/etc/mingmq_config'
else:
    CONFIG_FILE = 'C:\mingmq_config'