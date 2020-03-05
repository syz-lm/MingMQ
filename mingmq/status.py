class ServerStatus:
    def __init__(self, host, port, max_conn, user_name, passwd, timeout):
        self._host = host
        self._port = port
        self._user_name = user_name
        self._passwd = passwd
        self._max_conn = max_conn
        self._timeout = timeout

    def get_host(self):
        return self._host

    def get_port(self):
        return self._port

    def get_user_name(self):
        return self._user_name

    def get_passwd(self):
        return self._passwd

    def get_max_conn(self):
        return self._max_conn

    def get_timeout(self):
        return self._timeout
