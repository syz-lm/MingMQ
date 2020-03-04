import flask
from flask_httpauth import HTTPBasicAuth

from mingmq.client import Client

APP = flask.Flask(__name__)
AUTH = HTTPBasicAuth()

with open('/tmp/mingmq', 'r') as f:
    tmp = f.read()
    USER_NAME, PASSWD, PORT = tmp.split('\n')

CLIENT = Client('192.168.1.30', int(PORT))

CLIENT.login(USER_NAME, PASSWD)

USERS = [{
    'user_name': USER_NAME,
    'passwd': PASSWD
}]

@AUTH.get_password
def get_passwd(user_name):
    for user in USERS:
        if user['user_name'] == user_name:
            return user['passwd']
        return None


@APP.route('/')
@AUTH.login_required
def main():
    return 'Hello'


@APP.route('/logout')
@AUTH.login_required
def logout():
    return flask.Response(status=401)

@APP.route('/declare')
@AUTH.login_required
def declare():
    queue_name = flask.request.args.get('queue_name')
    return CLIENT.declare_queue(queue_name)


@APP.route('/delete')
@AUTH.login_required
def delete():
    queue_name = flask.request.args.get('queue_name')
    return CLIENT.del_queue(queue_name)


@APP.route('/get')
@AUTH.login_required
def get():
    queue_name = flask.request.args.get('queue_name')
    return CLIENT.get_data_from_queue(queue_name)


@APP.route('/put')
@AUTH.login_required
def put():
    queue_name = flask.request.args.get('queue_name')
    data = flask.request.args.get('data')
    return CLIENT.send_data_to_queue(queue_name, data)

@APP.route('/get_all')
@AUTH.login_required
def get_all():
    data = flask.request.args.get('data')
    return CLIENT.get_stat()

@APP.route('/clear')
@AUTH.login_required
def clear():
    queue_name = flask.request.args.get('queue_name')
    return CLIENT.clear_queue(queue_name)

@APP.route('/get_speed')
@AUTH.login_required
def get_speed():
    queue_name = flask.request.args.get('queue_name')
    return CLIENT.get_speed(queue_name)

if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=15674)