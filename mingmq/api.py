from datetime import timedelta

from flask import request, Flask, render_template, Response
from flask_httpauth import HTTPBasicAuth

from mingmq.client import Client

APP = Flask(__name__, static_folder='./staticfile', template_folder='./templates')

APP.config.from_mapping(
    SEND_FILE_MAX_AGE_DEFAULT=timedelta(seconds=1),
)

AUTH = HTTPBasicAuth()

with open('/tmp/mingmq', 'r') as f:
    tmp = f.read()
    USER_NAME, PASSWD, PORT = tmp.split('\n')

CLIENT = Client('localhost', int(PORT))

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


@APP.route('/', methods=['GET'])
@AUTH.login_required
def main():
    if request.method == 'GET':
        return render_template("main.html")


@APP.route('/logout', methods=['GET', 'POST'])
def logout():
    if request.method == 'POST':
        return Response(status=401)
    elif request.method == 'GET':
        return """
            <div style="position: absolute; left: 50%; top: 50%; transform: translate(-50%, 50%);">
                <b style="color: red" id="n">10</b>秒后即将跳转到<a href='/'>登陆页面</a>。
            </div>
            <script>
                    i = 10;
                    setInterval(function() {
                        document.getElementById("n").innerHTML = --i;
                    }, 1000);
                setTimeout("window.location='/';", 10000);
            </script>
            """


@APP.route('/declare')
@AUTH.login_required
def declare():
    queue_name = request.args.get('queue_name')
    return CLIENT.declare_queue(queue_name)


@APP.route('/delete')
@AUTH.login_required
def delete():
    queue_name = request.args.get('queue_name')
    return CLIENT.del_queue(queue_name)


@APP.route('/get')
@AUTH.login_required
def get():
    queue_name = request.args.get('queue_name')
    return CLIENT.get_data_from_queue(queue_name)


@APP.route('/put')
@AUTH.login_required
def put():
    queue_name = request.args.get('queue_name')
    data = request.args.get('data')
    return CLIENT.send_data_to_queue(queue_name, data)


@APP.route('/get_all')
@AUTH.login_required
def get_all():
    data = request.args.get('data')
    return CLIENT.get_stat()


@APP.route('/clear')
@AUTH.login_required
def clear():
    queue_name = request.args.get('queue_name')
    return CLIENT.clear_queue(queue_name)


@APP.route('/get_speed')
@AUTH.login_required
def get_speed():
    queue_name = request.args.get('queue_name')
    return CLIENT.get_speed(queue_name)


def main():
    try:
        APP.run(host='0.0.0.0', port=15674)
    finally:
        CLIENT.close()
