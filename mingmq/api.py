import json
import logging
import math
logging.basicConfig(level=logging.DEBUG)
from datetime import timedelta

from flask import request, Flask, render_template, Response
from flask_httpauth import HTTPBasicAuth

from mingmq.client import Pool
from mingmq.settings import CONFIG_FILE
from mingmq.db import AckProcessDB

from gevent.pywsgi import WSGIServer
from gevent import monkey


def init_mmcli_config():
    with open(CONFIG_FILE, 'r') as f:
        bd = json.load(f)
        user_name = bd['USER_NAME']
        passwd = bd['PASSWD']
        port = bd['PORT']
        ack_process_db_file = bd['ACK_PROCESS_DB_FILE']

    return user_name, passwd, port, ack_process_db_file


def init_flask():
    app = Flask(__name__, static_folder='./static', template_folder='./templates')

    app.config.from_mapping(
        SEND_FILE_MAX_AGE_DEFAULT=timedelta(seconds=1),
    )

    auth = HTTPBasicAuth()

    return app, auth

APP, AUTH = init_flask()
USER_NAME, PASSWD, PORT, ACK_PROCESS_DB_FILE = init_mmcli_config()
USERS = [{
    'user_name': USER_NAME,
    'passwd': PASSWD
}]

POOL = Pool('localhost', PORT, USER_NAME, PASSWD, 10)


@AUTH.get_password
def get_passwd(user_name):
    global USERS
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
    global USER_NAME, PASSWD, POOL
    queue_name = request.args.get('queue_name')
    result = POOL.opera('declare_queue', *(queue_name, ))
    if result: return result

@APP.route('/delete')
@AUTH.login_required
def delete():
    global USER_NAME, PASSWD, POOL
    queue_name = request.args.get('queue_name')
    result = POOL.opera('del_queue', *(queue_name, ))
    if result: return result


@APP.route('/get')
@AUTH.login_required
def get():
    global USER_NAME, PASSWD, POOL
    queue_name = request.args.get('queue_name')
    result = POOL.opera('get_data_from_queue', *(queue_name, ))
    if result: return result


@APP.route('/put', methods=['POST'])
@AUTH.login_required
def put():
    global USER_NAME, PASSWD, POOL
    queue_name = request.form['queue_name']
    message = request.form['message']
    result = POOL.opera('send_data_to_queue', *(queue_name, message))
    if result: return result


@APP.route('/get_all')
@AUTH.login_required
def get_all():
    global USER_NAME, PASSWD, POOL
    result = POOL.opera('get_stat')
    if result: return result


@APP.route('/clear')
@AUTH.login_required
def clear():
    global USER_NAME, PASSWD, POOL
    queue_name = request.args.get('queue_name')
    result = POOL.opera('clear_queue', *(queue_name, ))
    if result: return result


@APP.route('/ack', methods=['POST'])
@AUTH.login_required
def ack():
    global USER_NAME, PASSWD, POOL
    queue_name = request.form['queue_name']
    message_id = request.form['message_id']
    result = POOL.opera('ack_message', *(queue_name, message_id))
    if result: return result


@APP.route('/get_speed')
@AUTH.login_required
def get_speed():
    global USER_NAME, PASSWD, POOL
    queue_name = request.args.get('queue_name')
    result = POOL.opera('get_speed', *(queue_name, ))
    if result: return result


@APP.route('/pag_noack_task')
@AUTH.login_required
def pag_noack_task():
    global USER_NAME, PASSWD, POOL, ACK_PROCESS_DB_FILE
    page = request.args.get('page')
    ack_msg = AckProcessDB(ACK_PROCESS_DB_FILE)
    return {
        "data": ack_msg.pagnation_page(int(page)),
        'status': 1
    }



@APP.route('/get_noack_task_total_num')
@AUTH.login_required
def get_noack_task_total_num():
    global USER_NAME, PASSWD, POOL, ACK_PROCESS_DB_FILE
    ack_msg = AckProcessDB(ACK_PROCESS_DB_FILE)

    reuslt =  ack_msg.total_num()
    if reuslt and len(reuslt) > 0:
        return {
            "total_pages": math.ceil(reuslt[0][0] / 25)
        }
    else:
        return {
            "total_page": 0
        }


@APP.route('/delete_ack_message', methods=['POST'])
@AUTH.login_required
def delete_ack_message():
    global USER_NAME, PASSWD, POOL
    queue_name = request.form['queue_name']
    message_id = request.form['message_id']
    result = POOL.opera('delete_ack_message_id_queue_name', *(message_id, queue_name))
    if result: return result


def debug():
    try:
        APP.run(host='0.0.0.0', port=15674)
    except: pass
    finally: POOL.release()


def main():
    global APP
    try:
        monkey.patch_all()

        http_server = WSGIServer(('0.0.0.0', int(15674)), APP)
        http_server.serve_forever()
    except Exception as e:
        logging.error(e)
    finally:
        POOL.release()