"""MingMQ监控Web控制台。

这个模块定义了MingMQ监控Web控制台网站的的一些http服务。主要是实时监控消息队列中的
内存读取速度，内存占用大小，以及对特定的任务数据的修改操作。

所以，综上所述，此模块如果根据用户的角度来说，此模块提供了两个功能：

1. 查看消息队列使用情况，例如有该消息队列还有多少任务，多少任务未确认，消费速度，确认速度；
2. 对队列和未确认任务的操作，例如，创建队列，删除队列，清空队列，删除队列，确认消息，删除确认任务；

注意：该web服务的默认运行端口为15674。
"""

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


def _init_mmcli_config() -> (str, str, int, str):
    """读取MingMQ的端口，账号，密码，ack缓存文件

    """
    with open(CONFIG_FILE, 'r') as f:
        bd = json.load(f)
        user_name = bd['USER_NAME']
        passwd = bd['PASSWD']
        port = bd['PORT']
        ack_process_db_file = bd['ACK_PROCESS_DB_FILE']

    return user_name, passwd, port, ack_process_db_file


def _init_flask() -> (Flask, HTTPBasicAuth):
    """创建一个Flask APP对象，设置静态文件的缓存时间，以及HTTP Basic认证对象。

    """
    app = Flask(__name__, static_folder='./static', template_folder='./templates')

    app.config.from_mapping(
        SEND_FILE_MAX_AGE_DEFAULT=timedelta(seconds=1),
    )

    auth = HTTPBasicAuth()

    return app, auth

_APP, _AUTH = _init_flask()
_USER_NAME, _PASSWD, _PORT, _ACK_PROCESS_DB_FILE = _init_mmcli_config()
_USERS = [{
    'user_name': _USER_NAME,
    'passwd': _PASSWD
}]

_POOL = None


@_AUTH.get_password
def _get_passwd(user_name):
    """根据指定的用户名获取密码，这个是HTTPBasicAuth的Flask扩展
    自动调用的。

    """
    global _USERS
    for user in _USERS:
        if user['user_name'] == user_name:
            return user['passwd']
        return None


@_APP.route('/', methods=['GET'])
@_AUTH.login_required
def main():
    """当用户访问"/"时返回对应的main.html模版。当用户
    第一次访问这个路径时，会提示用户输入账号密码。账号密码
    是MingMQ的账号密码。

    所以，不要将该服务暴露在公网上，这样可能会导致账号密码被
    别人知道，需要将这个端口设置为特定都ip地址才能访问，或者
    走跳板也行。

    用户触发的权限认证是基于HTTP Basic认证的。

    """
    if request.method == 'GET':
        return render_template("main.html")


@_APP.route('/logout', methods=['GET', 'POST'])
def logout():
    """当用户访问"/logout"时，返回401，如果是post请求，若是
    get请求，则返回一段html文本给浏览器，这段文本会被浏览器渲染
    ，然后10秒后自动跳转到"/"页面，由于返回401之后，验证失效，所
    以，会提示用户输入账号密码。

    用户需要登陆，才能使用这个函数。

    """
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


@_APP.route('/declare')
@_AUTH.login_required
def declare():
    """声明队列，即创建一个队列。需要在url参数中提交queue_name参数，并且
    这个队列名必须要在MingMQ的服务中不存在，如果存在，就不会执行相应的操作
    ，可以放心的使用，因为不会导致重新分配内存。

    当用户访问"/declare"路径触发这个函数调用。用户需要登陆，这个函数才能为
    用户提供服务。

    Examples

    失败::

        暂无

    成功::

        {
            "json_obj": [],
            "status": 1,
            "type": 2
        }

    """
    global _USER_NAME, _PASSWD, _POOL
    queue_name = request.args.get('queue_name')
    result = _POOL.opera('declare_queue', *(queue_name,))
    if result: return result


@_APP.route('/delete')
@_AUTH.login_required
def delete():
    """删除指定的队列名。需要url参数提供queue_name，如果该queue_name
    存在于MingMQ则会删除该队列所有的任务和未确认的任务缓存，需要比较谨慎
    的态度，如果删除了，就意味着对未完成的任务不在乎。

    这个服务需要登陆才能使用。

    Examples

    失败::

        暂无

    成功::

        {
            "json_obj": [],
            "status": 1,
            "type": 2
        }

    """
    global _USER_NAME, _PASSWD, _POOL
    queue_name = request.args.get('queue_name')
    result = _POOL.opera('del_queue', *(queue_name,))
    if result: return result


@_APP.route('/get')
@_AUTH.login_required
def get():
    """获取指定队列的一个任务数据，需要用户登陆才能使用。
    返回的是json数据，具体要看实际的返回结果。

    数据返回只有失败和成功两种，失败是没有数据的，成功是
    有数据的。

    Examples

    失败::

        {
            "json_obj": [null],
            "status": 0,
            "type": 4
        }

    成功::

        {
            json_obj: [{
                message_data: "asdfaf",
                message_id: "task_id:1583969977.9167929"
            }],
            status: 1,
            type: 4
        }

    """
    global _USER_NAME, _PASSWD, _POOL
    queue_name = request.args.get('queue_name')
    result = _POOL.opera('get_data_from_queue', *(queue_name,))
    if result: return result


@_APP.route('/put', methods=['POST'])
@_AUTH.login_required
def put():
    """向队列发送一个任务数据，也就是当用户访问"/put"请求，并且带url
    参数queue_name和message，queue_name为队列名，message为要发送
    的数据。

    需要用户登陆后才能使用这个接口。

    Examples.

    失败::

        暂无

    成功::

        {
            "json_obj": [],
            "status": 1,
            "type": 3
        }

    """
    global _USER_NAME, _PASSWD, _POOL
    queue_name = request.form['queue_name']
    message = request.form['message']
    result = _POOL.opera('send_data_to_queue', *(queue_name, message))
    if result: return result


@_APP.route('/get_all')
@_AUTH.login_required
def get_all():
    """获取统计数据，这些统计数据是包含队列的发送速度，获取速度，确认速度
    队列还有多少任务没消费，多少任务未确认，内存占用等信息。

    需要用户登陆后才能访问这个接口，这个接口等访问地址为"/get_all"，支持
    所有等http方法。

    Examples.

    失败::

        暂无

    成功::

        {
            "json_obj": [{
                "queue_infor": {
                    "asfas": [1, 4900]
                },
                "speed_infor": {
                    "ack_asfas": 0,
                    "ack_word": 0,
                    "get_asfas": 0,
                    "get_word": 0,
                    "send_asfas": 0,
                    "send_word": 0
                },
                "task_ack_infor": {
                    "asfas": [0, 216]
                }
            }],
            "status": 1,
            "type": 11
        }

    """
    global _USER_NAME, _PASSWD, _POOL
    result = _POOL.opera('get_stat')
    if result: return result


@_APP.route('/clear')
@_AUTH.login_required
def clear():
    """清空队列，这个操作会导致MingMQ中关于该队列等所有内存以及缓存都
    全部丢失，而且不能回滚。所以要比较慎重的使用。

    该服务需要用户登陆后才能访问。

    该服务的访问地址为"/clean"，需要携带url参数queue_name队列名。

    Examples.

    失败::

        暂无

    成功::

        {
            "json_obj": [],
            "status": 1,
            "type": 2
        }

    """
    global _USER_NAME, _PASSWD, _POOL
    queue_name = request.args.get('queue_name')
    result = _POOL.opera('clear_queue', *(queue_name,))
    if result: return result


@_APP.route('/ack', methods=['POST'])
@_AUTH.login_required
def ack():
    """确认消息，就是当用户从一个队列中获取任务之后，需要对该任务
    进行一个确认，如果，一直没确认，管理人可以考虑重新将该任务放入
    任务队列。

    该服务需要用户登陆后才能使用。

    用户对访问地址为"/ack"，需要携带表单参数，queue_name，message_id
    ，并且是post请求。

    Examples.

    失败::

         暂无

    成功::

        {
            "json_obj": [],
            "status": 1,
            "type": 5
        }

    """
    global _USER_NAME, _PASSWD, _POOL
    queue_name = request.form['queue_name']
    message_id = request.form['message_id']
    result = _POOL.opera('ack_message', *(queue_name, message_id))
    if result: return result


@_APP.route('/get_speed')
@_AUTH.login_required
def get_speed():
    """获取队列的速度，这个没用过都，感觉很浪费。

    """
    global _USER_NAME, _PASSWD, _POOL
    queue_name = request.args.get('queue_name')
    result = _POOL.opera('get_speed', *(queue_name,))
    if result: return result


@_APP.route('/pag_noack_task')
@_AUTH.login_required
def pag_noack_task():
    """分页获取未确认的任务id，需要用户登陆后才能使用。
    用户访问路径"/pag_noack_task"，带url参数page页数。

    Examples.

    失败::

        暂无

    成功::

        {
            "data": [
                ["task_id:1583971487.9525692", "asdaf", "asfsaf", 1583971490.9007227]
            ],
            "status": 1
        }

    """
    global _USER_NAME, _PASSWD, _POOL, _ACK_PROCESS_DB_FILE
    page = request.args.get('page')
    ack_msg = AckProcessDB(_ACK_PROCESS_DB_FILE)
    return {
        "data": ack_msg.pagnation_page(int(page)),
        'status': 1
    }



@_APP.route('/get_noack_task_total_num')
@_AUTH.login_required
def get_noack_task_total_num():
    """获取未确认任务的总个数，是页数。

    用户要登陆才能访问。没有参数.

    Examples.

    失败::

         暂无

    成功::

        {
            "json_obj": [{
                "queue_infor": {
                    "asdaf": [0, 4298]
                },
                "speed_infor": {
                    "ack_asdaf": 0,
                    "get_asdaf": 0,
                    "send_asdaf": 0
                },
                "task_ack_infor": {
                    "asdaf": [1, 291]
                }
            }],
            "status": 1,
            "type": 11
        }

    """
    global _USER_NAME, _PASSWD, _POOL, _ACK_PROCESS_DB_FILE
    ack_msg = AckProcessDB(_ACK_PROCESS_DB_FILE)

    reuslt =  ack_msg.total_num()
    if reuslt and len(reuslt) > 0:
        return {
            "total_pages": math.ceil(reuslt[0][0] / 25)
        }
    else:
        return {
            "total_page": 0
        }


@_APP.route('/delete_ack_message', methods=['POST'])
@_AUTH.login_required
def delete_ack_message():
    """删除一个指定的task_id的未确认任务。

    用户需要登陆，并且访问"/delete_ack_message"还要带form
    表单参数，queue_name和message_id才能完成访问。

    Examples.

    失败::

        暂无

    成功::

        {
            "json_obj": [],
            "status": 1,
            "type": 13
        }

    """
    global _USER_NAME, _PASSWD, _POOL
    queue_name = request.form['queue_name']
    message_id = request.form['message_id']
    result = _POOL.opera('delete_ack_message_id_queue_name', *(message_id, queue_name))
    if result: return result


def debug():
    """在开发模式下使用

    """
    global _POOL, _APP
    try:
        _POOL = Pool('localhost', _PORT, _USER_NAME, _PASSWD, 10)
        _APP.run(host='0.0.0.0', port=15674)
    except: pass
    finally:
        if _POOL: _POOL.release()


def main():
    """生产环境下使用

    """
    global _APP, _POOL
    try:
        _POOL = Pool('localhost', _PORT, _USER_NAME, _PASSWD, 10)

        monkey.patch_all()

        http_server = WSGIServer(('0.0.0.0', int(15674)), _APP)
        http_server.serve_forever()
    except Exception as e:
        logging.error(e)
    finally:
        if _POOL: _POOL.release()