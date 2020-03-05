$(".logout").click(function () {
    $.ajax({
        url: "/logout",
        type: 'POST',
        cache: false,
        dataType: "json",
        statusCode: {
            401: function() {
                try {
                    if (window.navigator.userAgent.indexOf('Edge') != -1)
                        document.execCommand("ClearAuthenticationCache");
                } catch (e) {
                    console.log(e);
                }

                location.href = '/logout';
            }
        },
        async: true
    });
});

function get_stat() {
    $.ajax({
        url: "/get_all",
        type: 'GET',
        cache: false,
        dataType: "json",
        success: function(data) {
            var html =  "<tr><th>队列名</th>" +
                "<th>剩余任务数</th>" +
                "<th>剩余ACK数</th>" +
                "<th>推送速度</th>" +
                "<th>消费速度</th>" +
                "<th>确认速度</th>" +
                "<th>消息推送</th>" +
                "<th>消息获取</th>" +
                "<th>清空队列</th>" +
                "<th>消息确认</th>" +
                "<th>删除队列</th></tr>";
            if (data.status == 1) {
                var json_obj = data.json_obj;
                var stat_infor = json_obj[0];
                var queue_infor = stat_infor['queue_infor'];
                var speed_infor = stat_infor['speed_infor'];
                var task_ack_infor = stat_infor['task_ack_infor'];

                for(var key in queue_infor) {
                    var queue_name = key;
                    var task_num = queue_infor[key];
                    var ack_num = task_ack_infor[key];

                    var send_speed = speed_infor['send_' + queue_name];
                    var get_speed = speed_infor['get_' + queue_name];
                    var ack_speed = speed_infor['ack_' + queue_name];

                    html += "<tr><td>" + queue_name + "</td>" +
                        "<td>" + task_num + "</td>" +
                        "<td>" + ack_num + "</td>" +
                        "<td>" + send_speed + "</td><td>" + get_speed + "</td>" +
                        "<td>" + ack_speed + "</td><td><button queue_name='" +
                        queue_name + "' class='send_message'>send message</button></td><td><button queue_name='" +
                        queue_name + "' class='get_message'>get message</button></td><td><button queue_name='" + queue_name +
                        "' class='clear_queue'>clear queue</button></td>" +
                        "<td><button class='ack_message' queue_name='" + queue_name + "'>ack message</button></td>" +
                        "<td><button queue_name='" + queue_name + "' class='delete_queue'>delete queue</button></td></tr>";
                }

                $(".stat_infor").html(html);
            }
        },
        error: function (err) {
            location.href = '/';
        },
        async: true
    });
}

function init_table() {
    var html =  "<tr><th>队列名</th>" +
        "<th>剩余任务数</th>" +
        "<th>剩余ACK数</th>" +
        "<th>推送速度</th>" +
        "<th>消费速度</th>" +
        "<th>确认速度</th>" +
        "<th>消息推送</th>" +
        "<th>消息获取</th>" +
        "<th>清空队列</th>" +
        "<th>删除队列</th></tr><tr>正在拉取数据，请稍后。</tr>";
    $(".stat_infor").html(html);
}

$(function () {
    init_table();
    // get_stat();

    setInterval(function () {
        get_stat();
    }, 2000);
});

$(".stat_infor").on('click', 'tr > td > .send_message', function () {
    var queue_name = $(this).attr("queue_name");
    var message = prompt("请输入要推送的数据：");

    if (message == null || message.trim() == "" ) {
        alert("输入不能为空");
        return;
    }

    $.ajax({
        url: "/put",
        type: 'POST',
        cache: false,
        contentType: "application/x-www-form-urlencoded",
        data: {
            queue_name: queue_name,
            message: message
        },
        dataType: "json",
        success: function (data) {
            alert(JSON.stringify(data));
        },
        error: function (err) {
        },
        async: false
    });
});

$(".stat_infor").on('click', 'tr > td > .get_message', function () {
    var queue_name = $(this).attr("queue_name");
    if (confirm("确认要从" + queue_name + "中获取一个数据吗？"))
        $.ajax({
            url: "/get",
            type: 'GET',
            cache: false,
            contentType: "application/x-www-form-urlencoded",
            data: {
                queue_name: queue_name,
            },
            dataType: "json",
            success: function (data) {
                var jd = JSON.stringify(data);
                alert(jd);
                console.info('message_infor', jd);
            },
            error: function (err) {
            },
            async: false
        });
});

$(".stat_infor").on('click', 'tr > td > .clear_queue', function () {
    var queue_name = $(this).attr("queue_name");

    if (confirm("确认要清除" + queue_name + "中所有的数据吗？"))
        $.ajax({
            url: "/clear",
            type: 'GET',
            cache: false,
            contentType: "application/x-www-form-urlencoded",
            data: {
                queue_name: queue_name,
            },
            dataType: "json",
            success: function (data) {
                alert(JSON.stringify(data))
            },
            error: function (err) {
            },
            async: false
        });
});

$(".stat_infor").on('click', 'tr > td > .delete_queue', function () {
    var queue_name = $(this).attr("queue_name");
    if (confirm("确认要删除队列" + queue_name + "吗？"))
        $.ajax({
            url: "/delete",
            type: 'GET',
            cache: false,
            contentType: "application/x-www-form-urlencoded",
            data: {
                queue_name: queue_name,
            },
            dataType: "json",
            success: function (data) {
                alert(JSON.stringify(data))
            },
            error: function (err) {
            },
            async: false
        });
});

$(".stat_infor").on('click', 'tr > td > .ack_message', function () {
    var queue_name = $(this).attr("queue_name");
    var message_id = prompt("请输入确认的消息id：")
    if (message_id != null && message_id.trim() != "")
        $.ajax({
            url: "/ack",
            type: 'POST',
            cache: false,
            contentType: "application/x-www-form-urlencoded",
            data: {
                queue_name: queue_name,
                message_id: message_id
            },
            dataType: "json",
            success: function (data) {
                alert(JSON.stringify(data))
            },
            error: function (err) {
            },
            async: false
        });
});

$(".create_queue").click(function () {
    var queue_name = prompt("请输入要创建的队列名：");
    if (queue_name != null && queue_name.trim() != "")
        $.ajax({
            url: "/declare",
            type: 'GET',
            cache: false,
            contentType: "application/x-www-form-urlencoded",
            data: {
                queue_name: queue_name,
            },
            dataType: "json",
            success: function (data) {
                alert(JSON.stringify(data))
            },
            error: function (err) {
            },
            async: false
        });
    else
        alert("要创建的队列名不能为空。")
});

