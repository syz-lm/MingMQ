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
                "<th>剩余任务数/内存(M)</th>" +
                "<th>剩余ACK数/内存(M)</th>" +
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
                    var task_num_memory = queue_infor[key];
                    var ack_num_memory = task_ack_infor[key];

                    var send_speed = speed_infor['send_' + queue_name];
                    var get_speed = speed_infor['get_' + queue_name];
                    var ack_speed = speed_infor['ack_' + queue_name];

                    html += "<tr><td class='queue_name'>" + queue_name + "</td>" +
                        "<td>" + task_num_memory[0] + "/" + (task_num_memory[1]/(1024*1024)).toFixed(2) + "</td>" +
                        "<td>" + ack_num_memory[0] + "/" + (ack_num_memory[1]/(1024*1024)).toFixed(2) + "</td>" +
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
        "<th>剩余任务数/内存(M)</th>" +
        "<th>剩余ACK数／内存(M)</th>" +
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

    setInterval(function () {
        var d = new Date();
        var date = d.toLocaleDateString();
        var time = d.toLocaleTimeString();
        $('.now').html(date + ' ' + time);
    }, 1000);

    responsive();

    pag_noack_task(1);
    get_noack_task_total_num();
});

$(window).resize(function () {
    responsive();
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
        async: true
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
                console.info('message_infor', jd);

                if (data.status == 1) {
                    alert(jd);
                }
                else alert(jd);
            },
            error: function (err) {
            },
            async: true
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
            async: true
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
            async: true
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
            async: true
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
            async: true
        });
    else
        alert("要创建的队列名不能为空。")
});

function responsive() {
    // $(".task_ids > table").css({
    //     "height": $(window).height() - 20
    // })
}

$(".task_ids_ct").click(function () {
    if($(".task_ids").is(":visible")) {
        $(".task_ids").hide();
        $(this).css({
            "left": "5px"
        });
    } else {
        $(".task_ids").show();
        $(this).css({
            "left": $(".task_ids").width() + 5
        });
    }
});

// 分页获取未确认的task
function pag_noack_task(page) {
    $.ajax({
        url: "/pag_noack_task",
        type: 'GET',
        cache: false,
        contentType: "application/x-www-form-urlencoded",
        dataType: "json",
        data: {
            "page": page
        },
        success: function (data) {
            console.log(JSON.stringify(data));
            $(".current_page").html(page);
            var total_pages = $(".total_num").html();
            var hearder = ""+
                "<tr class='task'>"+
                "<th>队列</th>"+
                "<th>任务ID</th>"+
                "<th>数据</th>"+
                "<th>时间</th>"+
                "<th>删除</th>" +
                "</tr>";
            var footer = "" +
                "<tr class='pag'>" +
                '<td>总页数：<span class="total_num">' + total_pages + '</span></td>' +
                '<td>当前页：<span class="current_page">' + page + '</span></td>' +
                '<td><button class="refresh">刷新</button></td>' +
                '<td><button class="pre_page">上一页</button></td>' +
                '<td><button class="next_page">下一页</button></td>' +
                '</tr>';
            var html = '';
            if(data.data != null)
            {
                for (var i = 0; i < data.data.length; i++) {
                    var message_id = data.data[i][0];
                    var queue_name = data.data[i][1];
                    // var message_data = JSON.stringify({"data": data.data[i][2]});
                    var message_data = data.data[i][2];
                    var t = data.data[i][3];

                    var date = new Date(t);
                    var d = date.toLocaleDateString();
                    var tt = date.toLocaleTimeString();
                    var dt = d + ' ' + tt;

                    var bea = btoa(unescape(encodeURIComponent(message_data)));

                    html += "<tr class='task'>" +
                        "<td class='queue_name' title='" + queue_name + "'>" + queue_name + "</td>" +
                        "<td class='message_id' title='" + message_id + "'>" + message_id + "</td>" +
                        "<td class='data' title='base64编码过的数据，需要解码'>" + bea + "</td>" +
                        "<td class='dt' title='" + dt + "'>" + dt + "</td>" +
                        "<td><button class='delete'>delete</button></td>" +
                        "</tr>";
                }
                $(".task_ids > table").html(hearder + html + footer);
            }
            // $(html).insertAfter(".task:last-of-type");
        },
        error: function (err) {
        },
        async: true
    });

}

$(".task_ids > table").on('click', '.pag > td > .refresh', function () {
    pag_noack_task(1);
    get_noack_task_total_num();
});

// 获取总页数
function get_noack_task_total_num() {
    $.ajax({
        url: "/get_noack_task_total_num",
        type: 'GET',
        cache: false,
        contentType: "application/x-www-form-urlencoded",
        dataType: "json",
        success: function (data) {
            var toatal_pages = data.total_pages;
            $(".total_num").html(toatal_pages);
        },
        error: function (err) {
        },
        async: true
    });
}

// 上一页
$("table").on('click', '.pag > td > .pre_page', function () {
    var current_page = parseInt($(".current_page").html());
    if (current_page > 1) {
        pag_noack_task(current_page - 1);
    }
});

// 下一页
$("table").on('click', '.pag > td > .next_page', function () {
    var current_page = parseInt($(".current_page").html());
    var total_page = parseInt($(".total_num").html())
    if (current_page < total_page) {
        pag_noack_task(current_page + 1);
    }
});

$(".close").click(function () {
    $(".decode_data").hide();
});

$("table").on('click', '.task > .data', function () {
    var bda = decodeURIComponent(escape(atob($(this).html()).toString()));
    $(".decode_data > ._message_id").html($(this).prev().html());
    $(".decode_data > textarea").val(bda);
    $(".decode_data").show();
});


$("table").on('click', '.task > td > .delete', function () {
    var queue_name = $($(this).parent().prevAll()[3]).html();
    var message_id = $($(this).parent().prevAll()[2]).html()

    if(confirm("要从服务器中删除这个未确认的任务吗？"))
        $.ajax({
            url: "/delete_ack_message",
            type: 'POST',
            cache: false,
            contentType: "application/x-www-form-urlencoded",
            dataType: "json",
            data: {
                queue_name: queue_name,
                message_id: message_id
            },
            success: function (data) {
                var jd = JSON.stringify(data);
                alert(jd);
            },
            error: function (err) {
            },
            async: true
        });
});