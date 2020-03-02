# MingMQ

这是一个消息中间件服务器，内部的网络通讯模型分别采用了epoll单线程非阻塞IO和多线程
阻塞IO，当您在Linux下时，服务器的网络模型是单线程epoll的，而到了非Linux平台时，
服务器的网络模型是多线程阻塞的。

![](https://github.com/zswj123/MingMQ/blob/local/logo.jpeg)

支持和RabbitMQ服务器的大多数功能，因为我一直是RabbitMQ的使用者之一。并且我希望这
个服务器能够用在生产环境中。我尽可能的去提供有关服务器相关的特性。