.. MingMQ documentation master file, created by
   sphinx=quickstart on Thu Mar 12 05:53:04 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to MingMQ’s documentation!
======================================

这是一个模仿rabbitmq的消息队列服务器，在windows下使用多线程，linux下使用epoll，自带python编程驱动。项目是在2020年年初开发，然后在2021年
年初，经过这一年的使用，修改了非常多的bug，遇到了非常多的问题。并且，项目在以后的使用过程中会遇到越来越多的问题。由于没有做过多的测试，所以一直是
直接用，出了问题，一般都要去调试服务器和驱动，少则一个星期，多则半年。笔者就经历过这些修改bug的艰辛历程。

建议还是不要去重新造轮子，虽然造轮子能解决主要矛盾，但是就会遇到很多以前轮子已经解决过的次要矛盾。

当然，造轮子有造轮子的好处，在造轮子的过程中，能接触到许多在开发中遇不到的问题，并且由于遇到这些问题，从而导致能学到很多计算机中的知识。

笔者以前就是专职做爬虫开发的，所以这个消息队列也是主要是用于爬虫的，为什么这么说？因为爬虫的数据本身来说就不是很重要的数据，就算出错，也不会有太大的
问题，不会造成很大的损失。

如果用于涉及金融交易等场景，就不推荐使用，因为代码可能还存在很多bug，另外对不同运行环境还没有做大量的测试和成功案例。

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   user/index
   modules/index


Indices and tables
====================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`