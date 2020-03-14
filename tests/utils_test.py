from unittest import TestCase
from mingmq.utils import get_size
from .settings import *


class VariableTest(TestCase):
    def test_str(self):
        """测试这个html在linux和mac下的内存占用差距

        * Ubuntu: 0.7029838562011719
        * Mac: 0.7029838562011719

        """
        print('%sM' %  repr(get_size(HTML) / (1024 * 1024)))

    def test_int(self):
        """测试不同值的int值所占用的内存大小

        结论:
        * 0和其它数字占用内存有不一样，0和非0数字不是一个类型；
        * 引用貌似不算内存；
        * 数字所占用的内存都是一样的；

        """
        print('%sKB' % repr(get_size(0) / 1024))
        print('%sKB' % repr(get_size(1) / 1024))

        a = 0
        b = 1

        print('%sKB' % repr(get_size(a) / 1024))
        print('%sKB' % repr(get_size(b) / 1024))

    def test_float(self):
        """测试不同值的float值所占用的内存大小

        结论:
        * 0和其它数字占用内存有不一样，0和非0数字不是一个类型；
        * 引用貌似不算内存；
        * 数字所占用的内存都是一样的；

        """
        print('%sKB' % repr(get_size(0) / 1024))
        print('%sKB' % repr(get_size(0.1) / 1024))

        a = 0
        b = 1.1

        print('%sKB' % repr(get_size(a) / 1024))
        print('%sKB' % repr(get_size(b) / 1024))