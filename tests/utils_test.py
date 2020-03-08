from mingmq.utils import get_size
import sys


d = dict()

for i in range(1000):
    d[i] = i

print(get_size(d))

l = [i for i in range(1000)]
print(get_size(l), sys.getsizeof(l))

ls = sum([sys.getsizeof(i) for i in range(1000)])
print(ls)

print(sys.getsizeof(0), sys.getsizeof(-11))

# 为什么数字0占用的内存比 其它的数字要少4个字节。