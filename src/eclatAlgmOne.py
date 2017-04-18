#-*- coding: UTF-8 -*-
import sys
import time
type = sys.getfilesystemencoding()
from sys import argv

def eclat(prefix, items):
        while items:
            i,itids = items.pop()
            isupp = len(itids)
            if isupp >= minsup:
                print(sorted(prefix+[i]), ':', isupp)
                suffix = []
                for j, ojtids in items:
                    jtids = itids & ojtids
                    if len(jtids) >= minsup:
                        suffix.append((j,jtids))
                eclat(prefix+[i], sorted(suffix, key=lambda item: len(item[1]), reverse=True))

if __name__ == "__main__":
    data = {}
    str1 = '../data/mushroom.dat'

    #minsup   = int(argv[2])
    #minsup = 5

    ##支持度
    ratio = 0.5
    minsup = 8124*ratio
    trans = 0
    #f = open(argv[1], 'r')
    f = open(str1)

    for row in f:
        trans += 1
        for item in row.split():
            if item not in data:
                data[item] = set()
            data[item].add(trans)
    f.close()

    ##计算运行的时间
    start = time.clock()
    eclat([], sorted(data.items(), key=lambda item: len(item[1]), reverse=True))
    end = time.clock()

    print("run time: %f s" % (end-start))