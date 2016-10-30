#-*- coding:utf-8 -*-
from collections import defaultdict
from itertools import chain, combinations
from pybloom import BloomFilter
'''
REF:https://github.com/jaybaird/python-bloomfilter
'''
def getCandidate(fileCdd, kc):
    with open(fileCdd) as fr:
        for val in fr:
            valList = eval(val)
            if val and len(valList[3])==kc:
                return valList
            else:
                continue
    return 0

def getCddByTrans(trans):
    """ Returns non empty subsets of arr"""
    return chain(*[combinations(trans, i + 1) for i, a in enumerate(trans)])

def gettransactionList(fileTrans):
    transList = []
    with open(fileTrans, 'r') as fr:
        for val in fr:
            transList.append(eval(val))
    return transList

'''
currentCSet = returnItemsWithMinSupport(currentLSet,        #zi format:set(frozenset,frozenset,...)
                                        transactionList,    #zi format:list(frozenset,frozenset,...)
                                        minSupport,         #zi float
                                        freqSet)            #zi {itemSet:count, itemSet:count, ...}-- format:{frozenset(['267']): 223,...} 频繁项集计数；
currentLSet, transactionList, minSupport, freqSet --> currentCSet（局部变量，再添至全局变量）（返回格式：set(frozenset,frozenset,...)）
transList, candidateSet(全局->局部), lenItem, level --> candidateSet（全局）修改：将其放到一个全局变量。
'''

def checkCinT(currentCSet, transactionList, minSupport, freqSet):
    filterTrans = BloomFilter(capacity = len(transactionList), error_rate = 0.001)
    for val in transactionList:
        filterTrans.add(val)
    print filterTrans.count
    for cdd in currentCSet:
        pass
    return freqSet

def filterCdd(currentCSet, transactionList, minSupport, freqSet):
	filterCdd = BloomFilter(capacity = len(currentCSet), error_rate = 0.0001)
	for val in currentCSet:
		filterCdd.add(val)
	for trans in transactionList:
		for cdd in combinations(trans, 4):
			if cdd in filterCdd:
				freqSet[cdd] += 1
	return freqSet


'''
>>> l1 = ['a','b']
>>> f = BloomFilter(capacity=5, error_rate=0.001)
>>> f.add(l1)
False
>>> 'a' in f
False
>>> ['a', 'b'] in f
True
>>> ['a'] in f             #zi how to check? a candidate like ['a'] is contained by a transaction like ['a','b'] ? 
False
>>> print f.count
1
'''
def main():
    readTrans = './trans.txt'
    readCdd = './uniqAllCandidate.txt'
    minSupport = 0.05
    freqSet = defaultdict(int)
    transactionList = gettransactionList(readTrans)
    current4CSet = getCandidate(readCdd, 4)
    # print "current4CSet:%s" % current4CSet           #zi [['0', '10', '7', '8'], ['0', '10', '19', '7'], ...] 
    # print "transactionList:%s" % transactionList     #zi [[],[],...]
    # -----------------------------------------------------------
    checkCinT(current4CSet, transactionList, minSupport, freqSet)
    print "candidate map trans_filter:%s"%freqSet
    # -----------------------------------------------------------
    filterCdd(current4CSet, transactionList, minSupport, freqSet)
    print "trans map candidate_filter:%s" % freqSet


if __name__ == '__main__':
    main()