#-*- coding:utf-8 -*-
"""
Description: Simple Python implementation of the Apriori Algorithm
Usage:
    $python apriori.py -f DATASET.csv -s minSupport  -c minConfidence
    $python apriori.py -f DATASET.csv -s 0.15 -c 0.6
20161030：-f ../data/T10I4D100K.csv -s 0.01 -c 0.00
"""
import sys
import time
import hashTree_test02
import hashTree_test03
import cuckoofilter
from itertools import chain, combinations
from collections import defaultdict
from optparse import OptionParser
from pybloom import BloomFilter
from aprioriAlgm_bak20161106 import getTime

def subsets(arr):
    """ Returns non empty subsets of arr"""
    return chain(*[combinations(arr, i + 1) for i, a in enumerate(arr)])

def returnItemsWithMinSupport(itemSet, transactionList, minSupport, freqSet):
        """calculates the support for items in the itemSet and returns a subset of the itemSet each of whose elements satisfies the minimum support"""
        _itemSet = set()
        localSet = defaultdict(int)

        for item in itemSet:
                for transaction in transactionList:
                        if item.issubset(transaction):
                                freqSet[item] += 1
                                localSet[item] += 1

        for item, count in localSet.items():
                support = float(count)/len(transactionList)

                if support >= minSupport:
                        _itemSet.add(item)
        return _itemSet
#zi transaction stored in hashtree.    
def returnItemsWithMinSupportV21(itemSet, lenItem, transactionList, minSupport, freqSet):
    itemList = [list(val) for val in itemSet]
    for trans in transactionList:
        tempList = []
        tempList.append(list(trans))
        level = 0
        root = hashTree_test02.HashTree(tempList, lenItem, level)
        _itemSet = hashTree_test02.subsetV2(itemList, root, freqSet, minSupport)  #自 itemSet --> _itemSet；对应参数itemSet返回_itemSet。
    return _itemSet

#zi candidates(parameter-itemSet) stored in hashtree.
def returnItemsWithMinSupportV22(itemSet, lenItem, transactionList, minSupport, freqSet):
    itemList = [list(val) for val in itemSet]
    level = 0
    cddsFromTrans = []
    for trans in transactionList:
        for cdd in combinations(trans, lenItem):
            cddsFromTrans.append(cdd)
    print("Store n-candidates in hash tree. - %s"%getTime())
    root = hashTree_test03.HashTree(itemList, lenItem, level)        #zi root<->[[N-Candidate],[N-Candidate],...]<->如[['368', '217'], ['217', '766'],...] # print("root.items:%s"%root.items)
    print("Finish the store. \nMapping the cddFromTrans on hash tree. - %s"%getTime())
    _itemSet = hashTree_test03.subsetV3(cddsFromTrans, root, freqSet, minSupport)  #自 itemSet --> _itemSet；对应参数itemSet返回_itemSet。
    print("Finish the counting. - %s"%getTime())
    return _itemSet

# 使用BF进行候选集计数，传入[]、frozenset、str、int都可以，所以没有对传入到BF的元素做转换如转换为字符串。
'''
前如hash tree策略：对于每一项候选集，匹配事务，事务存储在hash tree；此策略对BF不适。因为BF存什么就【全量】匹配什么；
现在策略：每次将N项候选集的集合存入BF，对于每一条事务，生成对应的N项集，匹配候选集，候选集存储在BF。
'''
def returnItemsWithMinSupportV3(itemSet, lenItem, transactionList, minSupport, freqSet):
    _itemSet = set()
    localSet = defaultdict(int)
    if len(itemSet):
        filterCdd = BloomFilter(capacity = len(itemSet), error_rate = 0.0001)
    else:
        print("As I say, ValueError: Capacity must be > 0")
        return set([])
    print("Store cdds in BF ... - %s"%getTime())
    for val in itemSet:
        pass # 待引入counting BF，如达到minSup*len(transactionList)，则不插入；or 不用counting BF，判断，已在BF的则不再插入。 
        filterCdd.add(val)
    print("Mapping cddFromTrans on BF ... - %s"%getTime())
    for trans in transactionList:
        for cdd in combinations(trans, lenItem):
            cdd = frozenset(cdd)
            if cdd in filterCdd:
                freqSet[cdd] += 1           #zi 全局存一个
                localSet[cdd] += 1          #zi 局部存一个，(item, count)，然后过滤小于minSupport的。
    print("Filter cdds that less than minSup. - %s"%getTime())
    for item, count in localSet.items():
        support =  float(count)/len(transactionList)
        if support>minSupport:
            _itemSet.add(item)
    
    return _itemSet
#zi use Cuckoo Filter to calculate the candidate's support.
def returnItemsWithMinSupportV4(itemSet, lenItem, transactionList, minSupport, freqSet):
    _itemSet = set()
    localSet = defaultdict(int)
    def set2Str(cdd):
        return "_".join(cdd)    
    filterCdd = cuckoofilter.CuckooFilter(capacity = len(itemSet), fingerprint_size=1)
    print("Store cdds in CF ... - %s"%getTime())
    for val in itemSet:
        filterCdd.insert(set2Str(val))
    print("Mapping cddFromTrans on CF ... - %s"%getTime())    
    for trans in transactionList:                       #zi 20161112耗时，如果cdd仅几个，依然要全扫描所有trans并每条计算。
        for cdd in combinations(trans, lenItem):
            cdd = frozenset(cdd)
            if filterCdd.contains(set2Str(cdd)):
                freqSet[cdd] += 1           #zi 全局存一个
                localSet[cdd] += 1          #zi 局部存一个，(item, count)，然后过滤小于minSupport的。
    print("Filter cdds that less than minSup. - %s"%getTime())
    for item, count in localSet.items():
        support =  float(count)/len(transactionList)
        if support>minSupport:
            _itemSet.add(item)
    
    return _itemSet           

def joinSet(itemSet, length):
        """Join a set with itself and returns the n-element itemsets"""
        return set([i.union(j) for i in itemSet for j in itemSet if len(i.union(j)) == length])

def getItemSetTransactionList(data_iterator):
    transactionList = list()
    itemSet = set()
    for record in data_iterator:
        transaction = frozenset(record)
        transactionList.append(transaction)
        for item in transaction:
            itemSet.add(frozenset([item]))              # Generate 1-itemSets
    return itemSet, transactionList

def runApriori(data_iter, minSupport, minConfidence):
    """
    run the apriori algorithm. data_iter is a record iterator
    Return both:
     - items (tuple, support)
     - rules ((pretuple, posttuple), confidence)
    """
    itemSet, transactionList = getItemSetTransactionList(data_iter)  #zi itemSet:set(frozenset, frozenset, ...); transDB:list(frozenset,frozenset,...)
    print("Size of 1C:%s bytes \nSize of trans:%s bytes"%(sys.getsizeof(itemSet), sys.getsizeof(transactionList)))
    freqSet = defaultdict(int)
    largeSet = dict()
    # Global dictionary which stores (key=n-itemSets,value=support)
    # which satisfy minSupport
    assocRules = dict()
    # Dictionary which stores Association Rules
    print("Counting 1-FIs start...")
    oneCSet = returnItemsWithMinSupport(itemSet,
                                        transactionList,
                                        minSupport,
                                        freqSet)
    print("Counting 1-FIs finished! And the 1-Freq is %s (%s bytes)\n"%(len(oneCSet),sys.getsizeof(oneCSet)))
    currentLSet = oneCSet
    k = 2
    while(currentLSet != set([])):
        largeSet[k-1] = currentLSet
        # if put the judge behind, the largeSet cann't get 3-Freq data and func printResults() will forget to write it.
        if not currentLSet and k > 4:
            break
        print(str(k)+"-FIs start...  - %s" % getTime())
        print("Join start. Count of %s-FIs is %s (%s bytes)."%(k-1, len(currentLSet), sys.getsizeof(currentLSet)))
        currentLSet = joinSet(currentLSet, k)
        print("Join over. Count of %s-Cdds is %s (%s bytes).\nCounting %s-FIs start. - %s" % (k,len(currentLSet),sys.getsizeof(currentLSet),k,getTime()))
#         if k==3:
#             print "currentLSet:%s"%currentLSet
#             print "transactionList:%s"%transactionList
#             print "freqSet:%s"%freqSet
#             exit()
#--------------------origin-----------------------------------------
#         currentCSet = returnItemsWithMinSupport(currentLSet,          #zi format:set(frozenset,frozenset,...)
#                                                 transactionList,      #zi format:list(frozenset,frozenset,...)
#                                                 minSupport,
#                                                 freqSet)              #zi {itemSet:count, itemSet:count, ...}-- format:{frozenset(['267']): 223,...} 频繁项集计数；
#---------------hash tree----------------------------------------
#         currentCSet = returnItemsWithMinSupportV22(currentLSet,        #zi format:set(frozenset,frozenset,...)
#                                                 k,
#                                                 transactionList,      #zi format:list(frozenset,frozenset,...)
#                                                 minSupport,
#                                                 freqSet)              #zi {itemSet:count, itemSet:count, ...}-- format:{frozenset(['267']): 223,...} 频繁项集计数；
#-----------------bf && cf--------------------------------------
        currentCSet = returnItemsWithMinSupportV3(currentLSet,        #zi format:set(frozenset,frozenset,...)
                                                  k,
                                                transactionList,      #zi format:list(frozenset,frozenset,...)
                                                minSupport,
                                                freqSet)              #zi {itemSet:count, itemSet:count, ...}-- format:{frozenset(['267']): 223,...} 频繁项集计数；       
        currentLSet = currentCSet
        print("Counting %s-FIs finished! And count of %s-FIs is %s (%s bytes). - %s" % (k, k, len(currentCSet),sys.getsizeof(currentCSet),getTime()), "\n")
        k = k + 1

    def getSupport(item):
            """local function which Returns the support of an item"""
            return float(freqSet[item])/len(transactionList)
    def getCount(item):
            return int(freqSet[item])

    toRetItems = []
    for key, value in largeSet.items():
        toRetItems.extend([(tuple(item), getSupport(item), getCount(item), len(transactionList))
                           for item in value])

    toRetRules = []
    print("Start to get rules. - %s" % getTime())
    #python2# for key, value in largeSet.items()[1:]:
    for key, value in list(largeSet.items())[1:]:
        for item in value:
            _subsets = map(frozenset, [x for x in subsets(item)])
            for element in _subsets:
                remain = item.difference(element)
                if len(remain)>0 and freqSet[element]:  # if len(remain) > 0:
                    confidence = getSupport(item)/getSupport(element)
                    # item(or itemSet) = element + remain   pre --> post  rule: element -> remain
                    preCount = getCount(element)
                    setCount = getCount(item)            
                    if confidence >= minConfidence:
                        toRetRules.append(((tuple(element), tuple(remain)),
                                          (preCount, setCount, len(transactionList)),
                                           confidence))
    print("Get rules over. And count of Rule is %s - %s\n" % (len(toRetRules), getTime()))
    return toRetItems, toRetRules

def printResults(items, rules, fileFIs, fileRules):
    """prints the generated itemsets sorted by support and the confidence rules sorted by confidence"""
    print("Print start!")
    fFIs = open(fileFIs, 'w')
    fRules = open(fileRules, 'w')
    #python2# for item, support, count, lenOfT in sorted(items, key=lambda (item, support, count, lenOfT): support):
    for item, support, count, lenOfT in sorted(items, key=lambda items: items[1]):
        pass # print "item: %s , %.3f - %s/%s" % (str(item), support, count, lenOfT)
        fFIs.write("item:%s, %.3f - %s/%s\n" % (item, support, count, lenOfT))
    #python2# for rule, count, confidence in sorted(rules, key=lambda (rule, count, confidence): confidence):
    for rule, count, confidence in sorted(rules, key=lambda rules: rules[2]):
        pre, post = rule
        preC, setC, lenOfT = count
        tempStr = "Rule:%s ==> %s, %.3f - preC:%s, setC:%s, lenOfT:%s\n" % (pre, post, confidence, preC, setC, lenOfT)
        pass # print("Rule: %s ==> %s , %.3f (preC:%s, setC:%s, lenOfT:%s)" % (pre, post, confidence, preC, setC, lenOfT))
        fRules.write(tempStr)
    
    print("Print end! Write frequent items to %s and rules to %s"%(fileFIs,fileRules))
    fFIs.close()
    fRules.close()

def getTime():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

def dataFromFile(fname):
        """Function which reads from the file and yields a generator"""
        file_iter = open(fname, 'rU')
        for line in file_iter:
                line = line.strip().rstrip(',')                         # Remove trailing comma
                record = frozenset(line.split(','))
                yield record


if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-f', '--inputFile',
                         dest='input',
                         help='filename containing csv',
                         default=None)
    optparser.add_option('-s', '--minSupport',
                         dest='minS',
                         help='minimum support value',
                         default=0.15,
                         type='float')
    optparser.add_option('-c', '--minConfidence',
                         dest='minC',
                         help='minimum confidence value',
                         default=0.6,
                         type='float')

    (options, args) = optparser.parse_args()
    start, startTime = time.clock(), getTime()
    print("Start. %s.\n" % startTime)
    inFile = None
    if options.input is None:
            inFile = sys.stdin
    elif options.input is not None:
            inFile = dataFromFile(options.input)
    else:
            print('No dataset filename specified, system with exit\n')
            sys.exit('System will exit')

    minSupport = options.minS
    minConfidence = options.minC
    fileFIs = '../data/FIs.txt'
    fileRules = '../data/Rules.txt'
    #
    items, rules = runApriori(inFile, minSupport, minConfidence)
    #
    
    printResults(items, rules, fileFIs, fileRules)
    #
    end, endTime = time.clock(), getTime()
    print('\nThe Start ~ The End. %s ~ %s.\nTime cost:%s min'%(startTime,endTime,round((end-start)/60, 2)))
    
    
    
