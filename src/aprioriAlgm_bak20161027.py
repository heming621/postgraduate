#-*- coding:utf-8 -*-
"""
Description     : Simple Python implementation of the Apriori Algorithm
Usage:
    $python apriori.py -f DATASET.csv -s minSupport  -c minConfidence
    $python apriori.py -f DATASET.csv -s 0.15 -c 0.6
"""

import sys
import time
import hashTree_test02
from itertools import chain, combinations
from collections import defaultdict
from optparse import OptionParser

def subsets(arr):
    """ Returns non empty subsets of arr"""
    return chain(*[combinations(arr, i + 1) for i, a in enumerate(arr)])

def returnItemsWithMinSupport(itemSet, transactionList, minSupport, freqSet):
        """calculates the support for items in the itemSet and returns a subset  of the itemSet each of whose elements satisfies the minimum support"""
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
    
def returnItemsWithMinSupportV2(itemSet, lenItem, transactionList, minSupport, freqSet):
    itemList = [list(val) for val in itemSet]
    for trans in transactionList:
        tempList = []
        tempList.append(list(trans))
        level = 0
        root = hashTree_test02.HashTree(tempList, lenItem, level)
        _itemSet = hashTree_test02.subsetV2(itemList, root, freqSet, minSupport)  #自 itemSet --> _itemSet；对应参数itemSet返回_itemSet。
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

    freqSet = defaultdict(int)
    largeSet = dict()
    # Global dictionary which stores (key=n-itemSets,value=support)
    # which satisfy minSupport
    assocRules = dict()
    # Dictionary which stores Association Rules
    print "Counting 1-FIs start..."
    oneCSet = returnItemsWithMinSupport(itemSet,
                                        transactionList,
                                        minSupport,
                                        freqSet)
    print "Counting 1-FIs finished! And the 1-Freq is "+str(len(oneCSet))
    currentLSet = oneCSet
    k = 2
    while(currentLSet != set([])):
        largeSet[k-1] = currentLSet
        # if put the judge behind, the largeSet cann't get 3-Freq data and func printResults() will forget to write it.
        if k > 4:
            break
        print str(k)+"-FIs start...  - %s" % round(time.clock(), 2)
        print "Join start."
        currentLSet = joinSet(currentLSet, k)
        print "Join over. Get candidate-set.\nCounting %s-FIs start. - %s" % (k, round(time.clock(), 2))
#         if k==3:
#             print "currentLSet:%s"%currentLSet
#             print "transactionList:%s"%transactionList
#             print "freqSet:%s"%freqSet
#             exit()
#         currentCSet = returnItemsWithMinSupportV2(currentLSet,        #zi format:set(frozenset,frozenset,...)
#                                                 transactionList,      #zi format:list(frozenset,frozenset,...)
#                                                 minSupport,
#                                                 freqSet)              #zi {itemSet:count, itemSet:count, ...}-- format:{frozenset(['267']): 223,...} 频繁项集计数；
        currentCSet = returnItemsWithMinSupportV2(currentLSet,        #zi format:set(frozenset,frozenset,...)
                                                k,
                                                transactionList,      #zi format:list(frozenset,frozenset,...)
                                                minSupport,
                                                freqSet)              #zi {itemSet:count, itemSet:count, ...}-- format:{frozenset(['267']): 223,...} 频繁项集计数；

        currentLSet = currentCSet
        print "Counting %s-FIs finished! And the %s-Freq is %s. - %s " % (k, k, len(currentCSet), round(time.clock(),2)), "\n"
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
    print "Start to get rules. - %s" % round(time.clock(), 2)
    for key, value in largeSet.items()[1:]:
        for item in value:
            _subsets = map(frozenset, [x for x in subsets(item)])
            for element in _subsets:
                remain = item.difference(element)
                if len(remain) > 0:
                    confidence = getSupport(item)/getSupport(element)
                    # item(or itemSet) = element + remain   pre --> post  element --> remain
                    preCount = getCount(element)
                    setCount = getCount(item)            
                    if confidence >= minConfidence:
                        toRetRules.append(((tuple(element), tuple(remain)),
                                          (preCount, setCount, len(transactionList)),
                                           confidence))
    print "Get rules over. - %s" % round(time.clock(), 2)
    return toRetItems, toRetRules

def printResults(items, rules, fileFIs, fileRules):
    """prints the generated itemsets sorted by support and the confidence rules sorted by confidence"""
    fFIs = open(fileFIs, 'w')
    fRules = open(fileRules, 'w')
    for item, support, count, lenOfT in sorted(items, key=lambda (item, support, count, lenOfT): support):
        pass # print "item: %s , %.3f - %s/%s" % (str(item), support, count, lenOfT)
        fFIs.write("item:%s, %.3f - %s/%s\n" % (item, support, count, lenOfT))
    print "\n------------------------ RULES:"
    for rule, count, confidence in sorted(rules, key=lambda (rule, count, confidence): confidence):
        pre, post = rule
        preC, setC, lenOfT = count
        tempStr = "Rule:%s ==> %s, %.3f - preC:%s, setC:%s, lenOfT:%s\n" % (pre, post, confidence, preC, setC, lenOfT)
        print "Rule: %s ==> %s , %.3f (preC:%s, setC:%s, lenOfT:%s)" % (pre, post, confidence, preC, setC, lenOfT)
        fRules.write(tempStr)

    fFIs.close()
    fRules.close()

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

    inFile = None
    if options.input is None:
            inFile = sys.stdin
    elif options.input is not None:
            inFile = dataFromFile(options.input)
    else:
            print 'No dataset filename specified, system with exit\n'
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
    print 'The End.'
    
    
    
