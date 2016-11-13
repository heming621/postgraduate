#-*- coding:utf8 -*-

"""
Description     : Simple Python implementation of the Apriori Algorithm

Usage:
    $python apriori.py -f DATASET.csv -s minSupport  -c minConfidence
    $python apriori.py -f DATASET.csv -s 0.15 -c 0.6

主要需要修改：def dataTransform2(fnameIn):中的paraList等，选取要运行的字段即可。
0713版本，可运行多个数据源组合情况。
"""
import sys

from itertools import chain, combinations
from collections import defaultdict
from optparse import OptionParser
import time
import chardet
import codecs
import re

def subsets(arr):
    """ Returns non empty subsets of arr"""
    return chain(*[combinations(arr, i + 1) for i, a in enumerate(arr)])    #zi 生成各个长度为i的子序列

#zi 输入参数：itemSet候选项的集合、事务数据库、minSupport、存储一/二/三...项频繁集的全局字典freqSet
'''
itemSet format: set(frozenset,frozenset,...)
transactionList format: list(frozenset,frozenset,...)
minSupport format: float
freqSet format: {frozenset(['267']): 223,...}, {itemSet:count, itemSet:count, ...},频繁项集计数(不仅仅>minSup的吧)；
—— ——
_itemSet format: set([frozenset(['279']), frozenset(['469']),...)
'''
def returnItemsWithMinSupport(itemSet, transactionList, minSupport, freqSet):    
        """calculates the support for items in the itemSet and returns a subset
       of the itemSet each of whose elements satisfies the minimum support"""
        _itemSet = set()                                                      #zi 存储符合minSup的item.
        localSet = defaultdict(int)

        #zi 计数
        for item in itemSet:                                                  #zi 遍历全部项item；#zi 每个项集，匹配整个事务数据库，YY此IO费时；
                for transaction in transactionList:                           #zi 遍历每一事务，看是否出现该item
                        if item.issubset(transaction):
                                freqSet[item] += 1
                                localSet[item] += 1                           #zi 除了全局字典freqSet，还有本地字典，对key计数，如{'1': 2, '3': 11, '2': 5, '4': 5}
        #zi 筛选                                  
        for item, count in localSet.items():                                  #zi 计数完，再对大于支持度项集的进行过滤，用到前面的localSet。                           
                support = float(count)/len(transactionList)

                if support >= minSupport:
                        _itemSet.add(item)

        return _itemSet                     #zi _itemSet'format: set([frozenset(['279']), frozenset(['469']),...) #zi _itemSet如果添加的元素是列表形式的itemSet[]，则形式如([xx,xx,xx],[xx,xx],......)
'''
currentCSet = returnItemsWithMinSupport(currentLSet,        #zi format:set(frozenset,frozenset,...)
                                        transactionList,    #zi format:list(frozenset,frozenset,...)
                                        minSupport,         #zi float
                                        freqSet)            #zi {itemSet:count, itemSet:count, ...}-- format:{frozenset(['267']): 223,...} 频繁项集计数；
	currentLSet, transactionList, minSupport, freqSet --> currentCSet（局部变量，再添至全局变量）（返回格式：set(frozenset,frozenset,...)）
'''

def joinSet(itemSet, length):
        """Join a set with itself and returns the n-element itemsets"""
        return set([i.union(j) for i in itemSet for j in itemSet if len(i.union(j)) == length])


def getItemSetTransactionList(data_iterator):              #zi 接受参数1：数据集; #zi 获取数据集的方法不是返回一个迭代对象么
    transactionList = list()
    itemSet = set()
    for record in data_iterator:
        transaction = frozenset(record)                    #zi 变成集合进行输入，list()里面多个frozenset，
        transactionList.append(transaction)                #zzii transactionList(frozenset(record)-->transaction1, frozenset(record)-->transaction2, ...)
        for item in transaction:                           #zi 遍历每一条事务的item，添加到一个itemSet（set格式），候选一项集。
            itemSet.add(frozenset([item]))                 # Generate 1-itemSets
    return itemSet, transactionList                        #zi 返回一项候选集+事务数据库；即itemSet+transactionList


def runApriori(data_iter, minSupport, minConfidence):       #zi 接受三个参数的输入值：数据集、最小支持度、置信度
    """
    run the apriori algorithm. data_iter is a record iterator
    Return both:
     - items (tuple, support)
     - rules ((pretuple, posttuple), confidence)
    """
    itemSet, transactionList = getItemSetTransactionList(data_iter)           #zi 返回所有的item，事务数据库；格式分别为：set(frozenset, frozenset, ...)、list(frozenset, frozenset, ...)
    print "Get the itemSet and transactionList!"

    if not itemSet or not transactionList:
        print "The itemSet or transactionList is null."
        exit()
     
    freqSet = defaultdict(int)                                                #zi 第一个参数-工厂方法int，默认value类型为int，故可freqSet[item] += 1  #zi 字典类型为int，
    largeSet = dict()
    # Global dictionary which stores (key=n-itemSets,value=support)
    # which satisfy minSupport

    assocRules = dict()
    # Dictionary which stores Association Rules

    oneCSet = returnItemsWithMinSupport(itemSet,
                                        transactionList,
                                        minSupport,
                                        freqSet)
    if not oneCSet:
    	print "1-fim finished!"

    currentLSet = oneCSet
    k = 2
    while(currentLSet != set([])):                                        #zi 如果候选集([*],[*]，...)不为空，则
        largeSet[k-1] = currentLSet                                       #zi largeSet存储了key=k, value=对应的k项频繁集
        currentLSet = joinSet(currentLSet, k)                             #zi 当前频繁k项集，生成，候选k+1项集
        currentCSet = returnItemsWithMinSupport(currentLSet,
                                                transactionList,
                                                minSupport,
                                                freqSet)
        currentLSet = currentCSet
        print str(k)+"-fim finished!"
        k = k + 1

    def getSupport(item):
            """local function which Returns the support of an item"""
            return float(freqSet[item])/len(transactionList)

    # toRetItems，使用extend()方法不断添加多个元素到列表对象toRetItems；元素内容为：（一/二/三...项频繁集），支持度。
    toRetItems = []
    for key, value in largeSet.items():                                   
        toRetItems.extend([(tuple(item), getSupport(item))
                           for item in value])                            
    
    # toRetRules，存储置信度内容；会不会撑爆？ 
    toRetRules = []
    for key, value in largeSet.items()[1:]:       
        for item in value:
            _subsets = map(frozenset, [x for x in subsets(item)])   
            for element in _subsets:
                remain = item.difference(element)                  
                if len(remain) > 0:
                    confidence = getSupport(item)/getSupport(element) 
                    if confidence >= minConfidence:
                        toRetRules.append(((tuple(element), tuple(remain)),
                                           confidence))
    return toRetItems, toRetRules


def printResults(items, rules, fileFI, fileRules):
    """prints the generated itemsets sorted by support and the confidence rules sorted by confidence"""
    fw1 = open(fileFI,'w')
    fw2 = open(fileRules, 'w')
    #---zhm
    for item, support in sorted(items, key=lambda (item, support): support):
        itemStr = ",".join(item)#.decode('utf-8').encode('utf-8')
        fw1.write(itemStr + ' ' + str(support) + '\n')
        print "item: %s , %.3f" % (str(item), support)

    fw1.close()
    print "\n------------------------ RULES:"
    for rule, confidence in sorted(rules, key=lambda (rule, confidence): confidence):
        pre, post = rule
        print "Rule: %s ==> %s , %.3f" % (str(pre), str(post), confidence)
        preStr = ",".join(pre)#.decode('gb2312').encode('utf8')
        postStr = ",".join(post)#.decode('gb2312').encode('utf8')
        fw2.write(preStr + ' ==> ' + postStr + '  ' +str(confidence) +'\n')
    fw2.close()

def dataFromFile(fnameTs):
        """Function which reads from the file and yields a generator"""
        file_iter = open(fnameTs, 'rU')
        for line in file_iter:
                line = line.strip()#.rstrip(',').replace('\"','').decode('UTF-8-SIG').encode('utf-8')       #去掉",                
                if not line:
                    continue
                record = frozenset(line.split(','))
                yield record

def dataTransform2(fnameIn):
    
    transList = defaultdict(list)
    lineList = list()

    window = 300
    timeIndex = 1 #1
    moduleIndex1 = 2 #2
    moduleIndex2 = 3 #6
    moduleIndex3 = 0
    paraList = [timeIndex, moduleIndex1, moduleIndex2]
    outputPath = 'tmpTrans.csv'
    file_iter = open(fnameIn, 'rU')
    timeSet = set()
    fw = open(outputPath,'w')

    try:
        file_iter.next()
    except StopIteration:
        print 'The first line is null'
    
    """ 将一个文件里面的多个日期存储起来 """
    for line in file_iter:

        lineList = line.replace('\"','').replace(',',' , ').strip().split(',')

        if not lineList:
            continue
        if len(lineList) < max(paraList):                               # print "NONONONONON" # bug2 # 字段不全，筛选 # 两个if放在一起就不灵？不是'与'吗
            continue
        if not lineList[timeIndex].strip() and not lineList[moduleIndex1].strip() and not lineList[moduleIndex2].strip():     # 关键字段为空，筛选
            continue

        timeStr =  lineList[timeIndex].strip()
        pattern1 = re.compile(r"\d{4}(-\d\d){2} \d\d(:\d\d){2}")        # 字段（时间）值不规范，筛选
        pattern2 = re.compile(r"\d{4}(/\d\d?){2}\s\s?\d\d?(:\d\d?)+")   # 匹配如“2016/6/1  9:08:00”-->“2016/6/1  9:08”、“2016/12/12  12:12:00”
        pattern3 = re.compile(r"\d{4}(/\d\d?){2}\s\s?\d\d?(:\d\d)+")

        if pattern1.match(timeStr) or pattern2.match(timeStr):
            timeSet.add(timeStr.split()[0])

    # "for line in file_iter"重新打开进行迭代。
    file_iter.close()
    file_iter = open(fnameIn, 'rU')

    for line in file_iter:
        lineList = line.replace('\"','').replace(',',' , ').strip().split(',')
      
        if len(lineList) < max(paraList) :                               # 字段不全，筛选 # 两个if放在一起就不灵？不是'与'吗
            continue
        if not lineList[timeIndex].strip() and not lineList[moduleIndex1].strip() and not lineList[moduleIndex2].strip()  :     # 关键字段为空，筛选
            continue

        timeStr =  lineList[timeIndex].strip()
 
        if pattern1.match(timeStr):
            timeArray = time.strptime(timeStr, "%Y-%m-%d %H:%M:%S")               # 时间数组
            timeArrayInitial = time.strptime(min(list(set(timeSet))), "%Y-%m-%d") # 文件多个日期，取最小日期00:00:00的时间数组
        elif pattern2.match(timeStr):
            timeArray = time.strptime(timeStr, "%Y/%m/%d %H:%M")               # 时间数组
            timeArrayInitial = time.strptime(min(list(set(timeSet))), "%Y/%m/%d") # 文件多个日期，取最小日期00:00:00的时间数组
        elif pattern3.match(timeStr):
            timeArray = time.strptime(timeStr, "%Y/%m/%d %H:%M:%S")               # 时间数组
            timeArrayInitial = time.strptime(min(list(set(timeSet))), "%Y/%m/%d") # 文件多个日期，取最小日期00:00:00的时间数组
        else:
            # print "Bad time!"
            continue

        
        timeInt = int(time.mktime(timeArray))                                 # 时间戳--> timeStamp
        timeIntInitial = int(time.mktime(timeArrayInitial))                   # 文件多个日期，取最小日期00:00:00的时间戳--> timeStamp
    
        # for tVal in range(timeIntInitial, timeIntInitial + 86400):
        index = (timeInt-timeIntInitial) % ((86400/window)*len(timeSet))        # 86400/window = 288 # print index
        
        if lineList[moduleIndex2].strip() and lineList[moduleIndex1].strip() and lineList[moduleIndex3].strip():
            tmpStr = lineList[moduleIndex3].strip() + "`" +lineList[moduleIndex1].strip() + "`" +lineList[moduleIndex2].strip()
            transList[index].append(tmpStr)
        
    file_iter.close()

    if not transList:
        print "The transList is null!"
    else:
        print "The count of T (include null line): " + str(len(transList))

    # 将读取到的<key, value> --> (Tid, itemSet)存入文件。 
    for k,v in transList.items():
        filter(None,v)                               # 去空值；
        v = map(lambda x: x.strip(), v)              # 去空格；
        v = list(set(v))                             # 去重复值；
        v = ','.join(v).strip().lstrip(',').rstrip(',').strip()  #.decode('gb2312').encode('gbk')  # 删除前面的','，并转化为字符串。
        if v:
            fw.write(v +'\n')
    
    fw.close()
    return outputPath

if __name__ == "__main__":

    start = time.clock()
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

    (options, args) = optparser.parse_args()          #zi：options.input 、 options.mins 、 options.minC  

    inFile = None
    if options.input is None:
            inFile = sys.stdin
    elif options.input is not None:
            tsFile = dataTransform2(options.input)
            inFile = dataFromFile(tsFile)      #zi (options, args) = optparser.parse_args() 中 options.input，输入
    else:
            print 'No dataset filename specified, system with exit\n'
            sys.exit('System will exit')

    minSupport = options.minS
    minConfidence = options.minC


    items, rules = runApriori(inFile, minSupport, minConfidence)    #zi 接受三个参数的输入值：数据集、最小支持度、置信度

    fileFIs = '-FIMs.txt'
    fileRules = '-Rules.txt'
    printResults(items, rules, fileFIs, fileRules)

    elapsed = (time.clock() - start)
    print("Time used: %.2fs" % elapsed)
