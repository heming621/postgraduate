#-*- coding:utf8 -*-
from collections import defaultdict
from itertools import combinations
class HashTree:
    def __init__(self, items, lenItem, level):    #zi items-->[[one trans]]一条事务各个项，lenItem-->项集长度，level-->树的高度；
        self.length = lenItem
        self.items = items
        # print "init_self.items:%s" % self.items
        # Three sons
        self.leftChild = None
        self.midChild = None
        self.rightChild = None
        # the digital 0 stands for the first level
        self.makeUpTree(level)

    # Make up the hash tree, it is a recursive process
    def makeUpTree(self, level):
        #zi 终止条件，如果：树的高度level== 项集的长度-1；都是从0开始，如0~3的树高取值，self.length.#zi 因为length从0开始，所以level也是从0开始。 # the terminal condition
        if level == self.length - 1:
            # temp store the k-item
            tempbucket = []                           #zi 如果【selfitems--> many N-candidates】，似乎也是可以；即将候选项集存储到哈希树中，下面看看怎生匹配...
            for subitem in self.items:                #zi items--> one trans，[[]]形式；如[['1', '10', '12', '15', '17', '19', '2', '21', '22', '26', '28', '30', '32', '4', '6', '9']] # print "self.items:%s\n"%self.items
                prefixItem = subitem[:level]          #zi 假设是第三层，即level=2，则前两项固定，如['1','2','5','8','9']；此时tempbucket应该存的是['1','2','5']、['1','2','8']、['1','2','9']、...
                sufixItem = subitem[level:]           #如果length==2(N)，先取1(N-1)项放prefixItem，再依次遍历剩下的每一项，和prefixItem组合成2(N)项集。# print "level:%s, prefixItem:%s, sufixItem:%s" % (level, prefixItem, sufixItem)
                for sitem in sufixItem:               #zi subitem[:level]+['X']；如trans[['1','2','5','8','9','11']]，如果三项候选集集，则length=3，level=2，对应叶子节点存储：subitem[:2] + for item in subitem[2:] = ['1','2']+['5'] = ['1','2']+['8'] = ... = ['1','2','5']、['1','2','8']、['1','2','9']、...
                    tempbucket.append(prefixItem + [sitem])
            self.items = tempbucket                   #zi 此时，[[trans]]-->[[N项集],[N项集],[N项集],...] # print "tempbucket:%s\n"%tempbucket
        else:
            leftItems = []
            midItems = []
            rightItems = []    
            for subitem in self.items:  
                numPre = len(subitem) - self.length + 1
                # print "  level:%s, numPre:%s, subitem:%s, length:%s"%(level, numPre, subitem, self.length)  
                for index in range(level, level+numPre):                  #zi level树高。level+numPre表示一直到剩余几个项刚好组成候选项集长度即可。# get the subitem. for example: abcd->abcd, bcd, cd
                    tempItem = subitem[:level] + subitem[index:]          #zi 至第2（N）层，此时项集的第1（N-1）项是确定是哪个节点的(左中右)；从第N开始考虑即可。每次往后挪一个位置，范围为[level, level+numPre]，即[当前树高, 当前树高起的其余项（直至节点处理的‘项集’长度-候选项集长度）]。
                    # print "    subitem[:%s]:%s, subitem[%s:]:%s"%(level, subitem[:level], index, subitem[index:])
                    hashValue = int(subitem[index]) % 3                   #zi 查看每次取到的项集的开头项是什么值，依次添加到对应的子树节点中。# hash to the different bucket                    
                    if hashValue == 0:
                        leftItems.append(tempItem)
                    elif hashValue == 1:
                        midItems.append(tempItem)
                    else:
                        rightItems.append(tempItem)
            # judge whether the list is empty or not
            if len(leftItems) != 0:
                self.leftChild = HashTree(leftItems, self.length, level + 1)
            if len(midItems) != 0:
                self.midChild = HashTree(midItems, self.length, level + 1)
            if len(rightItems) != 0:
                self.rightChild = HashTree(rightItems, self.length, level + 1)

    def identifyCandidate(self, candidate, level):         #zi 参数：某个候选项集、树高？ level传入时为0；#zi 【该函数功能】查看candidate是否在root<->[[trans]]<->[[N项集],[N项集],[N项集],...]为根的哈希树中。
        if self.length == level + 1:                       #zi 如果候选项集长度如3，已经达到树的高度 0~2 2+1=3；#zi 三项候选集则到第三层(即level=2)停止、四项候选集则到第四层停止(即level=3时停止，其中level=0,1,2,3；四项候选集时length=4；)、... #
            # if candidate in self.items: 【修改如下】以防止trans->N-Candidates，出现因list有序而元素相同却无法匹配的情况。
            if set(candidate) in [set(val) for val in self.items]:                    #zi self.items原来是一条事务如[[trans]]现已变成[[N项集],[N项集],[N项集],...]。
                return True 
            else:
                return False
        else:
            hashvalue = int(candidate[level]) % 3
            # recursion process
            if hashvalue == 0:
                if self.leftChild == None:
                    return False
                ###|| Forget the condition of no son
                return self.leftChild.identifyCandidate(candidate, level + 1)
            elif hashvalue == 1:
                if self.midChild == None:
                    return False
                return self.midChild.identifyCandidate(candidate, level + 1)
            else:
                if self.rightChild == None:
                    return False
                return self.rightChild.identifyCandidate(candidate, level + 1)

# End for HashTree
'''
currentCSet: format:[['0', '3', '7'], ['0', '3', '8'],...]
root:
freqSet:
minSupport:
'''
def subsetV2(currentCSet, root, freqSet, minSupport):    # root<-->transactionList #自 freqSet全局变量，存储所有>1的项集。
    _itemSet = set()
    localSet = defaultdict(int)
    for cdd in currentCSet:
        level = 0
        if root.identifyCandidate(cdd, level):
            freqSet[frozenset(cdd)] += 1
            localSet[frozenset(cdd)] += 1

    for item, count in localSet.items():
        support = float(count)/10000                    #len(transactionList)
        if support >= minSupport:
            _itemSet.add(item)

    return _itemSet

def subsetV3(cddFromTrans, root, freqSet, minSupport):    #zi root<->[[N-Candidate],[N-Candidate],...]<->如[['368', '217'], ['217', '766'],...] # print("root.items:%s"%root.items) #zi freqSet全局变量，存储所有项集及其计数。
    _itemSet = set()
    localSet = defaultdict(int)
    for cdd in cddFromTrans:
        level = 0
        if root.identifyCandidate(cdd, level):
            freqSet[frozenset(cdd)] += 1
            localSet[frozenset(cdd)] += 1

    for item, count in localSet.items():
        support = float(count)/10000                    # len(transactionList)
        if support >= minSupport:
            _itemSet.add(item)

    return _itemSet
'''
currentCSet = returnItemsWithMinSupport(currentLSet,        #zi format:set(frozenset,frozenset,...)
                                        transactionList,    #zi format:list(frozenset,frozenset,...)
                                        minSupport,         #zi float
                                        freqSet)            #zi {itemSet:count, itemSet:count, ...}-- format:{frozenset(['267']): 223,...} 频繁项集计数；
currentLSet, transactionList, minSupport, freqSet --> currentCSet（局部变量，再添至全局变量）（返回格式：set(frozenset,frozenset,...)）
transList, candidateSet(全局->局部), lenItem, level --> candidateSet（全局）修改：将其放到一个全局变量。
'''
def getCandidate(kc):
    with open('./uniqAllCandidate.txt') as fr:
        for val in fr:
            valList = eval(val)
            if val and len(valList[3])==kc:
                return valList
            else:
                continue
    return 0
def getTrans():
    transList = []
    with open('./trans.txt') as fr:
        for val in fr:
            transList.append(eval(val))
    if not transList:
        return 0
    return transList


def main():
    freqSet = defaultdict(int)
    minSupport = 0.0
    with open('./trans_1lines.txt', 'r') as fr:
        for trans in fr:
            if not trans.strip():
                print("NoneType")
            else:
                transList = eval(trans)
                #
                tempList = []
                tempList.append(transList)
                # print "tempList:%s"%tempList  # tempList:[['1', '10', '12', '15', '17', '19', '2', '21', '22', '26', '28', '30', '32', '4', '6', '9']]
                lenItem = 3
                level = 0
                root = HashTree(tempList, lenItem, level)
                currentCSet = getCandidate(lenItem)                   # zi format:[['0', '3', '7'], ['0', '3', '8'],...]
                print("%s, 过滤前-currentCSet:"%(len(currentCSet)))
                currentLSet = subsetV2(currentCSet, root, freqSet, minSupport)
                print("%s, 过滤后-currentLSet:"%(len(currentLSet)))
def main2():
    freqSet = defaultdict(int)
    minSupport = 0.05
    currentCSet = []
    transList = getTrans()
    tempTransList = []
    with open('./uniq4Candidate.txt') as fr:
        for val in fr:
            currentCSet = eval(val)                         #zi （1）一次性读取所有4项候选集，于[['0', '10', '7', '8'], ['0', '10', '19', '7'],...]
            level = 0
            lenItem = len(currentCSet[0])
            root = HashTree(currentCSet, lenItem, level)       #zi （2）将四项候选集存储到哈希树
            for trans in transList:                     #zi （3）将事务数据库-->四项候选集形式[[],[],[],...]，匹配；输出满足条件的四项候选集（从trans的子集中来）。
                for t in combinations(trans, lenItem):
                    tempTransList.append(list(t))
            print("%s, Before filter:%s"%(len(currentCSet), currentCSet))
            currentLSet = subsetV3(tempTransList, root, freqSet, minSupport)
            print("%s, After filter:%s"%(len(currentLSet), currentLSet))
            break

if __name__ == '__main__':
    main2()