#-*- coding:utf8 -*-
from collections import defaultdict
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
    def makeUpTree(self, level):       #zi 终止条件，如果：树的高度level== 项集的长度-1；都是从0开始，如0~3的树高取值，self.length.#zi 因为length从0开始，所以level也是从0开始。 # the terminal condition
        if level == self.length - 1:
            # temp store the k-item
            tempbucket = []
            for subitem in self.items:                #zi items--> one trans，[[]]形式；如[['1', '10', '12', '15', '17', '19', '2', '21', '22', '26', '28', '30', '32', '4', '6', '9']]
                # print "self.items:%s\n"%self.items
                prefixItem = subitem[:level]
                sufixItem = subitem[level:]           #如果length==2(N)，先取1(N-1)项放prefixItem，再依次遍历剩下的每一项，和prefixItem组合成2(N)项集。
                # print "level:%s, prefixItem:%s, sufixItem:%s" % (level, prefixItem, sufixItem)
                for sitem in sufixItem:
                    tempbucket.append(prefixItem + [sitem])
            ###|| It is also a big mistake, I used list(sitem) ago, and is will convert ths string '11' to ['1', '1']
            self.items = tempbucket                   #zi 此时，[[trans]]-->[[N项集],[N项集],[N项集],...]
            # print "tempbucket:%s\n"%tempbucket
        else:
            # point the left , mid and right storage
            leftItems = []
            midItems = []
            rightItems = []    
            for subitem in self.items:  
                numPre = len(subitem) - self.length + 1
                # print "  level:%s, numPre:%s, subitem:%s, length:%s"%(level, numPre, subitem, self.length)  
                for index in range(level, level+numPre):                  #zi level树高。level+numPre表示一直到剩余几个项刚好组成候选项集长度即可。
                    # get the subitem. for example: abcd->abcd, bcd, cd
                    tempItem = subitem[:level] + subitem[index:]          #zi 至第2（N）层，此时项集的第1（N-1）项是确定是哪个节点的(左中右)；从第N开始考虑即可。每次往后挪一个位置，范围为[level, level+numPre]，即[当前树高, 当前树高起的其余项（直至节点处理的‘项集’长度-候选项集长度）]。
                    # print "    subitem[:%s]:%s, subitem[%s:]:%s"%(level, subitem[:level], index, subitem[index:])
                    hashValue = int(subitem[index]) % 3                   #zi 查看每次取到的项集的开头项是什么值，依次添加到对应的子树节点中。
                    # hash to the different bucket
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
    def identifyCandidate(self, candidate, level):         #zi 参数：某个候选项集、树高？ level传入时为0；
        if self.length == level + 1:                       #zi 如果候选项集长度如3，已经达到树的高度 0~2 2+1=3；
            if candidate in self.items:                    #zi self.items原来是一条事务如[[trans]]现已变成[[N项集],[N项集],[N项集],...]。
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
def subsetV2(currentCSet, root, freqSet, minSupport):    # root<-->transactionList
    currentLSet = set()
    localSet = defaultdict(int)
    for cdd in currentCSet:
        level = 0
        if root.identifyCandidate(cdd, level):
            freqSet[frozenset(cdd)] += 1
            localSet[frozenset(cdd)] += 1

    for item, count in localSet.items():
        support = float(count)/10000                    #len(transactionList)
        if support >= minSupport:
            currentLSet.add(item)

    return currentLSet
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
            valList = eval(val)               #zi [[],[],...]
            if val and len(valList[3])==kc:
                return valList
            else:
                continue
    return 0

def main():
    freqSet = defaultdict(int)
    minSupport = 0.0
    with open('./trans_1lines.txt', 'r') as fr:
        for trans in fr:
            if not trans.strip():
                print("NoneType")
            else:
                # print trans
                transList = trans.strip('\n').lstrip('[\'').rstrip('\']').split("\', \'")
                transList = eval(trans)
                #
                tempList = []
                tempList.append(transList)
                # print "tempList:%s"%tempList  # tempList:[['1', '10', '12', '15', '17', '19', '2', '21', '22', '26', '28', '30', '32', '4', '6', '9']]
                lenItem = 3
                level = 0
                root = HashTree(tempList, lenItem, level)
                currentCSet = getCandidate(lenItem)
                print("%s, currentCSet:"%(len(currentCSet)))
                currentLSet = subsetV2(currentCSet, root, freqSet, minSupport)
                print("%s, currentLSet:"%(len(currentLSet)))
                # print "root.items:%s\n" % root.items
                # print "root.leftChild:%s\n" % root.leftChild.items
                # print "root.midChild:%s\n" % root.midChild.items
                # print "root.rightChild:%s\n" % root.rightChild.items

if __name__ == '__main__':
	main()