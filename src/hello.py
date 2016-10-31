#-*- coding:utf8 -*-
import cuckoofilter
def set2Str(cdd):
	return "_".join(cdd)



def main():
    with open('../data/thfile','r') as fr:
        for line in fr:
            pass # print line
    print("WHATTHEFUCK!") 
    ##
    fset1 = frozenset({'947', '893'})
    print set2Str(fset1), type(set2Str(fset1))


if __name__ == '__main__':
    main()