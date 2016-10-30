#-*- coding:utf8 -*-
print "hello"
def main():
    with open('../data/thfile','r') as fr:
        for line in fr:
            print line
            
if __name__ == '__main__':
    main()