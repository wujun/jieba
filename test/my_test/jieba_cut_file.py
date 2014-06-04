import urllib2
import sys,time
import sys
sys.path.append("../../")
import jieba

jieba.enable_parallel(4)

url = sys.argv[1]
content = open(url,"rb").read()
t1 = time.time()
words = "/ ".join(jieba.cut(content))

t2 = time.time()
tm_cost = t2-t1

log_f = open("output.txt","wb")
log_f.write(words.encode('utf-8'))

print 'speed' , len(content)/tm_cost, " bytes/second"

jieba.disable_parallel()

