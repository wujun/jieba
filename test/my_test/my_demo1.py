import sys
import re
import jieba
import jieba.analyse
from optparse import OptionParser
from gensim import corpora, models, similarities
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

USAGE = "usage:    python extract_tags.py [file name] -k [top k] or \n python extract_tags.py [file name]"

parser = OptionParser(USAGE)
parser.add_option("-k", dest="topK")
opt, args = parser.parse_args()


if len(args) < 1:
    print USAGE
    sys.exit(1)


def data_fileter(msgs):
    filtered = ''
    strings = msgs.split('\n')
    for string in strings:
        text = string
        # Drop links temply
        text = re.sub(r'(http|https|ftp)://\S+', r'', text)
        # Drop @sombody temply
        text = re.sub(r'@\S+', r'', text)
        filtered += text + '\n'
    return filtered
    
# add user dict
jieba.load_userdict("userdict.txt")

file_name = args[0]

if opt.topK is None:
    topK = 10
else:
    topK = int(opt.topK)

content = open(file_name, 'rb').read()

# add data filter
filtered = data_fileter(content)

sentences = filtered.split('\n')

texts = []
for str in sentences:
    tags = jieba.analyse.extract_tags(str, topK=topK)
    texts.append(tags)
    #print ",".join(tags)

# only for display
for i in range(len(texts)):
    line = texts[i]
    str_line = ''
    for j in range(len(line)):
         str_line  += (line[j].encode("utf-8")) + ","
    print(str_line)

# tokenize
dictionary = corpora.Dictionary(texts)
dictionary.save('/tmp/weibo.dict')
corpus = [dictionary.doc2bow(text) for text in texts]
corpora.MmCorpus.serialize('/tmp/weibo.mm', corpus)
#print(corpus)
    
# create a transformation
tfidf = models.TfidfModel(corpus)
corpus_tfidf = tfidf[corpus]
#for doc in corpus_tfidf:
#    print(doc)

# LSI
lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=2)
corpus_lsi = lsi[corpus_tfidf]
#lsi.print_topics(2)
#for doc in corpus_lsi:
#    print(doc)

# init similarity query
sim_index = similarities.MatrixSimilarity(lsi[corpus])


# extreact key words
#tags = jieba.analyse.extract_tags(filtered, topK=topK)

#print ",".join(tags)