import sys
import re
import jieba
import jieba.analyse
from optparse import OptionParser

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
    
file_name = args[0]

if opt.topK is None:
    topK = 10
else:
    topK = int(opt.topK)

content = open(file_name, 'rb').read()

# add data filter
filtered = data_fileter(content)

sentences = filtered.split('\n')
for str in sentences:
    tags = jieba.analyse.extract_tags(str, topK=topK)
    print ",".join(tags)
    
# extreact key words
#tags = jieba.analyse.extract_tags(filtered, topK=topK)

#print ",".join(tags)