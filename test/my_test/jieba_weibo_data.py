#!/usr/bin/env python3

#from __future__ import print_function
import json, pymysql
import jieba
import sys, time
import re

class DataSource:
	FIELDS = ( "mid", "uid", "created_at", "text", "source", "original_pic", "geoid", "retweet_mid", "reposts_count", "comments_count", "attitudes_count", "pic_urls" )

	def read_msgs(fields = None, limit = None):
		return None

	@staticmethod
	def process_msg(msg):
		msg = list(msg)
		for i in range(len(msg)):
			if isinstance(msg[i], bytes):
				msg[i] = msg[i].decode('utf-8')
		return msg

class MySQLDataSource(DataSource):
	dbmodule = pymysql

	def __init__(self, host = 'localhost', port = 3306,
			database = 'weibo', user = 'root', password = ''):
		self.cnx = self.dbmodule.connect(host = host, port = port,
				database = database,
				user = user, passwd = password, charset = 'utf8')
		self.cursor = self.cnx.cursor()

	def read_msgs(self, fields = None, limit = None, offset = None):
		if not None is limit and limit <= 0:
			return None
		if None is fields:
			fields = DataSource.FIELDS
		statement = 'select {} from messages'.format(', '.join(fields))
		if limit:
			statement += ' limit {}'.format(limit)
			if offset:
				statement += ' offset {}'.format(offset)
		self.cursor.execute(statement)
		return self.cursor
	
# load dataset
mysql_config = json.load(open("weibo-data-digger-config.json", 'r'))['output']['mysql']
data_source = MySQLDataSource(**mysql_config)
	
# load user dictionary
jieba.load_userdict("../data/userdict.txt")

# get content by line
content = ''
for m in data_source.read_msgs(limit = 10):
	msg = data_source.process_msg(m)
	#print(repr(msg))
	#print(msg[3])
	#content += msg[3] + '\n'
	
	#url filter.
	substring = re.sub(r'(http|https|ftp)://\S+', '', msg[3])
	content += substring + '\n'
	
# cut by jieba 
#print(content, end='')
jieba.enable_parallel(4)

t1 = time.time()
words = "/ ".join(jieba.cut(content))

t2 = time.time()
tm_cost = t2-t1

log_f = open("output.txt","wb")
log_f.write(words.encode('utf-8'))

print 'speed' , len(content)/tm_cost, " bytes/second"

jieba.disable_parallel()