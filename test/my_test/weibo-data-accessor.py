#!/usr/bin/env python3

from __future__ import print_function
import sys, abc, json, pymysql
import jieba.analyse

class DataSource:
	'''Source of weibo data.'''

	__metaclass__ = abc.ABCMeta

	FIELDS = ( "mid", "uid", "created_at", "text", "source", "original_pic", "geoid", "retweet_mid", "reposts_count", "comments_count", "attitudes_count", "pic_urls" )

	def read_msgs(fields = None, limit = None):
		return None

	def find_msgs(self, text, fields = None, limit = None, offset = None):
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

	def find_msgs(self, text, fields = None, limit = None, offset = None):
		if not None is limit and limit <= 0:
			return None
		if None is fields:
			fields = DataSource.FIELDS
		statement = 'select {} from messages where text like %s'.format(', '.join(fields))
		if limit:
			statement += ' limit {}'.format(limit)
			if offset:
				statement += ' offset {}'.format(offset)
		# TODO: Handle the case when % exists in text
		self.cursor.execute(statement, ('%' + text + '%', ))
		return self.cursor

class WNSource:
	__metaclass__ = abc.ABCMeta

	def get_syn_chinese(self, word):
		return dict()

class WNSourceMySQL(WNSource):
	dbmodule = pymysql

	def __init__(self, host = 'localhost', port = 3306,
			database = 'wordnet', user = 'root', password = ''):
		self.cnx = self.dbmodule.connect(host = host, port = port,
				database = database,
				user = user, passwd = password, charset = 'utf8')
		self.cursor = self.cnx.cursor()

	def get_syn_chinese(self, word):
		if not isinstance(word, str) and not isinstance(word, unicode):
			raise Exception('Search word "{}" is not a string'.format(str))
		statement = 'select synset_id, chinese from wn_chinese where synset_id in (select distinct synset_id from wn_chinese where chinese = %s)'
		self.cursor.execute(statement, (word, ))
		result = dict()
		for rec in self.cursor:
			synset_id = rec[0]
			word = rec[1]
			if not synset_id in result:
				result[synset_id] = set()
			result[synset_id].add(word)
		return result

mysql_config = json.load(open("weibo-data-digger-config.json", 'r'))['output']['mysql']
data_source = MySQLDataSource(**mysql_config)

wn_mysql_config = json.load(open("wordnet-config.json", 'r'))['mysql']
wn_source = WNSourceMySQL(**wn_mysql_config)

for m in data_source.read_msgs(fields = [ 'text' ], limit = 100):
	# msg = data_source.process_msg(m)
	text = m[0]
	print('Entry:', text)
	tags = jieba.analyse.extract_tags(text, topK = 5)
	for t in tags:
		print('  Tag:', t)
		syns_dict = wn_source.get_syn_chinese(t)
		for synset_id in syns_dict:
			print(u'    {:<10}: {}'.format(synset_id, ', '.join(syns_dict[synset_id])).encode(sys.stdout.encoding))
			for word in syns_dict[synset_id]:
				for rec in data_source.find_msgs(word, [ 'text'], limit = 1):
					print(u'      Your random quote of "{}": {}'.format(word, rec[0].decode('utf-8')).encode(sys.stdout.encoding))
