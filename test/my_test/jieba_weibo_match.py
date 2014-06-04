#!/usr/bin/env python3

from __future__ import print_function
import sys, abc, json, re, pymysql, nltk
import jieba.analyse

def mysql_escape_like(s):
	s = s.replace('\\', '\\\\')
	s = s.replace('%', '\\%')
	s = s.replace('_', '\\_')
	return s

class DataSource:
	'''Source of weibo data.'''

	__metaclass__ = abc.ABCMeta

	FIELDS = ( "mid", "uid", "created_at", "text", "source", "original_pic", "geoid", "retweet_mid", "reposts_count", "comments_count", "attitudes_count", "pic_urls" )
	encoding = 'utf-8'

	def read_msgs(fields = None, limit = None):
		return None

	def find_msgs(self, text, fields = None, limit = None, offset = None):
		return None

	@classmethod
	def process_msg(cls, msg):
		msg = list(msg)
		for i in range(len(msg)):
			if isinstance(msg[i], bytes):
				msg[i] = msg[i].decode(cls.encoding)
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

	def find_msgs(self, text_list, fields = None, limit = None, offset = None):
		if not None is limit and limit <= 0:
			return None
		if None is fields:
			fields = DataSource.FIELDS
		statement = 'select {} from messages where {}'.format(', '.join(fields),
				' or '.join([ 'text like %s' ] * len(text_list)))
		if limit:
			statement += ' limit {}'.format(limit)
			if offset:
				statement += ' offset {}'.format(offset)
		args = list()
		for t in text_list:
			args.append('%' + mysql_escape_like(t) + '%')
		self.cursor.execute(statement, tuple(args))
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

def remove_meaningless(msgs):
	for i in range(len(msgs)):
		text = msgs[i]

		# Drop links
		text = re.sub(r'(http|https|ftp)://\S+', r'', text)
		# Drop @somebody
		text = re.sub(r'@\S+', r'', text)

		msgs[i] = text

def test_keywords(msgs):
	for text in msgs:
		print('Entry:', text)
		if not text.strip():
			continue
		tags = jieba.analyse.extract_tags(text, topK = 5)
		for t in tags:
			print('  Tag:', t)
			syns_dict = wn_source.get_syn_chinese(t)
			for synset_id in syns_dict:
				print(u'    {:<10}: {}'.format(synset_id, ', '.join(syns_dict[synset_id])).encode(sys.stdout.encoding))
				for word in syns_dict[synset_id]:
					for rec in data_source.find_msgs([ word ], [ 'text'], limit = 1):
						print(u'      Your random quote of "{}": {}'.format(word, rec[0].decode(data_source.encoding)).encode(sys.stdout.encoding))

def test_keywords2(msgs):
	for text in msgs:
		print('Entry:', text)
		tags = list(jieba.analyse.extract_tags(text, topK = 5))
		print('  Tags:', ', '.join(tags))
		related_msgs = [ r[0].decode(data_source.encoding)
				for r in data_source.find_msgs(tags, [ 'text']) ]
		related_info = list()
		for rtext in related_msgs:
			cut = list(jieba.cut(rtext))
			score = 0
			for t in tags:
				if t in cut:
					score += 1
			related_info.append(dict(text = rtext, score = score))
		related_info.sort(key = lambda rinfo: rinfo['score'], reverse = True)
		for i in range(5):
			rinfo = related_info[i]
			if rinfo['text'] == text:
				continue
			print(u'  Match {:1} ({:3}): {}'.format(i, rinfo['score'], rinfo['text']))

mysql_config = json.load(open("weibo-data-digger-config.json", 'r'))['output']['mysql']
data_source = MySQLDataSource(**mysql_config)

wn_mysql_config = json.load(open("wordnet-config.json", 'r'))['mysql']
wn_source = WNSourceMySQL(**wn_mysql_config)

records = list(data_source.read_msgs(fields = [ 'text' ], limit = 100))
msgs = [ r[0].decode(data_source.encoding) for r in records ]
remove_meaningless(msgs)
# msgs = [ list(jieba.cut(m)) for m in msgs ]
test_keywords2(msgs)
