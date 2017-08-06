import json
import pymysql

import utils
import log


class sql:
	def __init__(self, host, user, pwd, base):
		self.conn = None
		self.cursor = None

		try:
			self.conn = pymysql.connect(host=host, port=3306, user=user, passwd=pwd, db=base, autocommit=True)
			self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
		except Exception as e:
			log.error('mysql connection failed: %s' % (e,))

	def __del__(self):
		try:
			if self.cursor is not None:
				self.cursor.close()
			if self.conn is not None:
				self.conn.close()
		except:
			pass

	def __exec(self, query, data=None):
		self.conn.ping(reconnect=True)
		if self.cursor is None:
			return False
		if data is None:
			self.cursor.execute(query)
		else:
			self.cursor.execute(query, data)
		return True

	def __fetchone(self):
		self.conn.ping(reconnect=True)
		if self.cursor is None:
			return None
		return self.cursor.fetchone()

	def __fetchall(self):
		self.conn.ping(reconnect=True)
		if self.cursor is None:
			return None
		return self.cursor.fetchall()

	def __expand(self, item):
		if item is None:
			return None
		try:
			item['metadata'] = json.loads(item['metadata'])
		except:
			pass

		path = utils.resolve_paths(item['path'])
		if path is not None:
			item['path'] = path

		return item

	def get(self, _id):
		query = 'select * from files where id = %s'
		self.__exec(query, (_id,))
		item = self.__fetchone()

		item = self.__expand(item)
		if item is None:
			log.log('Database entry vanished, is: %s' % (_id))

		return item

	def get_new(self):
		query = 'select * from files where status = %s'
		self.__exec(query, ('NEW',))
		item = self.__fetchall()
		return [self.__expand(i) for i in item]

	def get_slaves(self, _id):
		query = 'select * from files where master = %s'
		self.__exec(query, (_id,))
		items = self.__fetchall()

		result = []
		for item in items:
			result.append(self.__expand(item))
		return result

	def insert(self, path, status, metadata, master=None, chunkid=None, video=None):
		query = 'insert into files(path, status, metadata, master, chunkid, video) values(%s, %s, %s, %s, %s, %s)'
		self.__exec(query, (path, status, json.dumps(metadata), master, chunkid, video))
		return self.cursor.lastrowid

	def count_slaves(self, _id, status):
		query = 'select count(*) as nb from files where master = %s and status != %s'
		self.__exec(query, (_id, status))
		return self.__fetchone()['nb']

	def update(self, _id, status, metadata=None, hostname=None, masterid=False):
		query = 'update files set status = %s where id = %s'
		self.__exec(query, (status, _id))

		if masterid is True:
			query = 'update files set status = %s where master = %s'
			self.__exec(query, (status, _id))

		if metadata is not None:
			query = 'update files set metadata = %s where id = %s'
			self.__exec(query, (json.dumps(metadata), _id))

		query = 'update files set hostname = %s where id = %s'
		if hostname is not None:
			self.__exec(query, (hostname, _id))
		else:
			self.__exec(query, (None, _id))

	def set_thumbs(self, masterid, thumbs):
		thumbs = json.dumps(thumbs)
		query = 'update files set thumbs = %s where id = %s'
		self.__exec(query, (thumbs, masterid))

		query = 'update files set thumbs = %s where master = %s'
		self.__exec(query, (thumbs, masterid))

	def delete(self, video):
		query = 'delete from files where video = %s'
		self.__exec(query, (video,))

	def dump(self):
		query = 'select id, status from video'
		self.__exec(query)
		return self.__fetchall()
