import os
import math
import io
import multiprocessing
from multiprocessing import Pool
import collections
import log

import boto
import boto.s3.connection
import config


class ceph:
	def __init__(self, filename=None, keyname=None, public=True):
		self.__connect()
		self.bucket = self.__get_bucket()
		self.uploads = collections.OrderedDict()
		self.pool = Pool(processes=24)

		if filename is not None and keyname is not None:
			self.upload(filename, keyname, public=public)
			self.sync()
		pass

	def __del__(self):
		try:
			self.pool.close()
			self.pool.join()
		except:
			pass

	def __connect(self):
		self.conn = boto.connect_s3(
			aws_access_key_id=config.key,
			aws_secret_access_key=config.secret,
			host=config.ceph_fqdn,
			is_secure=True,
			calling_format=boto.s3.connection.OrdinaryCallingFormat(),
		)

	def __create_bucket(self):
		self.bucket = self.conn.create_bucket(config.bucket)
		from boto.s3.cors import CORSConfiguration
		cors_cfg = CORSConfiguration()
		cors_cfg.add_rule(['GET',], '*', allowed_header='*')
		cors_cfg.add_rule('GET', '*')
		self.bucket.set_cors(cors_cfg)

	def __get_bucket(self):
		try:
			self.bucket = self.conn.get_bucket(config.bucket)
		except boto.exception.S3ResponseError:
			self.__create_bucket()
		return self.bucket

	def delete(self, key):
		self.bucket.delete_key(key)

	def set_public(self, key):
		log.log('setting key %s as public-read' % (key,))
		key = self.bucket.get_key(key)
		key.set_canned_acl('public-read')

	def init_mp(self, keyname):
		self.bucket.delete_key(keyname)
		return self.bucket.initiate_multipart_upload(keyname)

	def upload(self, filename, keyname, callback=None, args=None, public=True):
		log.log('uploading %s as %s' % (filename, keyname))
		self.sync(True)

		source_size = os.stat(filename).st_size
		chunk_size = 41943040 #same as rgw strip size
		chunk_count = int(math.ceil(source_size / float(chunk_size)))

		if chunk_count > 1:
			mp = self.init_mp(keyname)
			offlist = []

			for i in range(chunk_count):
				offset = chunk_size * i
				bytes = min(chunk_size, source_size - offset)
				offlist.append({'offset': offset,
						'bytes': bytes,
						'i': i,
						'path': filename,
						'mp': mp
						})

			map_async = self.pool.map_async(upload_part, offlist)
		else:
			mp = None
			self.bucket.delete_key(keyname)
			key = self.bucket.new_key(keyname)

			data = {'path': filename,
				'key': key}
			map_async = self.pool.map_async(upload_file, [data])

		try:
			map_async.get(0.001)
		except multiprocessing.context.TimeoutError:
			pass
		self.uploads[keyname] = {'mp': mp,
				    'callback': callback,
				    'args': args,
				    'map_async': map_async,
				    'completed': False,
				    'public': public,
				   }

	def end_upload(self, keyname):
		item = self.uploads[keyname]
		if item['mp'] is not None:
			log.log('ending mp: %s' % (keyname))
			item['mp'].complete_upload()

		if item['public'] is True:
			self.set_public(keyname)
		if item['callback'] is not None:
			item['callback'](item['args'])

	#you MUST NOT use this object after sync(lazy=False)
	def sync(self, lazy=False):
		if lazy is True:
			for keyname in self.uploads:
				item = self.uploads[keyname]
				if item['completed'] is True:
					continue
				try:
					self.uploads[keyname]['map_async'].get(0.001)
					self.uploads[keyname]['completed'] = True
				except multiprocessing.context.TimeoutError:
					continue
				self.end_upload(keyname)
			return
		self.pool.close()
		for keyname in self.uploads:
			item = self.uploads[keyname]
			if item['completed'] is True:
				continue
			self.uploads[keyname]['map_async'].get()
			self.end_upload(keyname)
		self.pool.join()


def upload_part(x):
	log.log('upload part:', x)
	with open(x['path'], 'rb') as f:
		f.seek(x['offset'])
		output = io.BytesIO()
		output.write(f.read(x['bytes']))
		output.seek(0)
		x['mp'].upload_part_from_file(output, part_num=x['i'] + 1)
		output.close()
	return x['mp']


def upload_file(x):
	log.log('upload file: %s' % (x,))
	with open(x['path'], 'rb') as f:
		x['key'].set_contents_from_file(f)
