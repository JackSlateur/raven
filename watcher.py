import json
import pymysql
import requests
import time
from collections import namedtuple

import log
import config
import amqp

from sql import sql

Call = namedtuple('Call', ['update', 'function'])


def call_upstream(result, duration, _id, metadata=None, thumbnails=None):
	if result is False:
		data = {'status': 'FAILED'}
	else:
		data = {'status': 'SUCCESS',
			'metadata': json.dumps(metadata),
			'duration': duration,
			'thumbnails': thumbnails
			}

	for i in range(1, 10):
		result = requests.post(config.upstream_uri % (_id,), json=data)
		if result.status_code == 200:
			break
		time.sleep(5)
	try:
		del data['thumbnails']
	except KeyError:
		pass
	db.delete(_id)
	log.log('call_upstream (thumbnails removed): %s %s -> %s' % (_id, data, result))


def scan_new():
	for job in db.get_new():
		data = {'status': 'TO_PROBE',
			'id': job['id'],
			'path': job['path'],
			}
		result = amqp.master.pub(data, priority=True)
		if result is not False:
			db.update(job['id'], 'SEEN')


def __encoded(body):
	item = db.get(body['id'])
	if item is None:
		return

	master = item['master']
	nb = db.count_slaves(master, 'ENCODED')
	if nb == 0: #All chunks are encoded, keep going
		db.update(master, 'ENCODED')
		slaves = db.get_slaves(master)

		path = [i['path'] for i in slaves]

		data = {'status': 'TO_MERGE_UGLY',
			'path': path,
			'id': master,
			'video': slaves[0]['video'],
			}
		amqp.worker.pub(data)


def __merged_ugly(body):
	slaves = db.get_slaves(body['id'])
	if len(slaves) == 0:
		log.log('No slaves found, job %s is probably canceled' % (body['id'],))
		return
	path = [i['path'] for i in slaves]

	data = {'status': 'TO_THUMB',
		'path': path,
		'id': body['id'],
		'video': slaves[0]['video'],
		}
	amqp.worker.pub(data)


def __thumbed(body):
	db.set_thumbs(body['id'], body['thumbnails'])

	job = db.get_slaves(body['id'])
	amqp.master.pub({'status': 'TO_MERGE', 'id': body['id'], 'job': job})


def __probed(body):
	job = db.get(body['id'])
	if job is not None:
		data = {'status': 'TO_SPLIT', 'id': body['id'], 'path': job['path'], 'metadata': body['metadata'], 'video': job['video']}
		amqp.master.pub(data, priority=True)


def __merged(body):
	job = db.get(body['id'])
	if job is not None:
		call_upstream(True, body['duration'], job['video'], job['metadata'], job['thumbs'])


def __insert_chunk(body):
	item = db.get(body['id'])
	if item is not None:
		path = json.dumps(body['path'])
		inserted_id = db.insert(path, 'SPLITED', item['metadata'], body['id'], body['chunkid'], item['video'])
		amqp.worker.pub({'status': 'TO_ENCODE', 'metadata': item['metadata'], 'path': path, 'id': inserted_id})


def __update_db(body, masterid=False):
	if 'hostname' in body:
		hostname = body['hostname']
		db.update(body['id'], body['status'], hostname=hostname, masterid=masterid)
	elif 'metadata' in body:
		metadata = body['metadata']
		db.update(body['id'], body['status'], metadata=metadata, masterid=masterid)
	else:
		db.update(body['id'], body['status'], masterid=masterid)


def __failed(body):
	item = db.get(body['id'])
	if item is not None:
		video = item['video']
		call_upstream(False, None, video)


def __is_valid(body):
	item = db.get(body['id'])
	if item is None:
		amqp.watcher.pub_reply({'status': False}, body['props'])
	else:
		amqp.watcher.pub_reply({'status': True}, body['props'])


def scan_events():
	calls = {
		'SPLITED': Call('single', None),
		'ENCODING': Call('single', None),
		'PROBING': Call('master', None),
		'SPLITING': Call('master', None),
		'MERGING': Call('master', None),
		'MERGING-UGLY': Call('master', None),
		'THUMBING': Call('master', None),
		'ENCODED': Call('single', __encoded),
		'MERGED-UGLY': Call('master', __merged_ugly),
		'THUMBED': Call('master', __thumbed),
		'PROBED': Call('master', __probed),
		'MERGED': Call(None, __merged),
		'SPLITED_CHUNK': Call(None, __insert_chunk),
		'FAILED': Call(None, __failed),
		'IS_VALID': Call(None, __is_valid),
	}

	while True:
		method, props, body = amqp.watcher.get()
		if method is None or body is None:
			return
		body['props'] = props

		if body['status'] not in calls:
			log.error('Bug found: unknown message received: %s' % (body,))
		else:
			call = calls[body['status']]
			if call.update == 'single':
				__update_db(body)
			elif call.update == 'master':
				__update_db(body, masterid=True)
			if call.function is not None:
				call.function(body)
		amqp.watcher.ack(method.delivery_tag)


def run():
	global db
	db = sql(config.db_host, config.db_user, config.db_passwd, config.db_db)
	while True:
		try:
			scan_new()
			scan_events()
			time.sleep(1)
		except Exception as e:
			log.log(e)
