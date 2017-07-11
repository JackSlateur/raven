import json
import pymysql
import requests
import time

import log
import config
import amqp

from sql import sql


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


def __encoded(body, status):
	db.update(body['id'], status)
	item = db.get(body['id'])
	if item is None:
		log.log('Database entries vanished, id: %s' % (body['id'],))
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


def __merged_ugly(body, status):
	db.update(body['id'], status, masterid=True)

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


def __thumbed(body, status):
	db.update(body['id'], status, masterid=True)
	db.set_thumbs(body['id'], body['thumbnails'])

	job = db.get_slaves(body['id'])
	amqp.master.pub({'status': 'TO_MERGE', 'id': body['id'], 'job': job})


def __probed(body, status):
	db.update(body['id'], status, metadata=body['metadata'], masterid=True)
	job = db.get(body['id'])
	if job is None:
		log.log('Database entries vanished, id: %s' % (body['id'],))
		return


	data = {'status': 'TO_SPLIT', 'id': body['id'], 'path': job['path'], 'metadata': body['metadata'], 'video': job['video']}
	amqp.master.pub(data, priority=True)


def __merged(body):
	job = db.get(body['id'])
	if job is None:
		log.log('Database entries vanished, id: %s' % (body['id'],))
		return

	call_upstream(True, body['duration'], job['video'], job['metadata'], job['thumbs'])
	db.delete(job['id'], masterid=True)


def __insert_chunk(body):
	item = db.get(body['id'])
	if item is None:
		log.log('Database entries vanished, id: %s' % (body['id'],))
		return
	path = json.dumps(body['path'])
	inserted_id = db.insert(path, 'SPLITED', item['metadata'], body['id'], body['chunkid'], item['video'])
	amqp.worker.pub({'status': 'TO_ENCODE', 'metadata': item['metadata'], 'path': path, 'id': inserted_id})


def scan_events():
	while True:
		method, props, body = amqp.watcher.get()
		if method is None or body is None:
			return
		status = body['status']

		if status == 'ENCODING':
			db.update(body['id'], status, hostname=body['hostname'])
		elif status in ('PROBING', 'SPLITING', 'MERGING', 'MERGING-UGLY', 'THUMBING'):
			db.update(body['id'], status, hostname=body['hostname'], masterid=True)
		elif status == 'SPLITED':
			db.update(body['id'], status)
		elif status == 'ENCODED':
			__encoded(body, status)
		elif status == 'MERGED-UGLY':
			__merged_ugly(body, status)
		elif status == 'THUMBED':
			__thumbed(body, status)
		elif status == 'PROBED':
			__probed(body, status)
		elif status == 'MERGED':
			__merged(body)
		elif status == 'SPLITED_CHUNK':
			__insert_chunk(body)
		elif status == 'FAILED':
			item = db.get(body['id'])
			if item is None:
				log.log('Database entries vanished, id: %s' % (body['id'],))
			else:
				video = item['video']
				call_upstream(False, None, video)
				db.delete(item['master'], masterid=True)
		elif status == 'IS_VALID':
			item = db.get(body['id'])
			if item is None:
				amqp.watcher.pub_reply({'status': False}, props)
			else:
				amqp.watcher.pub_reply({'status': True}, props)
		else:
			log.error('Bug found: unknown message received: %s' % (body,))
		amqp.watcher.ack(method.delivery_tag)


def run():
	global db
	db = sql(config.db_host, config.db_user, config.db_passwd, config.db_db)
	while True:
		try:
			scan_new()
			scan_events()
			time.sleep(1)
		except (pymysql.err.OperationalError, pymysql.err.InternalError, pymysql.err.ProgrammingError) as e:
			log.log(e)
