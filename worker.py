import json
import uuid

import amqp
import ceph
import config
import log
import ffmpeg
import utils

def job_exists(_id):
	data = {'id': _id, 'status': 'IS_VALID'}
	result = amqp.watcher.direct_reply_to(data)
	return result['status']

def __encode(path, metadata):
	encoded, ugly = ffmpeg.encode(path, metadata)

	if encoded is None:
		return False

	up = ceph.ceph()
	keyname = '%s-encoded' % (path.split('/')[-1:][0],)
	up.upload(encoded, keyname)

	keyname = '%s-ugly' % (keyname,)
	up.upload(ugly, keyname)
	up.sync()
	return True

def encode(body, tag):
	if job_exists(body['id']) is False:
		log.log('encode: ID %s is None, job dropped' % (body['id'],))
		amqp.worker.ack(tag)
		return

	amqp.watcher.notify(body['id'], 'ENCODING')

	result = __encode(body['path'], body['metadata'])

	data = {'id': body['id'], 'status': 'ENCODED'}
	amqp.watcher.pub(data, result is False)
	amqp.worker.ack(tag)

def __merge_ugly(path, video):
	files = []
	for i in path:
		files.append('%s-encoded-ugly' % (i,))

	ugly = ffmpeg.merge(files)
	if ugly is None:
		return False

	ceph.ceph(ugly, '%s.mp4' % (video,), public=False)
	return True

def merge_ugly(body, tag):
	if job_exists(body['id']) is False:
		log.log('merge_ugly: ID %s is None, job dropped' % (body['id'],))
		amqp.worker.ack(tag)
		return
	amqp.watcher.notify(body['id'], 'MERGING-UGLY')

	result = __merge_ugly(body['path'], body['video'])

	data = {'id': body['id'], 'status': 'MERGED-UGLY'}
	amqp.watcher.pub(data, result is False)
	amqp.worker.ack(tag)

def __thumb(path, video):
	files = []
	for i in path:
		files.append('%s-encoded-ugly' % (i,))

	png = ffmpeg.thumb(files)
	if png is None:
		return None

	up = ceph.ceph()
	png = sorted(png, key=str.lower)
	thumbnails = {}
	for i in png:
		out_png = '%s-%s-%s.png' % (video, png.index(i), uuid.uuid4())
		up.upload(i, out_png)
		thumbnails[png.index(i)] = out_png

	up.sync()
	thumbnails = thumbnails

	return thumbnails

def thumb(body, tag):
	if job_exists(body['id']) is False:
		log.log('thumb: ID %s is None, job dropped' % (body['id'],))
		amqp.worker.ack(tag)
		return
	amqp.watcher.notify(body['id'], 'THUMBING')

	thumbs = __thumb(body['path'], body['video'])
	data = {'id': body['id'], 'thumbnails': thumbs, 'status': 'THUMBED'}
	amqp.watcher.pub(data, thumbs is None)
	amqp.worker.ack(tag)

def do_job(ch, method, props, body):
	utils.setup_tmpdir(config.tmpdir)

	try:
		body = json.loads(body.decode('utf-8'))
	except:
		pass

	path = utils.resolve_paths(body['path'])
	if path is not None:
		body['path'] = path

	if body['status'] == 'TO_ENCODE':
		encode(body, method.delivery_tag)

	if body['status'] == 'TO_MERGE_UGLY':
		merge_ugly(body, method.delivery_tag)

	if body['status'] == 'TO_THUMB':
		thumb(body, method.delivery_tag)

def run():
	while True:
		try:
			amqp.worker.consume_forever(do_job)
		except Exception as e:
			log.log('Error found: %s' % (e,))

