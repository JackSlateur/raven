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

	if 'ugly' in config.outputs:
		keyname = '%s-ugly' % (keyname,)
		up.upload(ugly, keyname)

	up.sync()
	return True


def encode(body):
	if job_exists(body['id']) is False:
		log.log('encode: ID %s is None, job dropped' % (body['id'],))
		return

	amqp.watcher.notify(body['id'], 'ENCODING')

	result = __encode(body['path'], body['metadata'])

	data = {'id': body['id'], 'status': 'ENCODED'}
	amqp.watcher.pub(data, result is False)


def __merge_ugly(path, video):
	files = []
	for i in path:
		files.append('%s-encoded-ugly' % (i,))

	ugly = ffmpeg.merge(files)
	if ugly is None:
		return False

	ceph.ceph(ugly, '%s.mp4' % (video,), public=False)
	return True


def merge_ugly(body):
	if 'ugly' in config.outputs:
		if job_exists(body['id']) is False:
			log.log('merge_ugly: ID %s is None, job dropped' % (body['id'],))
			return

		amqp.watcher.notify(body['id'], 'MERGING-UGLY')

		result = __merge_ugly(body['path'], body['video'])
	else:
		result = True

	data = {'id': body['id'], 'status': 'MERGED-UGLY'}
	amqp.watcher.pub(data, result is False)


def __thumb(path, video):
	files = []
	for i in path:
		if 'ugly' in config.outputs:
			files.append('%s-encoded-ugly' % (i,))
		else:
			files.append('%s-encoded' % (i,))

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


def thumb(body):
	Id = body['id']
	if 'thumbs' not in config.outputs:
		thumbs = {}
	else:
		if job_exists(Id) is False:
			log.log('thumb: ID %s is None, job dropped' % (Id,))
			return

		amqp.watcher.notify(Id, 'THUMBING')
		thumbs = __thumb(body['path'], body['video'])

	data = {'id': Id, 'status': 'THUMBED', 'thumbnails': thumbs}
	amqp.watcher.pub(data, thumbs is None)


@amqp.ack
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
		encode(body)

	if body['status'] == 'TO_MERGE_UGLY':
		merge_ugly(body)

	if body['status'] == 'TO_THUMB':
		thumb(body)

	return amqp.worker, method.delivery_tag


def run():
	amqp.worker.consume_forever(do_job)
