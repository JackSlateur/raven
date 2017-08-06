import operator
import os
import re
import time
import xml.etree.cElementTree as ET
from collections import namedtuple

import amqp
import ceph
import config
import ffmpeg
import ffprobe
import log
import utils


Call = namedtuple('Call', ['begin', 'end', 'function', 'failure', 'result'])


def split(_id, body):
	up = ceph.ceph()

	splitlist = ffmpeg.split(body['path'], body['video'], body['metadata'])
	if len(splitlist) == 0:
		return False

	index = -1
	for i in splitlist:
		index += 1
		i = i.rstrip('\n\r')
		keyname = '%s-%s' % (i, index)
		path = '%s/%s' % (config.tmpdir, i)

		args = {'status': 'SPLITED_CHUNK',
			'path': {'type': 'ceph', 'filename': keyname},
			'id': _id,
			'chunkid': index,
			}

		up.upload(path, keyname, amqp.watcher.pub, args)

	up.sync()

	return True


# shitty code: changed are inplace, this is not really handled..
def fix_audio_xml(root, adaptationSet, metadata):
	root.remove(adaptationSet)
	for representation in adaptationSet:
		for baseurl in representation.iter('BaseURL'):
			match = re.match('[0-9]+-stream([0-9]+).m4s', baseurl.text)
			if not match:
				continue
			streamid = match.group(1)
			AS = ET.SubElement(root, 'AdaptationSet')
			AS.set('segmentAlignment', 'true')
			try:
				lang = metadata[streamid]['lang']
				AS.set('lang', lang)
				representation.set('lang', lang)
			except KeyError:
				pass
			AS.append(representation)


def fix_dash(mpd, vtts, video, metadata):
	tree = ET.parse(mpd)
	root = tree.getroot()
	ns = '{urn:mpeg:dash:schema:mpd:2011}'
	for elem in root.getiterator():
		if elem.tag.startswith(ns):
			elem.tag = elem.tag[len(ns):]

	for child in root:
		if child.tag == 'Period':
			root = child
			break

	for adaptationSet in root:
		attr = adaptationSet.attrib
		if 'contentType' not in attr or attr['contentType'] != 'audio':
			continue
		fix_audio_xml(root, adaptationSet, metadata)

	as_list = {}
	index = 0
	for i in vtts:
		index += 1

		AS = ET.SubElement(root, 'AdaptationSet')
		AS.set('mimeType', 'text/vtt')
		if i['lang'] in as_list:
			lang = '%s-%s' % (i['lang'], index)
		else:
			lang = i['lang']
			as_list[i['lang']] = True

		AS.set('lang', lang)
		repre = ET.SubElement(AS, 'Representation')
		repre.set('id', '%s' % (index,))
		repre.set('bandwidth', '256')
		repre.set('lang', lang)
		baseurl = ET.SubElement(repre, 'BaseURL')
		baseurl.text = '%s-%s-%s.vtt' % (video, i['lang'], i['i'])

	tree.write(mpd)


def upload_hls(upload, m3u8, video):
	upload.upload(m3u8, '%s.m3u8' % (video,), public=False)

	match = re.compile('.*\.ts$')
	for root, dirs, files in os.walk(config.tmpdir):
		for name in files:
			if re.match(match, name):
				upload.upload(os.path.join(root, name), name)


def upload_dash(upload, mpd, vtts, video, metadata):
	for i in vtts:
		upload.upload(i['file'], '%s-%s-%s.vtt' % (video, i['lang'], i['i']), public=False)

	meta = {}
	numb = 0
	for i in metadata:
		if metadata[i]['type'] != 'subtitle':
			meta[numb] = metadata[i]
			numb += 1
	for i, _ in sorted(meta.items(), key=operator.itemgetter(0)):
		tmp = '%s-stream%s.m4s' % (video, i)
		upload.upload('%s/%s' % (config.tmpdir, tmp), tmp, public=False)
	upload.upload(mpd, '%s.mpd' % (video,), public=False)


# job must be chunkid-ordered
def merge(_, body):
	jobs = body['job']
	files = []
	if len(jobs) == 0:
		return None
	for i in jobs:
		files.append('%s-encoded' % (i['path'],))

	job = jobs[0]

	try:
		bigfile, m3u8, mpd, vtts = ffmpeg.makePlaylists(files, job['video'], job['metadata'])
	except Exception as e:
		log.log('merge error on master %s : %s' % (job['master'], e.stderr))
		return None

	up = ceph.ceph()

	if m3u8 is not None:
		upload_hls(up, m3u8, job['video'])

	if mpd is not None and vtts is not None:
		fix_dash(mpd, vtts, job['video'], job['metadata'])
		upload_dash(up, mpd, vtts, job['video'], job['metadata'])

	up.sync()

	duration = ffprobe.get_duration(bigfile)
	return duration


def probe(_, body):
	return ffprobe.ffprobe(body['path'])


def do_job():
	utils.setup_tmpdir(config.tmpdir)

	calls = {
		'TO_SPLIT': Call('SPLITING', 'SPLITED', split, False, 'None'),
		'TO_PROBE': Call('PROBING', 'PROBED', probe, None, 'metadata'),
		'TO_MERGE': Call('MERGING', 'MERGED', merge, None, 'duration'),
	}

	method, props, body = amqp.master.get()
	if method is None or body is None:
		time.sleep(1)
		return

	status = body['status']
	_id = body['id']

	if status not in calls:
		log.error('Bug found: unknown message received: %s' % (body,))
		amqp.master.ack(method.delivery_tag)
		return

	call = calls[status]
	output = {'id': _id, 'status': call.end}

	amqp.watcher.notify(_id, call.begin)

	result = call.function(_id, body)
	output[call.result] = result
	if result is call.failure:
		output['status'] = 'FAILED'

	amqp.watcher.pub(output)
	amqp.master.ack(method.delivery_tag)

	if status == 'TO_SPLIT':
		try:
			os.remove(body['path'])
		except FileNotFoundError:
			pass


def run():
	while True:
		try:
			do_job()
		except Exception as e:
			log.log('Error found: %s' % (e,))
