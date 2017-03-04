import json
import operator
import os
import re
import sys
import time
import xml.etree.cElementTree as ET

import amqp
import ceph
import config
import ffmpeg
import ffprobe
import log
import utils

def split(_id, path, metadata, video):
	up = ceph.ceph()

	splitlist = ffmpeg.split(path, video, metadata)
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

# job must be chunkid-ordered
# TODO: cleanup
def merge(jobs):
	#shitty code: changed are inplace, this is not really handled..
	def fix_audio_xml(root, adaptationSet):
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
					lang = job['metadata'][streamid]['lang']
					AS.set('lang', lang)
					representation.set('lang', lang)
				except KeyError:
					pass
				AS.append(representation)

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
	up.upload(m3u8, '%s.m3u8' % (job['video'],), public=False)

	match = re.compile('.*\.ts$')
	for root, dirs, files in os.walk(config.tmpdir):
		for name in files:
			if re.match(match, name):
				up.upload(os.path.join(root, name), name)

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
		fix_audio_xml(root, adaptationSet)

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
		baseurl.text = '%s-%s-%s.vtt' % (job['video'], i['lang'], i['i'])

	tree.write(mpd)

	log.log('uploading playlist, subs and m4s')
	for i in vtts:
		up.upload(i['file'], '%s-%s-%s.vtt' % (job['video'], i['lang'], i['i']), public=False)

	meta = {}
	numb = 0
	for i in job['metadata']:
		if job['metadata'][i]['type'] != 'subtitle':
			meta[numb] = job['metadata'][i]
			numb += 1
	for i, _ in sorted(meta.items(), key=operator.itemgetter(0)):
		channel = meta[i]
		tmp = '%s-stream%s.m4s' % (job['video'], i)
		up.upload('%s/%s' % (config.tmpdir, tmp), tmp, public=False)
	up.upload(mpd, '%s.mpd' % (job['video'],), public=False)
	up.sync()

	duration = ffprobe.get_duration(bigfile)
	return duration

def do_job():
	utils.setup_tmpdir(config.tmpdir)

	method, props, body = amqp.master.get()
	if method is not None and body is not None:
		status = body['status']
		_id = body['id']

		if status == 'TO_SPLIT':
			amqp.watcher.notify(_id, 'SPLITING')
			success = split(_id, body['path'], body['metadata'], body['video'])
			data = {'id': _id, 'status': 'SPLITED'}
			amqp.watcher.pub(data, success is False)
			try:
				os.remove(body['path'])
			except FileNotFoundError:
				pass

		elif status == 'TO_PROBE':
			amqp.watcher.notify(_id, 'PROBING')
			metadata = ffprobe.ffprobe(body['path'])
			data = {'id': _id, 'metadata': metadata, 'status': 'PROBED'}
			amqp.watcher.pub(data, metadata is None)

		elif status == 'TO_MERGE':
			amqp.watcher.notify(_id, 'MERGING')
			duration = merge(body['job'])
			data = {'id': _id, 'status': 'MERGED', 'duration': duration}
			amqp.watcher.pub(data, duration is None)

		else:
			log.error('Bug found: unknown message received: %s' % (body,))
		amqp.master.ack(method.delivery_tag)
		return

	time.sleep(1)

def run():
	while True:
		try:
			do_job()
		except Exception as e:
			log.log('Error found: %s' % (e,))

