import json
from sh import ffprobe as program

import utils
from log import log, warn

def ffprobe(filename):
	log('probing %s' % (filename))
	probe = program.bake('-show_error', '-show_streams', '-print_format', 'json')

	try:
		output = probe(filename)
	except Exception as e:
		err = json.loads(e.stdout.decode('utf-8'))
		warn('error on %s: %s' % (filename, err['error']['string']))
		return None

	output = json.loads(output.stdout.decode('utf-8'))
	result = {}
	for i in output['streams']:
		#Shitty stuff
		try:
			if i['codec_type'] == 'data':
				continue
		except KeyError:
			pass

		try:
			if i['codec_name'] in ('mjpeg', 'png', 'pgssub', 'dvd_subtitle', 'hdmv_pgs_subtitle'):
				continue
		except KeyError:
			pass

		stream = {
			'type': i['codec_type'],
			}

		try:
			i['tags'] = utils.CaseInsensCreepyDict(i['tags'])
			stream['lang'] = i['tags']['language']
		except KeyError:
			pass

		if i['codec_type'] == 'subtitle' and 'lang' not in stream:
			#unknown language, drop subs
			continue

		result[i['index']] = stream

	return result

def get_duration(filename):
	try:
		output = program('-show_programs', '-show_streams', '-print_format', 'json', filename)
		output = json.loads(output.stdout.decode('utf-8'))

		for i in output['programs']:
			try:
				end = i['end_time']
				start = i['start_time']
				return int(float(end) - float(start))
			except KeyError:
				pass

		# cannot found duration based on programs.. let's try streams
		for stream in output['streams']:
			try:
				return int(float(stream['duration']))
			except KeyError:
				pass
	except:
		pass
	raise Exception('Cannot find duration')
