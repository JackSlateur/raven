import operator
import uuid
import os
import re

import config
import log
from sh import ffmpeg as program

program = program.bake("-loglevel", "warning")


def split(filename, _id, metadata):
	log.log('spliting %s' % (filename))

	mapped_ffmpeg = program.bake('-fflags', '+genpts', '-i', filename)
	for stream, _ in sorted(metadata.items(), key=operator.itemgetter(0)):
		mapped_ffmpeg = mapped_ffmpeg.bake('-map', '0:%s' % (stream,))

	log.log('splitting using matroska..')
	ffmpeg = mapped_ffmpeg.bake('-c', 'copy', '-f', 'segment', '-segment_time', '300', '-segment_list', '/dev/stdout')
	try:
		return ffmpeg('%s/%s-split-%s-%%2d.mkv' % (config.tmpdir, _id, uuid.uuid4()))
	except Exception as e:
		log.log('failed to split %s using matroska: %s' % (filename, e.stderr))

	#fallback #1: encode web_vtt to subrip
	log.log('splitting using matroska, encoding subs to subrip..')
	ffmpeg = mapped_ffmpeg.bake('-c:v', 'copy', '-c:a', 'copy', '-c:s', 'subrip', '-f', 'segment', '-segment_time', '300', '-segment_list', '/dev/stdout')
	try:
		return ffmpeg('%s/%s-split-%s-%%2d.mkv' % (config.tmpdir, _id, uuid.uuid4()))
	except Exception as e:
		log.log('failed to split %s using matroska & subrip: %s' % (filename, e.stderr))

	#fallback #2: mpegts (and web_vtt to subrip, mpegts won't handle web_vtt anyway)
	log.log('splitting using MPEG transport stream..')
	try:
		return ffmpeg('%s/%s-split-%s-%%2d.ts' % (config.tmpdir, _id, uuid.uuid4()))
	except Exception as e:
		log.log('failed to split %s using mpegts, giving up: %s' % (filename, e.stderr))

	return []


def merge(files):
	log.log('merging files: %s' % (files,))

	stdin = []
	for i in files:
		stdin.append("file '%s'\n" % (i,))

	outfile = '%s/result.mp4' % (config.tmpdir,)
	try:
		program('-f', 'concat', '-safe', '0', '-protocol_whitelist', 'file,http,tcp', '-i', '/dev/stdin', '-map', '0', '-c', 'copy', outfile, _in=stdin)
		return outfile
	except Exception as e:
		log.log('merge error on %s: %s' % (files, e.stderr))
		return None


def thumb(files):
	log.log('creating thumbs: %s' % (files,))
	stdin = []
	for i in files:
		stdin.append("file '%s'\n" % (i,))

	try:
		program('-f', 'concat', '-safe', '0', '-protocol_whitelist', 'file,http,tcp', '-i', '/dev/stdin', '-map', '0:v:0', '-vf', 'fps=1/10,scale=240:-1', '%s/img%%04d.png' % (config.tmpdir,), _in=stdin)
	except Exception as e:
		log.warn('thumbing error on %s : %s' % (files, e.stderr))
		return None

	png = []
	match = re.compile('.*\.png$')
	for root, dirs, files in os.walk(config.tmpdir):
		for name in files:
			if re.match(match, name):
				png.append(os.path.join(root, name))
	return png


def encode(path, metadata):
	log.log('encoding %s' % (path,))
	output = '%s/encoded.mp4' % (config.tmpdir,)
	ugly_output = '%s/ugly.mp4' % (config.tmpdir,)

	ffmpeg = program.bake('-nostats', '-i', path)

	#Best quality encode
	for stream, _ in sorted(metadata.items(), key=operator.itemgetter(0)):
		ffmpeg = ffmpeg.bake('-map', '0:%s' % (stream,))

	ffmpeg = ffmpeg.bake('-c:a', 'aac', '-q:a', '2')
	ffmpeg = ffmpeg.bake('-c:v', 'libx264', '-crf', '18', '-x264opts', 'min-keyint=96:keyint=96:scenecut=0:no-scenecut')
	ffmpeg = ffmpeg.bake('-c:s', 'mov_text')
	ffmpeg = ffmpeg.bake(output)

	#Ugly quality encode
	for stream, _ in sorted(metadata.items(), key=operator.itemgetter(0)):
		ffmpeg = ffmpeg.bake('-map', '0:%s' % (stream,))
	ffmpeg = ffmpeg.bake('-c:a', 'aac', '-q:a', '2')
	ffmpeg = ffmpeg.bake('-c:v', 'libx264', '-crf', '27')
	ffmpeg = ffmpeg.bake('-c:s', 'mov_text')
	ffmpeg = ffmpeg.bake('-movflags', 'faststart')

	try:
		ffmpeg(ugly_output)
	except Exception as e:
		log.warn('Error on file %s: %s' % (path, e.stderr))
		return None, None

	return output, ugly_output


def make_mpd(path, _id, metadata):
	log.log('generating DASH playlist from %s (%i)' % (path, _id))
	output = '%s/%s.mpd' % (config.tmpdir, _id)
	ffmpeg = program.bake('-nostats', '-i', path)
	for stream, _ in sorted(metadata.items(), key=operator.itemgetter(0)):
		_type = metadata[stream]['type']
		if _type == 'video' or _type == 'audio':
			ffmpeg = ffmpeg.bake('-map', '0:%s' % (stream,))
	ffmpeg = ffmpeg.bake('-c', 'copy', '-f', 'dash', '-single_file', '1', '-min_seg_duration', '4')
	ffmpeg(output)
	return output


def make_hls(path, _id, metadata):
	log.log('generating HLS playlist from %s (%i)' % (path, _id))
	output = '%s/%s.m3u8' % (config.tmpdir, _id)
	ffmpeg = program.bake('-nostats', '-i', path)
	for stream, _ in sorted(metadata.items(), key=operator.itemgetter(0)):
		ffmpeg = ffmpeg.bake('-map', '0:%s' % (stream,))
	ffmpeg = ffmpeg.bake('-bsf:v', 'h264_mp4toannexb', '-c:v', 'copy', '-c:a', 'copy', '-c:s', 'dvb_subtitle', '-segment_list_type', 'hls', '-segment_list', output, '-segment_time', '10', '-f', 'segment')
	ffmpeg('%s/hls-%s-chunk-%s-%%3d.ts' % (config.tmpdir, _id, uuid.uuid4()))
	return output


def extract_subs(path, _id, metadata):
	log.log('extracting subtitles from %s using %s' % (path, metadata))
	result = []
	for i in metadata:
		channel = metadata[i]
		if channel['type'] != 'subtitle':
			continue
		output = '%s/%s-%s.vtt' % (config.tmpdir, channel['lang'], i)
		result.append({'file': output, 'lang': channel['lang'], 'i': i})
		i = program('-y', '-i', path, '-map', '0:%s' % (i,), output)
	return result


def makePlaylists(files, _id, metadata):
	log.log('generating playlists from %s' % (files,))

	outfile = merge(files)

	m3u8 = make_hls(outfile, _id, metadata)
	mpd = make_mpd(outfile, _id, metadata)
	vtts = extract_subs(outfile, _id, metadata)

	return outfile, m3u8, mpd, vtts
