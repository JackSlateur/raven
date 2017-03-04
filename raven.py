#!/usr/bin/python3

from distutils.version import LooseVersion
import socket
import sys

import sh

import amqp
import config
import log
import master
import watcher
import worker

sys.stdout = open('/tmp/raven.log', 'a')
sys.stderr = sys.stdout

def check_sh():
	cur = LooseVersion(sh.__version__)
	want = LooseVersion('1.11')
	if cur < want:
		log.error('sh version %s required (%s found), probably due to https://github.com/amoffat/sh/commit/5b3a99cdc03dac6b171ce095efea20332d9b7c01' % (want, cur))
		sys.exit(1)

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print('Usage: %s <role>' % (sys.argv[0],))
		sys.exit(1)

	# We might share our tmpspace with other roles
	config.tmpdir = '%s/%s' % (config.tmpdir, sys.argv[1])

	check_sh()
	log.log('Starting raven %s as %s..' % (config.version, sys.argv[1]))

	amqp.init()
	if sys.argv[1] == 'watcher':
		watcher.run()
	elif sys.argv[1] == 'master':
		master.run()
	elif sys.argv[1] == 'worker':
		worker.run()

