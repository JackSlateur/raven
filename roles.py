#!/usr/bin/python3

import config

import socket

roles = ''
if socket.gethostname() == config.master:
	roles += 'master watcher'
	if config.singlenode_setup is True:
		roles += ' worker'
else:
	roles += 'worker'

print(roles)
