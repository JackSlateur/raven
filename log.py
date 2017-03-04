import syslog
import os
import sys

try:
	syslog.openlog('[raven %s %s]' % (sys.argv[1], os.getpid()), facility=syslog.LOG_LOCAL7)
except IndexError:
	syslog.openlog('[raven %s]' % (os.getpid()), facility=syslog.LOG_LOCAL7)

def log(message, status='debug'):
	syslog.syslog('%s: %s' % (status, message))

def error(message):
	log(message, 'error')
	sys.exit(1)

def warn(message):
	log(message, 'warn')
