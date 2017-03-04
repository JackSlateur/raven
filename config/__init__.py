from .default import *

try:
	from .raven import *
except:
	import log
	import sys

	log.error('No custom configuration found. Please fill config/raven.py')
	sys.exit(1)
