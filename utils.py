import json
import os
import shutil
import sys

import config
from formats import *

class CaseInsensCreepyDict(dict):
	def __setitem__(self, key, value):
		super(CaseInsensCreepyDict, self).__setitem__(key.lower(), value)

	def __getitem__(self, key):
		return super(CaseInsensCreepyDict, self).__getitem__(key.lower())

def resolve_paths(path):
	try:
		path = json.loads(path)
	except TypeError:
		return None

	resolver = sys.modules['formats.%s' % (path['type'],)]
	path = resolver.resolve(path)
	return path

def setup_tmpdir(tmpdir):
	shutil.rmtree(tmpdir, ignore_errors=True)
	try:
		os.mkdir(tmpdir)
	except FileExistsError:
		pass
