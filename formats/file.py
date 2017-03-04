import config

def resolve(path):
	path = '%s/%s' % (config.datadir, path['filename'])
	return path

