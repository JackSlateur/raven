import config

def resolve(path):
	path = 'http://%s/%s/%s' % (config.ceph_fqdn, config.bucket, path['filename'])
	return path

