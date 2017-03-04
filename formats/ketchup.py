import config

def resolve(path):
	path = '%s/m3u.m3u8?channel=%s&timestamp=%s&duration=%s' % (config.ketchup_host, path['channel'], path['timestamp'], path['duration'])
	return path

