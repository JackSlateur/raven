#!/usr/bin/python3

import sys

import boto3

import config
import sql

db = sql.sql(config.db_host, config.db_user, config.db_passwd, config.db_db)
s3 = boto3.resource(service_name='s3',
		endpoint_url='https://%s' % (config.ceph_fqdn,),
		aws_access_key_id=config.key,
		aws_secret_access_key=config.secret
		)
bucket = s3.Bucket(config.bucket)

data = db.dump()
max = data[-1]['id']

result = {}
for i in data:
	result[i['id']] = i['status']
data = result

print('%s keys, max key: %s' % (len(data), max))

def purge(_id):
	print('\tPurging id %s' % (_id,))
#	for key in bucket.objects.filter(Prefix='%s-' % (_id,)):
#		print(key.key)
#	return
	while True:
		try:
			info = bucket.objects.filter(Prefix='%s-' % (_id,)).delete()
			break
		except:
			pass

for i in range(1, max):
	if i not in data:
		print('%s can be completely removed, no longer exists' % (i,))
		purge(i)
		purge('hls-%s' % (i,))
		continue
	#hop
	if data[i] == 'FAILED':
		print('%s can be completely removed, it failed' % (i,))
		purge(i)
		purge('hls-%s' % (i,))
		continue

	continue

