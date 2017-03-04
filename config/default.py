### Do not edit me ! You can override all settings in config/raven.py ###

version = "2.0.0"

# Should be large enought to store all chunks. Make it larger for the master.
tmpdir = '/tmp/raven'

# The master's FQDN. The master should have more tmpspace,
# and get access to the datasource.
# The master runs the watcher, which is unique, and some specific tasks too.
master = 'raven01'

# This database must be reachable from all nodes.
db_host = '10.0.0.1'
db_user = 'raven'
db_passwd = 'passwd'
db_db = 'db'

# Datasource, used to feed the master with new files. Irrelevant on slaves.
datadir = '/media/shared'

# Rabbitmq must be reachable from all nodes, read & write.
ramq_host = '10.0.0.1'
ramq_user = 'raven'
ramq_passwd = 'passwd'

# Ceph information. Ceph is used to share data between nodes.
# It should works with Amazon s3.
key = 'key'
secret = 'secret'
ceph_fqdn = 'files.domain.com'
bucket = 'raven'

# If False, the master node does not run slaves's tasks.
# That way, the master can dedicate itself to the required tasks.
# Set it to True if you have a small cluster (potentially just a single node).
singlenode_setup = False

# formats/ketchup.py
ketchup_host = 'http://ketchup.domain.com'

# Upstream's URI, for callback. It must contains a %s, that will be replaced by
# upstream's id
upstream_uri = 'http://upstream.domain.com/callbackup/%s/'
