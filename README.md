RAVEN: Raven's an Asynchronous Video ENcoder
===

What is raven
---

Raven is a distributed video encoder. Based on ffmpeg, it is supposed to be run on multiple nodes, most of which will work on the same movie.
Currently, raven can process files as well as our replay.
It has multiple outputs: an HLS output, a DASH output, and multiple small thumbnails (1 per 10sec of video).


Raven internal state is stored in a databases, which is also the process entry point (schema can be found in db.sql).
Raven also uses a shared storage, based on Ceph, for data sharing and output posting.

IPC is handled using rabbitmq. Access to the database server is required for the watcher only.

There is three kinds of nodes, the "master", the "watcher" and the army of clone "slaves".
The watcher can be seen as the orchestrator. It does nothing but reading tasks and filling new tasks. It must get access to the database.
The master is a special node that have access to a special shared storage (out of scope, may be NFS) for new tasks. It is also in charge for doing tasks which need
large tmpspace.
Workers does the rest of the tasks.

So far, watcher and master runs on the same node.

Notes on database
---

Insert is allowed to create a new task.
Delete can be used to cancel a job.
You must not update any entry nor delete entry you've not inserted.

How to install
---
On all nodes:
```
aptitude install python3 python3-requests python3-sh python3-boto ffmpeg python3-pika
ln -s /root/raven/raven.init /etc/init.d/raven && update-rc.d raven defaults
```

On the watcher:
```
aptitude install python3-pymysql
```

Don't forget to create a rabbitmq user:
```
rabbitmqctl add_user bob bob
rabbitmqctl set_permissions -p / bob ".*" ".*" ".*"
```

Shared logging
---

The code log useful code to syslog, local7 facility. Additionnal but mostly useless data may be written to /tmp/raven.log.
To redirect all raven's log to a single host, on the logging server:

```
~# cat /etc/rsyslog.conf
[..]
$ModLoad imudp
$UDPServerRun 514

$AllowedSender UDP, <raven's IP>/24
[..]
```

On raven's node:
```
~# cat /etc/rsyslog.conf
[..]
local7.* @<logging server's IP:514
[..]
```

Periodic data scrubbing
---
scrub.py allow you to cleanup deleted movies, failed movies and temporary chunks. This does check all files, based on the fileID from upstream. You may want to run this using cron, on a daily basis
