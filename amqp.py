import json
import socket
import time

import pika

import config
import log


def connect():
	credentials = pika.PlainCredentials(config.ramq_user, config.ramq_passwd)
	params = pika.ConnectionParameters(host=config.ramq_host, credentials=credentials, heartbeat_interval=0)

	connection = pika.BlockingConnection(params)

	args = {'x-queue-mode': 'lazy',
		'x-max-priority': 2,
		}

	chan = connection.channel()
	chan.queue_declare(queue='master', durable=True, arguments=args)
	chan.queue_declare(queue='worker', durable=True, arguments=args)
	chan.queue_declare(queue='watcher', durable=True, arguments=args)
	return chan


class DirectRT:
	def __init__(self, adapter):
		self.result = None
		self.adapter = adapter

	def direct_reply_to(self, body):
		self.adapter.channel.basic_consume(self.callback, queue='amq.rabbitmq.reply-to', no_ack=True)
		self.adapter.pub(body, reply=True)
		self.adapter.channel.start_consuming()
		return self.result

	def callback(self, ch, method_frame, properties, body):
		if body is not None:
			self.result = json.loads(body.decode('utf-8'))
		ch.stop_consuming()

	def pub(self, body, props):
		self.adapter.channel.basic_publish('', body=body, routing_key=props.reply_to)


class Adapter:
	def __init__(self, queue):
		self.queue = queue
		try:
			self.channel = connect()
		except pika.exceptions.ConnectionClosed:
			self.channel = None

	def __reconnect(self):
		if self.channel is None:
			self.channel = connect()

	def get(self):
		try:
			self.__reconnect()
			method, props, body = self.channel.basic_get(self.queue)
		except pika.exceptions.ConnectionClosed:
			self.channel = None
			return None, None, None

		if body is not None:
			body = json.loads(body.decode('utf-8'))
		return method, props, body

	def pub(self, body, failed=False, reply=False, priority=False):
		if failed is True:
			body['status'] = 'FAILED'
		body = json.dumps(body)
		try:
			self.__reconnect()

			if reply is True:
				props = pika.BasicProperties(reply_to='amq.rabbitmq.reply-to')
			else:
				props = pika.BasicProperties(delivery_mode=2)
			if priority is True:
				props.priority = 2
			self.channel.basic_publish('', self.queue, body, props)
		except (pika.exceptions.ConnectionClosed, AttributeError):
			self.channel = None
			return False

	def ack(self, tag):
		try:
			self.__reconnect()
			self.channel.basic_ack(delivery_tag=tag)
		except pika.exceptions.ConnectionClosed:
			self.channel = None
			return False

	def consume_forever(self, callback):
		while True:
			try:
				self.__reconnect()
				self.channel.basic_qos(prefetch_count=1)
				self.channel.basic_consume(callback, queue=self.queue)
				self.channel.start_consuming()
			except (pika.exceptions.ConnectionClosed, AttributeError) as e:
				self.channel = None
				log.log('consume_forever: %s' % (e,))
			time.sleep(1)

	def notify(self, _id, status):
		body = {'id': _id,
			'status': status,
			'hostname': socket.gethostname(),
			}
		for i in range(1, 3):
			if self.pub(body) is True:
				return
		raise Exception('notify failed 3 times')

	def direct_reply_to(self, body):
		return DirectRT(self).direct_reply_to(body)

	def pub_reply(self, body, props):
		body = json.dumps(body)
		return DirectRT(self).pub(body, props)


def init():
	global master, worker, watcher
	master = Adapter('master')
	worker = Adapter('worker')
	watcher = Adapter('watcher')
