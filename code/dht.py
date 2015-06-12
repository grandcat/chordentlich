#!/usr/bin/python3

import threading
import time
import socket
import asyncio
from multiprocessing import Process

class DHTAsync(asyncio.Protocol):

	def __init__(self, msg, loop):
		self.msg = msg
		self.loop = loop

	def connection_made(self, transport):

		peername = transport.get_extra_info('peername')
		print('Connected ', peername)
		self.transport = transport

	def data_received(self, data):

		# Send message to server
		msg = data.decode()
		print('received ', peername)
		print('msg ', msg)
		self.transport.write(data)
		self.transport.close()

	def connection_lost(self, exc):
		print('Con closed')
		self.loop.stop()

loop = asyncio.get_event_loop()
loop.run_until_complete( loop.create_connection(lambda: DHTAsync('Some command to server', loop),
'127.0.0.1', 1339)) # send to 1339
loop.close()
