#!/usr/bin/python3

import threading
import time
import socket
import helpers
import asyncio
from multiprocessing import Process 
import getopt, sys

class DHTAsyncServer(asyncio.Protocol):
	def connection_made(self, transport):
		peer = transport.get_extra_info('peername')
		self.transport = transport

	def data_received(self, data):
		msg = data.decode()
		print("received message:")
		print(message)	
		self.transport.write(data)
		self.transport.close()

# Parse console arguments
opts, args = getopt.getopt(sys.argv[1:], "p:")
port = 1337
for key, val in opts:
	if key=="-p":
		port = int(val)

helpers.test()
print("server node running on port ", port)
loop = asyncio.get_event_loop()
con = loop.create_server(DHTAsyncServer, '127.0.0.1', port)
server = loop.run_until_complete(con)
print('Socket open', server.sockets[0].getsockname())

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
