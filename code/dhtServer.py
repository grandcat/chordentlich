#!/usr/bin/python3

import threading
import time
import socket
import helpers
import asyncio
from multiprocessing import Process
import getopt, sys


class DHTAsyncClient(asyncio.Protocol):
	def __init__(self, msg, loop4):
		self.msg = msg
		self.loop4 = loop4

	def connection_made(self, transport):
		print('  Connected ', transport.get_extra_info('peername'))
		self.transport = transport
		transport.write(self.msg.encode())

	def data_received(self, data):
		# Send message to server
		msg = data.decode()
		print('Server returned: ', msg)
		self.transport.write(data)
		self.transport.close()

	def connection_lost(self, exc):
		# self.loop4.stop()
		print("Client  Connection Lost")


class DHTAsyncServer(asyncio.Protocol):
	serverConnections = {}  # remember active connections

	@asyncio.coroutine
	def send_data(self, data):

		print("  send data back..." + data)
		server2 = self.serverConnections.get("1338")

		if server2 is None or not server2.connected:
			protocol, server2 = yield from loop.create_connection(lambda: DHTAsyncClient("messagetoother server", loop),
																  '127.0.0.1', 1338)
			server2.server_transport = self.transport
			self.serverConnections["1338"] = server2

		server2.transport.write(data.encode())

	def connection_made(self, transport):

		print("new connection")
		peer = transport.get_extra_info('peername')
		self.transport = transport

	def data_received(self, data):

		msg = data.decode()

		if msg == "client_cmd_dht_store":
			print("GOT DHT STORE COMMAND")
			asyncio.Task(self.send_data("lalala"))

		self.transport.write(" ok".encode())  # send back
		self.transport.close()

# Parse console arguments
opts, args = getopt.getopt(sys.argv[1:], "p:")
port = 1339
for key, val in opts:
	if key == "-p":
		port = int(val)

print("server port ", port)
print("-------------------")
loop = asyncio.get_event_loop()
loop.set_debug(True)


@asyncio.coroutine
def initialize(loop):
	dhtServer = yield from loop.create_server(DHTAsyncServer, '127.0.0.1', port)
	if port == 1339:
		# make a local client
		threading.Thread(target=connectClient).start()


def connectClient():
	print("1")
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_address = ('127.0.0.1', 1339)
	sock.connect(server_address)
	print("2")
	try:
		message = 'client_cmd_dht_store'
		sock.sendall(bytes(message, 'UTF-8'))

		amount_received = 0
		amount_expected = len(message)

		while amount_received < amount_expected:
			data = sock.recv(16)
			amount_received += len(data)

		print("client server got" + str(data))

	finally:
		sock.close()


asyncio.Task(initialize(loop))
loop.run_forever()
