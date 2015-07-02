#!/usr/bin/python3

import copy
import threading
import time
import socket
import helpers
import asyncio
import hashlib
from multiprocessing import Process
import getopt, sys
import json
from jsonschema import validate, Draft4Validator
import msg_schemata

schema = {
    "action": {"type": "string"},
    "key": {"type": "string"},
}

PORT_START = None
SERVER_COUNT = None

class DHTAsyncClient(asyncio.Protocol):
	def __init__(self, msg, loop4):
		self.msg = msg
		self.loop4 = loop4

	def connection_made(self, transport):
		print('  Connected ', transport.get_extra_info('peername'))
		self.transport = transport
		# transport.write(self.msg.encode())

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

	def __init__(self, server_address, port):
		self.server_address = server_address
		self.port = port

		self.__serverConnections = {}  # remember active connections

		print("My key: ", self.get_key())

	@asyncio.coroutine
	def send_data(self, data):

		print("  send data back..." + data)
		server2 = self.__serverConnections.get("1338")

		if server2 is None or not server2.connected:
			protocol, server2 = yield from loop.create_connection(lambda: DHTAsyncClient(data, loop), '127.0.0.1', 1338)
			server2.server_transport = self.transport
			self.__serverConnections["1338"] = server2
			print("Server2: ", data)


		server2.transport.write(data.encode())
		# server2.transport.close()

	def connection_made(self, transport):

		print("new connection")
		peer = transport.get_extra_info('peername')
		self.transport = transport

	def data_received(self, data):
		# Warning: do not call data.decode() twice
		# print("DATA IS" + data.decode())
		print("Start data_received.")
		try:
			print("Raw data: " + str(data))
			msg = json.loads(data.decode())
			print(msg)
		except Exception as e:
			print("JSON problem: " + str(e))
		if msg["action"] == "client_debug":
			print("GOT DHT STORE COMMAND")

			message = json.dumps({
				"action": "FIND_SUCCESSOR",
				"key": self.get_keytemp("127.0.0.1", 1338),
				#"source_identity": self.get_key(),
				"source_port": self.port,
				"source_ip" : self.server_address
			})
			asyncio.Task(self.send_data(message))

		elif msg["action"] == "FIND_SUCCESSOR":
			print("got find successor request")
			if msg["key"] == self.get_key():
				# We are the target
				print("Sending reply to origin")
			else:
				# Forward message to next peer
				new_msg = copy.deepcopy(msg)
				# TODO: add trace
				asyncio.Task(self.send_data(new_msg))

		self.transport.write(json.dumps({
			"STATUS": "OK"
		}).encode())  # send back
		self.transport.close()

	def get_next_server(self):
		ip_address = "localhost"
		port = self.port + 1

		return ip_address, port

	# TODO: public key hash
	def get_key(self):
		return hashlib.sha256((self.server_address + str(self.port)).encode()).hexdigest()

	def get_keytemp(self, address, port):
		return hashlib.sha256((address + str(port)).encode()).hexdigest()



# Parse console arguments
opts, args = getopt.getopt(sys.argv[1:], "p:s:c:")
port = None
for key, val in opts:
	if key == "-p":
		port = int(val)
	elif key == "-s":
		PORT_START = int(val)
	elif key == "-c":
		SERVER_COUNT = int(val)

print("Port", port)
print("-------------------")
loop = asyncio.get_event_loop()
loop.set_debug(True)

@asyncio.coroutine
def initialize(loop):
	# TODO: improve passing of parameters
	dhtServer = yield from loop.create_server(lambda: DHTAsyncServer('127.0.0.1', port), '127.0.0.1', port)
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

		message = json.dumps({
			"action": "client_debug",
			# "action": "FIND_SUCCESSOR",
		})
		# validate(message, schema)
		sock.sendall(bytes(message, 'UTF-8'))

		amount_received = 0
		amount_expected = len(message)

		while amount_received < amount_expected:
			data = sock.recv(16)
			amount_received += len(data)

		print("client server got" + str(data))

	finally:
		sock.close()

# Main
asyncio.Task(initialize(loop))
loop.run_forever()

