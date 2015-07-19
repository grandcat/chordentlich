#!/usr/bin/python3

import copy
import threading
import time
import socket
import asyncio
import hashlib
from multiprocessing import Process
import getopt
import sys
import json
from jsonschema import validate, Draft4Validator
from fingerTable import Node
from enum import Enum

# Save status to manage async calls.. TODO: Exclude this in a new file
class STATE(Enum):
   JOINING = 1    # The node joins the chord network`
   READY = 2   # The node ist ready for key lookups

schema = {
    "action": {"type": "string"},
    "key": {"type": "string"},
}

PORT_START = None
SERVER_COUNT = None
CURRENT_STATE = STATE.JOINING

class DHTAsyncClient(asyncio.Protocol):

    def __init__(self, msg, loop4):
        self.msg = msg
        self.loop4 = loop4

    def connection_made(self, transport):
        #print('  Connected ', transport.get_extra_info('peername'))
        self.transport = transport
        # transport.write(self.msg.encode())

    def data_received(self, data):
        # Send message to server
        msg = data.decode()
        #print('Server returned: ', msg)
        self.transport.write(data)
        self.transport.close()

    def connection_lost(self, exc):
        # self.loop4.stop()
        #print("Client  Connection Lost")
        pass

class DHTAsyncServer(asyncio.Protocol):
    """
    Independent DHT node acting as a server or even bootstrap node.
    """

    def __init__(self, host_address, host_port, bootstrap_address=None):
        # Node
        self.host_address = host_address
        self.host_port = host_port
        self.bootstrap_address = bootstrap_address
        self.node = Node(self.get_key(), host_address, host_port, bootstrap_address)
        # Server state
        self.__serverConnections = {}  # remember active

    @asyncio.coroutine
    def send_data(self, data):
        #print("Data to send is", data )
        dataString = json.dumps(data);

        server2 = self.__serverConnections.get(str(data["destination_ip"]+":"+str(data["destination_port"])))
        if server2 is None or not server2.connected:
            protocol, server2 = yield from loop.create_connection(lambda: DHTAsyncClient(json.dumps(data), loop), data["destination_ip"], data["destination_port"])
            server2.server_transport = self.transport
            self.__serverConnections[str(data["destination_ip"]+":"+str(data["destination_port"]))] = server2

        server2.transport.write(dataString.encode())
        # server2.transport.close()

    def connection_made(self, transport):

        print("new connection")
        peer = transport.get_extra_info('peername')
        self.transport = transport

    def data_received(self, data):
        # Warning: do not call data.decode() twice
        # print("DATA IS" + data.decode())

        try:
            #print("Raw data: " + str(data))
            msg = json.loads(data.decode())
        except Exception as e:
            print("JSON problem: " + str(e))

        print("ACTION:", msg["action"])

        if msg["action"] == "client_start":

            # First we JOIN the Chord network. Therefore we initialize the
            # finger Table with start values

            if self.bootstrap_address is not None: # TODO: Change bootstrap port to address
                # Make a find successor request
                message = {
                    "action": "FIND_SUCCESSOR",
                    "key": self.get_keytemp(self.host_address, self.bootstrap_address), # TODO: Change port to address
                    #"source_identity": self.get_key(),
                    "destination_port": self.host_port,
                    "destination_ip" : self.host_address,
                    "source_port": self.host_port,
                    "source_ip" : self.host_address,
                    "ttl" : 4 # limit number of requests for debugging reasons!
                }
                asyncio.Task(self.send_data(message))

            self.node.initFingerTable()
        elif msg["action"] == "FIND_SUCCESSOR_REPLY":

            # Update Successor
            self.node.successor = Node(msg["nodeId"], msg["host_port"], msg["host_address"])

        elif msg["action"] == "FIND_SUCCESSOR":


            # Case 1: We are the target (looked up id is between self.nodeId and self.successor.nodeId)
            if int(msg["key"]) > self.get_key() and int(msg["key"]) <= self.node.successor.nodeId:

                print("    - FIND_SUCCESSOR SEND REPLY!!");
                message = json.dumps({
                    "action": "FIND_SUCCESSOR_REPLY",
                    "key": self.get_keytemp(msg["source_ip"], msg["source_port"]), # TODO: Change port to address
                    #"source_identity": self.get_key(),
                    "destination_port": msg["source_port"],
                    "destination_ip" : msg["source_ip"],
                    "source_port": self.host_port,
                    "source_ip" : self.host_address,

                    "nodeId" : self.node.successor.nodeId,
                    "host_port" : self.node.successor.host_port,
                    "host_address" : self.node.successor.host_address

                })
                asyncio.Task(self.send_data(message))


            else:
                # Case 2: We are not the target ----> Forward message to closest preceding finger
                precedingNode = self.node.getClosestPrecedingFinger(msg["key"])
                print("    - FIND_SUCCESSOR - FORWARD");

                # Forward message to next peer
                new_msg = copy.deepcopy(msg)
                new_msg["destination_ip"] = precedingNode.host_address
                new_msg["destination_port"] = precedingNode.host_port
                new_msg["ttl"] = msg["ttl"] - 1;

                # TODO: add trace
                if msg["ttl"] > 0:
                    asyncio.Task(self.send_data(new_msg))

        self.transport.write(json.dumps({
            "STATUS": "OK"
        }).encode())  # send back
        self.transport.close()

    def find_predecessor(self):
        # TODO: set successor
        pass

    def get_next_server(self):
        ip_address = "localhost"
        port = self.host_port + 1

        return ip_address, port

    # TODO: public key hash
    def get_key(self):
        # TODO: remove modulo
        return int(hashlib.sha256((self.host_address + str(self.host_port)).encode()).hexdigest(), 16) % Node.chordRingSize

    def get_keytemp(self, address, port):
        return int(hashlib.sha256((address + str(port)).encode()).hexdigest(), 16) % Node.chordRingSize


@asyncio.coroutine
def initialize(loop, port):
    # TODO: improve passing of parameters

    boostrapNodePort = 1339 if port!=1339 else None

    print("Address/Port of Bootstrap Node: ", boostrapNodePort)
    #print("My key: ", self.get_key())

    dhtServer = yield from loop.create_server(lambda: DHTAsyncServer('127.0.0.1', port, bootstrap_address=boostrapNodePort), '127.0.0.1', port)
    # make a local client
    threading.Thread(target=connectClient).start()

def connectClient():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('127.0.0.1', port)
    sock.connect(server_address)
    try:

        message = {
            "action": "client_start",
            # "action": "FIND_SUCCESSOR",
        }

        # validate(message, schema)
        sock.sendall(bytes(json.dumps(message), 'UTF-8'))

        amount_received = 0
        amount_expected = len(message)

        while amount_received < amount_expected:
            data = sock.recv(16)
            amount_received += len(data)

        #print("RESPONSE FROM client_start: " + str(data))

    finally:
        sock.close()

"""
Main application
"""
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

# Start async server
loop = asyncio.get_event_loop()
loop.set_debug(True)
asyncio.Task(initialize(loop, port))
loop.run_forever()
