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
from Node import *
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

    def __init__(self, msg, loop4, server_transport):
        self.msg = msg  # Not used!
        self.loop4 = loop4
        # Transport connection to reply to originally asking DHT node
        self.server_transport = server_transport

    def connection_made(self, transport):
        #print('  Connected ', transport.get_extra_info('peername'))
        self.transport = transport
        # transport.write(self.msg.encode())

    def data_received(self, data):
        # Send message to server
        msg = data.decode()
        print('[Client] Server returned: ', msg)
        # self.transport.write(data)    # BUG: what's it for? Makes no difference
        # Test: makes our server replying to the asking peer
        new_json_data = {}
        json_data = json.loads(msg)
        new_json_data["old"] = json_data
        new_json_data["port"] = port
        self.server_transport.write(json.dumps(new_json_data).encode())
        self.server_transport.close()
        # Test END

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
        # This node
        self.node = Node(host_address, host_port, bootstrap_address=bootstrap_address)
        # Server/Client states
        self.__serverConnections = {}  # remember active connections to other DHT servers

    @asyncio.coroutine
    def send_data(self, data):
        #print("Data to send is", data )
        dataString = json.dumps(data)

        server2 = self.__serverConnections.get(str(data["destination_ip"]+":"+str(data["destination_port"])))
        if server2 is None or not server2.connected:
            protocol, server2 = yield from loop.create_connection(lambda: DHTAsyncClient(json.dumps(data), loop, self.transport), data["destination_ip"], data["destination_port"])
            server2.server_transport = self.transport
            self.__serverConnections[str(data["destination_ip"]+":"+str(data["destination_port"]))] = server2

        server2.transport.write(dataString.encode())
        # server2.transport.close()
        return 0

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

        if msg["action"] == "init_node":

            # First we JOIN the Chord network. Therefore we initialize the
            # finger Table with start values
            print('Received init_node')

            if self.node.bootstrap_address is not None: # TODO: Change bootstrap port to address
                # Make a find successor request
                message = {
                    "action": "FIND_SUCCESSOR",
                    "key": self.get_keytemp(self.node.host_address, self.node.host_port), # TODO: Change port to address
                    #"source_identity": self.get_key(),
                    "destination_port": self.node.host_port,
                    "destination_ip" : self.node.host_address,
                    "source_port": self.node.host_port,
                    "source_ip" : self.node.host_address,
                    "ttl" : 3 # limit number of requests for debugging reasons!
                }
                result = asyncio.Task(self.send_data(message), loop=loop)
                # result.add_done_callback(self.handle_result)
                print("init_node: async send_data ", result)

            self.node.initFingerTable()

        elif msg["action"] == "FIND_SUCCESSOR_REPLY":

            # Update Successor
            self.node.successor = Node(msg["nodeId"], msg["host_port"], msg["host_address"])

        elif msg["action"] == "FIND_SUCCESSOR":
            # Case 1: We are the target (looked up id is between self.nodeId and self.successor.nodeId)
            if int(msg["key"]) > self.get_key() and int(msg["key"]) <= self.node.successor.id:

                print("    - FIND_SUCCESSOR SEND REPLY!!")
                message = json.dumps({
                    "action": "FIND_SUCCESSOR_REPLY",
                    "key": self.get_keytemp(msg["source_ip"], msg["source_port"]), # TODO: Change port to address
                    #"source_identity": self.get_key(),
                    "destination_port": msg["source_port"],
                    "destination_ip" : msg["source_ip"],
                    "source_port": self.node.host_port,
                    "source_ip" : self.node.host_address,

                    "nodeId" : self.node.successor.id,
                    "host_port" : self.node.successor.host_port,
                    "host_address" : self.node.successor.host_address

                })
                asyncio.Task(self.send_data(message))


            else:
                # Case 2: We are not the target ----> Forward message to closest preceding finger
                precedingNode = self.node.getClosestPrecedingFinger(msg["key"])
                print("    - FIND_SUCCESSOR - FORWARD")
                print("    - with destination: ", precedingNode.host_port)

                # Forward message to next peer
                # NOTE: host_address + host_port contain our self right now. Therefore, just pass the message
                # to our preceding neighbor. Therefore, "-1" is applied to precedingNode.host_port
                new_msg = copy.deepcopy(msg)
                new_msg["destination_ip"] = precedingNode.host_address
                new_msg["destination_port"] = precedingNode.host_port - 1 # TODO: remove -1 once fingertable is fixed
                new_msg["ttl"] = msg["ttl"] - 1

                # TODO: add trace
                if msg["ttl"] > 0:
                    asyncio.Task(self.send_data(new_msg))
                else:
                    print("Abort FIND_SUCCESSOR - FORWARD: ttl exceeded.")
                    self.transport.write(json.dumps({
                        "STATUS": "ENDOFLINE",
                        "HOST_PORT": port   # TODO: remove, for debugging
                    }).encode())  # send back
                    self.transport.close()

        # self.transport.write(json.dumps({
        #     "STATUS": "OK",
        #     "HOST_PORT": port   # TODO: remove, for debugging
        # }).encode())  # send back
        # self.transport.close()

    def find_predecessor(self):
        # TODO: set successor --> should actually be part of Node implementation
        pass

    def get_next_server(self):
        ip_address = "localhost"
        port = self.node.host_port + 1

        return ip_address, port

    # TODO: public key hash
    def get_key(self):
        # TODO: remove modulo
        return int(hashlib.sha256((self.node.host_address + str(self.node.host_port)).encode()).hexdigest(), 16) % CHORD_RING_SIZE

    def get_keytemp(self, address, port):
        return int(hashlib.sha256((address + str(port)).encode()).hexdigest(), 16) % CHORD_RING_SIZE


@asyncio.coroutine
def initialize(loop, port):
    # TODO: improve passing of parameters

    # Last port is bootstrap node
    boostrapNodePort = (PORT_START + SERVER_COUNT - 1) if port != (PORT_START + SERVER_COUNT - 1) else None

    print("Address/Port of Bootstrap Node: ", boostrapNodePort)
    #print("My key: ", self.get_key())

    dhtServer = yield from loop.create_server(lambda: DHTAsyncServer('127.0.0.1', port, bootstrap_address=boostrapNodePort), '127.0.0.1', port)
    # Spawn one unique local client on server n-1 responsible for all active DHT servers
    if port == (PORT_START + SERVER_COUNT - 2):
        threading.Thread(target=connectClient).start()

def connectClient():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('127.0.0.1', port)
    sock.connect(server_address)
    try:

        message = {
            "action": "init_node",
            # "action": "FIND_SUCCESSOR",
        }

        # validate(message, schema)
        sock.sendall(bytes(json.dumps(message), 'UTF-8'))

        amount_received = 0
        data_available = 1

        output = bytearray()
        while data_available > 0 and amount_received < 1024:
            data = sock.recv(16)
            data_available = len(data)
            amount_received += len(data)
            output.extend(data)

        print("RESPONSE FROM init_node: " + output.decode())

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
