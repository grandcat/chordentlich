#!/usr/bin/python3

import asyncio
import getopt
import json
import sys


class EchoClientProtocol(asyncio.Protocol):
    def __init__(self, message, loop):
        self.message = message
        self.loop = loop

    def connection_made(self, transport):
        transport.write(self.message.encode())
        print('Data sent: {!r}'.format(self.message))

    def data_received(self, data):
        print('Data received: {!r}'.format(data.decode()))

    def connection_lost(self, exc):
        print('The server closed the connection.')
        print('Stop event loop.')
        self.loop.stop()

# Parse console arguments
opts, args = getopt.getopt(sys.argv[1:], "p:a:")
port = action = None
for key, val in opts:
    if key == "-p":
        port = int(val)
    elif key == "-a":
        action = str(val)

# Start client
message = {"action": "init_node"}
if action is not None:
    message["action"] = action

loop = asyncio.get_event_loop()
coro = loop.create_connection(lambda: EchoClientProtocol(json.dumps(message), loop),
                              '127.0.0.1', port or 1338)
loop.run_until_complete(coro)
# loop.run_forever()
loop.close()