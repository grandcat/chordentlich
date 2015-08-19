#!/usr/bin/python3
import asyncio
import getopt

import aiomas
import logging
import sys
from Node import Node
from ipc import ApiServer

"""
Main application
"""
# Setup logging
logging.basicConfig(format='[%(levelname)s:%(threadName)s:%(name)s:%(funcName)s] %(message)s', level=logging.INFO)

# Parse console arguments
opts, args = getopt.getopt(sys.argv[1:], "I:i:B:b:")

port_start = -1
ipaddress = "127.0.0.1"
bootip = None


bootport = 9025
port = 9025

for key, val in opts:
    if key == "-I":
        ipaddress = val
    if key == "-i":
        port = int(val)
    if key == "-B":
        bootip = val
    elif key == "-b":
        bootport = int(val)

print("bootip", bootip)
print("Port", port)
# Testing: First port is bootstrap node
bootstrap_addr = ("tcp://"+bootip+":" + str(bootport) + "/0") if bootip else None
print("Address/Port of Bootstrap Node: ", (bootstrap_addr or "this node"))
print("-------------------")
print("port", port)
print("port_start", port_start)
# TODO: parse INI config file here

# Define multiple agents per node for accepting RPCs
c = aiomas.Container((ipaddress, port))
nodes = [c.spawn(Node) for i in range(1)]

loop = asyncio.get_event_loop()
# Start API server interface
api_server = loop.create_server(lambda: ApiServer(nodes[0]), ipaddress, 3086 + port)
loop.run_until_complete(api_server)
# Start DHT node
loop.run_until_complete(nodes[0].join(bootstrap_address=bootstrap_addr))
loop.run_until_complete(nodes[0].stabilize())

# Test RPC calls within the same node from backup agent 1 to agent 0
#loop.run_until_complete(nodes[1].test_get_node_id(nodes[0].addr))

# Test RPC calls to bootstrap node (first port)
#if bootstrap_addr:
#    loop.run_until_complete(nodes[0].test_get_node_id(bootstrap_addr))
    #loop.run_until_complete(nodes[0].test_get_closest_preceding_finger(bootstrap_addr, 123))
    #loop.run_until_complete(nodes[0].test_find_my_successor(bootstrap_addr))

loop.run_forever()
c.shutdown()
