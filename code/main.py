#!/usr/bin/python3
import asyncio
import getopt

import aiomas
import hashlib
import logging
import sys
import Node


"""
Main application
"""
# Setup logging
logging.basicConfig(format='[%(levelname)s:%(threadName)s:%(name)s:%(funcName)s] %(message)s', level=logging.INFO)

# Parse console arguments
opts, args = getopt.getopt(sys.argv[1:], "p:s:c:")
port = -1
port_start = -1
host_count = -1
for key, val in opts:
    if key == "-p":
        port = int(val)
    elif key == "-s":
        port_start = int(val)
    elif key == "-c":
        host_count = int(val)

print("Port", port)
# Testing: First port is bootstrap node
bootstrap_addr = ("tcp://localhost:" + str(port_start) + "/0") if port != port_start else None
print("Address/Port of Bootstrap Node: ", (bootstrap_addr or "this node"))
print("-------------------")

# Define multiple agents per node for accepting RPCs
c = aiomas.Container(("localhost", port))
nodes = [c.spawn(Node) for i in range(2)]
# Start async server
loop = asyncio.get_event_loop()
loop.run_until_complete(nodes[0].setup_node(bootstrap_address=bootstrap_addr))
loop.run_until_complete(nodes[0].join())
#loop.run_until_complete(nodes[1].setup_node(bootstrap_address=bootstrap_addr))

# Test RPC calls within the same node from backup agent 1 to agent 0
loop.run_until_complete(nodes[1].test_get_node_id(nodes[0].addr))

# Test RPC calls to bootstrap node (first port)
if bootstrap_addr:
    loop.run_until_complete(nodes[0].test_get_node_id(bootstrap_addr))
    #loop.run_until_complete(nodes[0].test_get_closest_preceding_finger(bootstrap_addr, 123))
    #loop.run_until_complete(nodes[0].test_find_my_successor(bootstrap_addr))

loop.run_forever()
c.shutdown()
