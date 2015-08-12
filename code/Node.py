#!/usr/bin/python3
import asyncio
import getopt

import aiomas
import hashlib
import logging
import sys

CHORD_FINGER_TABLE_SIZE = 8 # TODO: 256
CHORD_RING_SIZE = 2**CHORD_FINGER_TABLE_SIZE  # Maximum number of addresses in the Chord network

def in_interval(search_id, node_left, node_right, inclusive_left=False, inclusive_right=False):
    """
    Interval checks.
    """
    if inclusive_left:
        node_left = (node_left - 1) % CHORD_RING_SIZE
    if inclusive_right:
        node_right = (node_right + 1) % CHORD_RING_SIZE

    if node_left < node_right:
        return node_left < search_id < node_right
    elif node_left > node_right:
        # Circle wrapped around: separately regard cases for node_id > 0 and node_id < 0
        return (0 <= search_id < node_right) or \
               (node_left < search_id < CHORD_RING_SIZE)  # might be buggy, double check ranges!
    else:
        return False

# def deserialize_minimal_node(obj):
#     predecessor = None
#     if "predecessor" in obj:
#         predecessor = Node(None, None)
#         predecessor.setup_node(node_id=obj["predecessor"]["node_id"], node_address=obj["predecessor"]["node_address"])
#
#     node = Node(None, None)
#     node.setup_node(node_id=obj["node_id"], node_address=obj["node_address"], predecessor=predecessor)
#
#     return node


# class Finger(object):
#     def __init__(self, start_id, successor=None):
#         self.startID = start_id
#         self.successor = successor
#
#     def __repr__(self):
#         return 'Start: ' + str(self.startID) + ', SuccessorID: ' + str(self.successor.nodeId)


class Node(aiomas.Agent):

    def __init__(self, container, node_address):
        # Async RPC init
        super().__init__(container, node_address)
        self.node_address = node_address
        # Logging
        self.log = logging.getLogger(__name__)
        self.log.info("Node server listening on %s.", node_address)
        # Node state
        self.fingertable = []

    @asyncio.coroutine
    def setup_node(self, node_id=None, node_address=None, bootstrap_address=None):
        """
        Set ups all internal state variables needed for operation.
        Needs to be called previously to any other function or RPC call.
        """
        self.id = node_id or self.generate_key(self.node_address)
        self.node_address = self.node_address or node_address   # only overwrite address if created by deserialization
        self.bootstrap_address = bootstrap_address
        self.log.info("[Configuration]  node_id: %d, bootstrap_node: %s", self.id, self.bootstrap_address)

        self.successor = self.predecessor = self.as_dict()   # Todo: fix dependencies (should be set to None here and initialized properly)

        # Create first version of finger table to prevent crash during message forward when debugging
        self.init_finger_table()

    def as_dict(self, serialize_neighbors=False):
        dict_node = {
            "node_id": self.id,
            "node_address": self.node_address,
        }
        if serialize_neighbors and self.successor:
            dict_node["successor"] = self.successor
        # Check: rarely needed, could be removed maybe
        if serialize_neighbors and self.predecessor:
            dict_node["predecessor"] = self.predecessor

        return dict_node


    def to_object(obj):
        return Node(None, None)

    @staticmethod
    def generate_key(address):
        # TODO: public key hash instead of protocol + IP address + port
        # TODO: remove modulo
        return int(hashlib.sha256(address.encode()).hexdigest(), 16) % CHORD_RING_SIZE

    def get_entry(self, position):
        # Todo: rethink whether this makes sense
        addition = 2**(position-1)
        entry = (self.id + addition) % CHORD_RING_SIZE
        return entry

    @aiomas.expose
    def get_node_id(self):
        return self.id

    def init_finger_table(self):
        if self.bootstrap_address:
            # Regular node joining via bootstrap node
            self.__generate_fingers(None)
            # Joining node
            self.successor = self.predecessor = None

        else:
            # This is the bootstrap node
            successor_node = self.as_dict()
            self.__generate_fingers(successor_node)
            self.successor = self.predecessor = successor_node

        # Optimization for joining node (if not bootstrap node)
        # - Find close node to myself (e.g., successor)
        # - Request finger table and store temporary_entries
        # - for each of my needed finger table starts, use closest entries and directly ask this node.
        # - Fallback to node asked previously (or bootstrap node as last fallback) if node is not responding

    def __generate_fingers(self, successor_reference):
        self.fingertable = []

        for k in range(0, CHORD_FINGER_TABLE_SIZE):
            entry = {
                "start": ((self.id + 2**k) % CHORD_RING_SIZE),
                "successor": successor_reference
            }
            # TODO: add successor if not bootstrap node
            self.fingertable.append(entry)

        self.log.info("Default finger table: %s", str(self.fingertable)+"\n\n")

    def print_finger_table(self):
        print(self.fingertable)

    def find_successor(self, node_id):
        node = self.find_predecessor(node_id)
        return node["successor"]  # Attention: relies on available successor information which has to be
                                  # retrieved by closest_preceding_finger()

    def find_predecessor(self, node_id):
        # Special case: id we are looking for is managed by our immediate successor
        if in_interval(node_id, self.id, self.successor["node_id"]):
            return self.as_dict(serialize_neighbors=True)

        selected_node = self.as_dict(serialize_neighbors=True)
        while not in_interval(node_id, selected_node["node_id"], selected_node["successor"]["node_id"], inclusive_right=True):
            if selected_node["node_id"] == self.id:
                # Typically in first round: use our finger table to locate close peer
                selected_node = self.get_closest_preceding_finger(node_id)
                # If still our self, we do not know closer peer and should stop searching
                if selected_node["node_id"] == self.id:
                    break

            else:
                # For all other remote peers, we have to do a RPC here
                peer = yield from self.container.connect(selected_node["node_address"])
                selected_node = yield from peer.rpc_get_closest_preceding_finger(selected_node["node_id"])
                # TODO: validate received input before continuing the loop
                self.log.info("Remote closest node: %s", str(selected_node))

        return selected_node

    def get_closest_preceding_finger(self, node_id):
        """
        Find closest preceding finger within m -> 0 fingers.

        :param node_id: Node ID as an integer.
        :return: Returns the interesting node descriptor as a dictionary with successor and predecessor.
        """
        for k in range(CHORD_FINGER_TABLE_SIZE - 1, -1, -1):
            finger_successor = self.fingertable[k]["successor"]
            self.log.debug("Iterate finger %d: %d in %s", k, node_id, self.fingertable[k])

            if in_interval(finger_successor["node_id"], self.id, node_id):
                return finger_successor

        return self.as_dict(serialize_neighbors=True)

    ### RPC wrappers ###
    @aiomas.expose
    def rpc_find_successor(self, node_id):
        # TODO: validate params to prevent attacks!
        return self.find_successor(node_id)

    @aiomas.expose
    def rpc_get_closest_preceding_finger(self, node_id):
        # TODO: validate params to prevent attacks!
        return self.get_closest_preceding_finger(node_id)

    ### RPC tests ###
    @asyncio.coroutine
    def test_get_node_id(self, addr):
        # RPC to remote node
        remote_agent = yield from self.container.connect(addr)
        id = yield from remote_agent.get_node_id()
        print("%s got answer from %s: ID is %d" % (self.node_address, addr, id))

    @asyncio.coroutine
    def test_get_closest_preceding_finger(self, addr, node_id):
        # RPC to remote node
        remote_agent = yield from self.container.connect(addr)
        res_node = yield from remote_agent.rpc_get_closest_preceding_finger(node_id)
        print("%s got answer from %s: closest node is %s" % (self.node_address, addr, str(res_node)))

    @asyncio.coroutine
    def test_find_my_successor(self, addr):
        # RPC to remote node
        remote_agent = yield from self.container.connect(addr)
        res_node = yield from remote_agent.rpc_get_closest_preceding_finger(self.id + 1)
        print("%s got answer from %s: my successor is %s" % (self.node_address, addr, str(res_node)))


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
#loop.run_until_complete(nodes[1].setup_node(bootstrap_address=bootstrap_addr))
# Test RPC calls within the same node from backup agent 1 to agent 0
loop.run_until_complete(nodes[1].test_get_node_id(nodes[0].addr))

# Test RPC calls to bootstrap node (first port)
if bootstrap_addr is not None:
    loop.run_until_complete(nodes[0].test_get_node_id(bootstrap_addr))
    loop.run_until_complete(nodes[0].test_get_closest_preceding_finger(bootstrap_addr, 123))
    loop.run_until_complete(nodes[0].test_find_my_successor(bootstrap_addr))

loop.run_forever()
c.shutdown()