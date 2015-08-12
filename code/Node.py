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
        self.predecessor = None
        self.log.info("[Configuration]  node_id: %d, bootstrap_node: %s", self.id, self.bootstrap_address)

        # Create first version of finger table to prevent crash during message forward when debugging
        # self.init_finger_table()

    def as_dict(self, serialize_neighbors=False):
        dict_node = {
            "node_id": self.id,
            "node_address": self.node_address,
        }
        if serialize_neighbors:
            dict_node["successor"] = self.fingertable[0]["successor"]
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

    @asyncio.coroutine
    def init_finger_table(self):
        if self.bootstrap_address:
            # Regular node joining via bootstrap node
            self.__generate_fingers(None)

            remote_peer = yield from self.container.connect(self.bootstrap_address)
            # print("Looking for %s" % self.fingertable[0]["start"])
            successor = yield from remote_peer.rpc_find_successor(self.fingertable[0]["start"])
            self.fingertable[0]["successor"] = successor  # TODO: validate successor
            self.print_finger_table()

            # Fix predecessor reference on our direct successor.
            # Retrieve the address of our direct predecessor.
            remote_peer = yield from self.container.connect(successor["node_address"])
            update_pred = yield from remote_peer.rpc_update_predecessor(self.as_dict())
            self.log.debug("Predecessor update result: %s", update_pred)
            # TODO: validate input
            if "old_predecessor" in update_pred:
                # Use successor node if chord overlay only has bootstrap node as only one
                self.predecessor = update_pred["old_predecessor"] or successor
                self.log.info("Set predecessor: %s", self.predecessor)
            else:
                # Something went wrong during update
                self.log.error("Could not update predecessor reference of our successor. Try restarting.")
                # TODO: clean exit

            # Ask successor for the fingers ....

        else:
            # This is the bootstrap node
            successor_node = self.as_dict()
            self.__generate_fingers(successor_node)
            # self.predecessor = successor_node  # If removed, easier to replace with checks on update_predecessor

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
        print("Finger table: %s" % self.fingertable)

    def find_successor(self, node_id):
        node = yield from self.find_predecessor(node_id)
        print("[find_successor] Calculated node for %d: %s" % (node_id, node))
        return node["successor"]  # Attention: relies on available successor information which has to be
                                  # retrieved by closest_preceding_finger()
    @asyncio.coroutine
    def find_predecessor(self, node_id):
        # Special case: id we are looking for is managed by our immediate successor
        successor = self.fingertable[0]["successor"]
        if in_interval(node_id, self.id, successor["node_id"]):
            print("Special case")
            return self.as_dict(serialize_neighbors=True)

        selected_node = self.as_dict(serialize_neighbors=True)
        while not in_interval(node_id, selected_node["node_id"], selected_node["successor"]["node_id"], inclusive_right=True):
            if selected_node["node_id"] == self.id:
                # Typically in first round: use our finger table to locate close peer
                print("Looking for predecessor in first round.")
                selected_node = self.get_closest_preceding_finger(node_id)
                print("Closest finger: %s" % selected_node)
                # If still our self, we do not know closer peer and should stop searching
                if selected_node["node_id"] == self.id:
                    break

            else:
                # For all other remote peers, we have to do a RPC here
                self.log.debug("Starting remote call.")
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


    ### RPC wrappers and functions ###
    @aiomas.expose
    def rpc_update_predecessor(self, remote_node):
        if isinstance(remote_node, dict):
            remote_id = remote_node["node_id"]
            remote_addr = remote_node["node_address"]
            if self.predecessor is None or in_interval(remote_id, self.predecessor["node_id"], self.id):
                # If this is a bootstrap node and this is the first node joining,
                # set predecessor of new node to us. Like this, the ring topology is properly maintained
                old_predecessor = self.predecessor
                self.predecessor = {"node_id": remote_id, "node_address": remote_addr}

                res = self.predecessor.copy()
                res["old_predecessor"] = old_predecessor
                return res
            else:
                # No change for this node's predecessor, because it is closer to our node.
                # Probably, some nodes requested on the way to us, do not have a proper view on this overlay network.
                # Note: Might be better to raise exception
                return self.as_dict()

        else:
            raise TypeError('Invalid type in argument.')

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
        res_node = yield from remote_agent.rpc_find_successor(self.id + 1)
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
loop.run_until_complete(nodes[0].init_finger_table())
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