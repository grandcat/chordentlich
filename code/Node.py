#!/usr/bin/python3

import hashlib
import logging

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

class Finger(object):
    def __init__(self, start_id, successor_node=None):
        self.startID = start_id
        self.successor = successor_node

    def __repr__(self):
        return 'Start: ' + str(self.startID) + ', SuccessorID: ' + str(self.successor.nodeId)


class Node(object):

    def __init__(self, host_address, host_port, node_id=None, bootstrap_port=None, predecessor=None):
        self.log = logging.getLogger(__name__)
        # Node structure
        self.id = node_id or self.generate_key(host_address, host_port)
        self.host_address = host_address
        self.host_port = host_port
        self.bootstrap_port = bootstrap_port
        self.fingertable = []
        self.successor = self   # Todo: fix dependencies (should be set to None here and initialized properly)

        if predecessor == None:
            self.predecessor = self

        self.log.info("nodeID: %d, port: %d", self.id, self.host_port)
        # Create first version of finger table to prevent crash during message forward when debugging
        self.init_finger_table()

    @staticmethod
    def generate_key(host_address, host_port):
        # TODO: public key hash instead of IP address + port
        # TODO: remove modulo
        return int(hashlib.sha256((host_address + str(host_port)).encode()).hexdigest(), 16) % CHORD_RING_SIZE

    def get_entry(self, position):
        # Todo: rethink whether this makes sense
        addition = 2**(position-1)
        entry = (self.id + addition) % CHORD_RING_SIZE
        return entry

    @property
    def get_node_id(self):
        return self.id

    def print_finger_table(self):
        print(self.fingertable)

    def init_finger_table(self):
        # if self.bootstrap_address is None:
        self.fingertable = []
        for k in range(0, CHORD_FINGER_TABLE_SIZE):
            # entry = self
            # self.fingertable.append(Finger((self.id + 2**k) % (CHORD_RING_SIZE), entry))
            entry = {
                "start": ((self.id + 2**k) % CHORD_RING_SIZE),
                "successor_node": self
            }
            # TODO: add successor if not bootstrap node
            self.fingertable.append(entry)

        self.log.info("Fingertable: %s", str(self.fingertable)+"\n\n")
        if self.bootstrap_port is None:
            # We are the bootstrap node or start a new Chord network
            self.successor = self.predecessor = self

    def find_successor(self, node_id):
        node = self.find_predecessor(node_id)
        return node.successor  # Attention: relies on available successor information which has to be
                               # retrieved by closest_preceding_finger()

    def find_predecessor(self, node_id):
        selected_node = self
        # Special case: id we are looking for is managed by our immediate successor
        if in_interval(node_id, self.id, self.successor.id):
            return self

        while not in_interval(node_id, selected_node.id, selected_node.successor.id, inclusive_right=True):
            if selected_node.id == self.id:
                selected_node = self.get_closest_preceding_finger(node_id)  # deadlocking right now
            else:
                # For all other nodes, we have to do a RPC here
                self.log.warn("RPC not implemented here.")
                break

        return selected_node

    def get_closest_preceding_finger(self, node_id):
        # Find closest preceding finger from m -> 0
        for k in range(CHORD_FINGER_TABLE_SIZE - 1, -1, -1):
            finger_successor = self.fingertable[k]["successor_node"]
            self.log.debug("Iterate finger %d: %d in %s", k, node_id, self.fingertable[k])

            if in_interval(finger_successor.id, self.id, node_id):
                return finger_successor  # BUG: successor information missing, maybe need additional RPC here

        return self


# nodeIdToSearch = hashlib.sha256("213123213213".encode()).hexdigest();
#
# nodeId = hashlib.sha256("213123213213".encode()).hexdigest();
# ft1 = FingerTable(nodeId)
#
# print (ft1.getClosestPrecedingFinder(nodeId))
# print (ft1.getClosestPrecedingFinder(nodeId))
#
#
# print (ft1.getEntry(3))
# print (ft1.getEntry(4))
# print (ft1.getEntry(5))
# print (ft1.getEntry(6))
