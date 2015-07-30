#!/usr/bin/python3

import hashlib

CHORD_FINGER_TABLE_SIZE = 8 # TODO: 256
CHORD_RING_SIZE = 2**CHORD_FINGER_TABLE_SIZE  # Maximum number of addresses in the Chord network

class Finger:
    def __init__(self, startID, successor_node=None):
        self.startID = startID
        self.successor = successor_node

    def __repr__(self):
        return 'Start: ' + str(self.startID) + ', SuccessorID: ' + str(self.successor.nodeId)

class Node():

    def __init__(self, host_address, host_port, nodeId=None, bootstrap_address=None):
        self.id = nodeId or self.generate_key(host_address, host_port)
        self.host_address = host_address
        self.host_port = host_port
        self.bootstrap_address = bootstrap_address
        self.entries = []
        self.successor = self # Successor is self at the beginning
        self.predecessor = None

    @staticmethod
    def generate_key(host_address, host_port):
        # TODO: public key hash
        # TODO: remove modulo
        return int(hashlib.sha256((host_address + str(host_port)).encode()).hexdigest(), 16) % CHORD_RING_SIZE

    def getEntry(self, position):
        addition = 2**(position-1)
        entry = (self.id + addition) % CHORD_RING_SIZE
        return entry

    def printFingerTable(self):
        print(self.entries)

    def initFingerTable(self):
        if self.bootstrap_address is None:
            for k in range(1, CHORD_FINGER_TABLE_SIZE):
                entry = self
                self.entries.append(Finger((self.id + 2**k) % (CHORD_RING_SIZE), entry))
                self.entries.append({'start': ((self.id + 2**k) % CHORD_RING_SIZE)})
                # print(int((self.nodeId + 2**k) % (self.chordRingSize)))

    def getClosestPrecedingFinger(self, searchKey):
        for k in range(len(self.entries), 0, -1):
            entry = self.entries[k]
            if  searchKey > entry.startID:
                return self.successor

        return self

    @property
    def getNodeId(self):
        return self.id

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
