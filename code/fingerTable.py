#!/usr/bin/python3

import hashlib
import math
import datetime


class Finger:
    def __init__(self, startID, successor_node):
        self.startID = startID
        self.successor = successor_node

    def __repr__(self):
        return 'Start: ' + str(self.startID) + ', SuccessorID: ' + str(self.successor.nodeId)

class Node():
    chordFingerTableSize = 8 # TODO: 256
    chordRingSize = 2**chordFingerTableSize  # Maximum number of addresses in the Chord network

    def __init__(self, nodeId, host_address, host_port, bootstrap_address=None):
        self.nodeId = nodeId # make integer for calculations
        self.host_address = host_address
        self.host_port = host_port
        self.bootstrap_address = bootstrap_address
        self.entries = []

    def getEntry(self, position):
        addition = 2**(position-1)
        entry = (self.nodeId + addition) % self.chordRingSize
        return entry

    def initFingerTable(self):
        if self.bootstrap_address is None:
            for k in range(1, self.chordFingerTableSize):
                entry = self
                self.entries.append(Finger((self.nodeId + 2**k) % (self.chordRingSize), entry))
                # print(int((self.nodeId + 2**k) % (self.chordRingSize)))

        print(self.entries)


    def getClosestPrecedingFinder(self, searchKey):
        for k in range(self.chordFingerTableSize, 0, -1):
            if  searchKey > self.getEntry(k):
                return self.getEntry(k+1)

        return self.nodeId

    @property
    def getNodeId(self):
        return self.nodeId

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
