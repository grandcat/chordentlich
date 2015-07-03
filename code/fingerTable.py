#!/usr/bin/python3

import hashlib
import math
import datetime

chordFingerTableSize = 4;
chordRingSize = 2**chordFingerTableSize; # Maximum number of addresses in the Chord network

class Finger():
	def __init__(self, nodeId=None):
		# Start: keys from [ finger[this], finger[this+1] ]
		self.start = None
		# Successor
		self.successor_nodeId = nodeId
		self.successor_host = None

class FingerTable():
	def __init__(self, nodeId):
		self.nodeId = int(nodeId, 16) % chordRingSize # make integer for calculations
		self.fingerTable = []

	def getEntry(self, position):
		addition = 2**(position-1)
		entry = (self.nodeId + addition) % chordRingSize
		return hex(entry)

	def initFingerTable(self, node_successor):
		# for k in range(0, chordFingerTableSize):
		self.addEntry(Finger(nodeId=node_successor))

	def addEntry(self, finger_obj):
		self.fingerTable.append(finger_obj)


	def getClosestPrecedingFinder(self, searchKey):
		for k in range(chordFingerTableSize, 0, -1):
			if  int(searchKey, 16) > int(self.getEntry(k), 16):
				return self.getEntry(k+1)

		return hex(self.nodeId)

	def getNodeId(self):
		return self.nodeId

nodeIdToSearch = hashlib.sha256("213123213213".encode()).hexdigest();

nodeId = hashlib.sha256("213123213213".encode()).hexdigest();
ft1 = FingerTable(nodeId)

print (ft1.getClosestPrecedingFinder(nodeId))
print (ft1.getClosestPrecedingFinder(nodeId))


print (ft1.getEntry(3))
print (ft1.getEntry(4))
print (ft1.getEntry(5))
print (ft1.getEntry(6))


