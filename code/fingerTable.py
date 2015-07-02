#!/usr/bin/python3

import hashlib
import math
import datetime

chordFingerTableSize = 256;
chordRingSize = 2**chordFingerTableSize; # Maximum number of addresses in the Chord network

class Finger():
	

class FingerTable():

	def __init__(self, nodeId):
		self.nodeId = int(nodeId, 16) # make integer for calculations
		self.fingerTable = []
	def getEntry(self, position):
		addition = 2**(position-1)
		entry = (self.nodeId + addition) % chordRingSize
		return hex(entry)

	def initFingerTable(self):

		self.fingerTable.append()

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
