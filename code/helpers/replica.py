#!/usr/bin/python3
import hashlib

"""
The replica module provides functions for managing replication in the Chord ring.
This part is based on the paper `Dynamic Replica Management in Distributed Hash Tables <http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.2.5845&rep=rep1&type=pdf/>`_ from Waldvogel et al.
"""

class Replica:

    """
    :param chordRingSize: The Size of the Chord ring. Usually something like 2^256
    """
    def __init__(self, chordRingSize, replicationCount=3):
        self.chordRingSize = chordRingSize
        self.replicationCount = replicationCount

    def get_key(self, key, replicaIndex=1):
        """Set default attribute values only

        :param key: the key to be hased as byte array
        :returns: a hash value which is the location in the chord ring
        :rtype: int
        """

        if replicaIndex>0:
            key = self.get_key(key, replicaIndex-1)

        return int(hashlib.sha256(key.to_bytes(32, byteorder='big')).hexdigest(), 16) % self.chordRingSize

    def get_key_list(self, key, replicationCount=-1):
        """Get a list of replica keys for a given replica key

        :param key: the key to be hased as byte array
        :type key: bytearray
        :returns: a list of integer replica keys as
        :rtype: list of type int
        """
        replicationCount = self.replicationCount if replicationCount == -1 else replicationCount

        indices = []
        for i in range(0, replicationCount):
            indices.append(self.get_key(key, i))

        return indices
