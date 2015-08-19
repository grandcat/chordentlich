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
    def __init__(self, chordRingSize):
        self.chordRingSize = chordRingSize

    """Set default attribute values only

    :param key: the key to be hased as byte array
    :returns: a hash value which is the location in the chord ring
    :rtype: int
    """
    def get_key(self, key, replicaIndex=1):
        return int(hashlib.sha256(key+replicaIndex.to_bytes(1, byteorder='big')).hexdigest(), 16) % self.chordRingSize

    """Get a list of replica keys for a given replica key

    :param key: the key to be hased as byte array
    :type key: bytearray
    :returns: a list of integer replica keys as
    :rtype: list of type int
    """
    def get_key_list(self, key, replicationCount):
        indices = []
        for i in range(0, replicationCount):
            indices.append(self.get_key(key, i))

        return indices
